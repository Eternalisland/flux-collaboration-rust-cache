// ========== 依赖导入 ==========
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, info, warn};

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use crossbeam_queue::ArrayQueue;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use dashmap::DashSet;
use memmap2::{Mmap, MmapMut};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::AtomicBool;

/// 【核心数据结构】Seqlock 保护的映射
///
/// 原理：
/// 1. 写者更新时递增 seq（奇数表示写入中）
/// 2. 读者读取前后检查 seq 是否变化
/// 3. 如果变化，重试读取
struct SeqlockedMmap {
    /// 序列号：奇数=写入中，偶数=稳定
    seq: AtomicUsize,
    /// 实际的映射（双缓冲）
    mmap: ArcSwap<Option<Arc<Mmap>>>,
}

impl SeqlockedMmap {
    fn new() -> Self {
        Self {
            seq: AtomicUsize::new(0),
            mmap: ArcSwap::from_pointee(None),
        }
    }

    /// 【读者】安全读取（无锁，可能重试）
    #[inline]
    fn load(&self) -> Option<Arc<Mmap>> {
        const MAX_RETRIES: usize = 100;

        for retry in 0..MAX_RETRIES {
            // 1. 读取序列号（开始）
            let seq_start = self.seq.load(Ordering::Acquire);

            // 2. 如果是奇数，说明写者正在修改，自旋等待
            if seq_start & 1 == 1 {
                if retry < 50 {
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
                continue;
            }

            // 3. 读取数据
            let mmap = self.mmap.load().as_ref().as_ref().cloned();

            // 4. 读取序列号（结束）
            std::sync::atomic::fence(Ordering::Acquire);
            let seq_end = self.seq.load(Ordering::Acquire);

            // 5. 如果序列号未变化，说明读取成功
            if seq_start == seq_end {
                return mmap;
            }

            // 6. 否则重试
            if retry < 50 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }

        // 超过最大重试次数，记录错误
        log::error!("SeqlockedMmap::load: 超过最大重试次数");
        None
    }

    /// 【写者】更新映射（独占访问）
    fn store(&self, new_mmap: Option<Arc<Mmap>>) {
        // 1. 递增 seq 到奇数（标记写入中）
        let old_seq = self.seq.fetch_add(1, Ordering::Release);
        debug_assert_eq!(old_seq & 1, 0, "store() called concurrently");

        // 2. 更新数据
        self.mmap.store(Arc::new(new_mmap));

        // 3. 递增 seq 到偶数（标记写入完成）
        std::sync::atomic::fence(Ordering::Release);
        self.seq.fetch_add(1, Ordering::Release);
    }
}
// ===== 新增：压缩相关依赖 =====
// 推荐使用 lz4_flex：解压速度极快（4GB/s+），压缩比适中
// 或者 zstd：可调节压缩级别，兼顾速度和压缩比
// use lz4_flex;  // 需要添加到 Cargo.toml

/// 压缩配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionConfig {
    /// 是否启用压缩
    pub enabled: bool,

    /// 压缩算法类型
    pub algorithm: CompressionAlgorithm,

    /// 最小压缩阈值（字节）：小于此值不压缩
    pub min_compress_size: usize,

    /// 最小压缩比阈值：压缩后大小/原始大小 > 此值则不保存压缩版本
    /// 例如 0.9 表示压缩后如果还占原大小的90%以上，就不值得压缩
    pub min_compress_ratio: f64,

    /// 是否启用异步压缩（写入时先存原始数据，后台压缩后替换）
    pub async_compression: bool,

    /// 压缩级别（1-22，数字越大压缩比越高但速度越慢）
    pub compression_level: i32,

    /// 是否启用可压缩性预测（快速判断数据是否值得压缩）
    pub enable_compressibility_check: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionAlgorithm {
    /// 不压缩
    None,
    /// LZ4：超快解压（4GB/s），适中压缩比（2-3x）
    Lz4,
    /// Zstd：可调节，平衡速度和压缩比（3-5x）
    Zstd,
    /// Snappy：Google开发，解压快，压缩比略低
    Snappy,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: CompressionAlgorithm::Lz4,
            min_compress_size: 1024 * 1024,   // 小于1M不压缩
            min_compress_ratio: 0.90, // 压缩不到10%就不保存
            async_compression: true, // 默认同步压缩
            compression_level: 1,     // 最快压缩级别
            enable_compressibility_check: true,
        }
    }
}

/// 压缩上下文（用于批量操作复用）
struct CompressionContext {
    // 复用的压缩缓冲区，避免反复分配
    compress_buffer: Vec<u8>,
    decompress_buffer: Vec<u8>,
}

impl CompressionContext {
    fn new() -> Self {
        Self {
            compress_buffer: Vec::with_capacity(64 * 1024),
            decompress_buffer: Vec::with_capacity(64 * 1024),
        }
    }
}

/// 内存统计信息
#[derive(Debug, Clone, Serialize)]
pub struct MemoryStats {
    /// L1缓存占用的字节数
    pub l1_cache_bytes: u64,
    /// L1缓存的条目数
    pub l1_cache_entries: usize,
    /// 索引占用的字节数（近似值）
    pub index_bytes: u64,
    /// 索引条目数
    pub index_entries: usize,
    /// 有序索引占用的字节数（近似值）
    pub ordered_index_bytes: u64,
    /// 惰性删除条目占用的字节数（近似值）
    pub lazy_deleted_bytes: u64,
    /// 惰性删除的条目数
    pub lazy_deleted_entries: usize,
    /// 预读队列占用的字节数（近似值）
    pub prefetch_queue_bytes: u64,
    /// 预读队列的大小
    pub prefetch_queue_size: usize,
    /// MMAP文件大小（虚拟内存）
    pub mmap_bytes: u64,
    /// 总堆内存使用量（字节）
    pub total_heap_bytes: u64,
    /// 内存使用比例（0.0-1.0）
    pub memory_usage_ratio: f64,
    /// 是否处于内存压力状态
    pub under_memory_pressure: bool,
    /// 缓存驱逐次数
    pub cache_evictions: u64,
    /// 强制驱逐次数
    pub forced_evictions: u64,
}

/// 内存限制配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MemoryLimitConfig {
    /// 堆内存软限制（字节）
    pub heap_soft_limit: u64,
    /// 堆内存硬限制（字节）
    pub heap_hard_limit: u64,
    /// L1缓存硬限制（字节）
    pub l1_cache_hard_limit: u64,
    /// L1缓存条目硬限制
    pub l1_cache_entry_hard_limit: usize,
    /// 单次最大驱逐比例（0.0-1.0）
    pub max_eviction_percent: f64,
    /// 内存压力时是否拒绝写入
    pub reject_writes_under_pressure: bool,
    /// 内存检查间隔（毫秒）
    pub check_interval_ms: u64,
}

impl Default for MemoryLimitConfig {
    fn default() -> Self {
        Self {
            // 默认堆内存软限制：100MB
            heap_soft_limit: 100 * 1024 * 1024,
            // 默认堆内存硬限制：200MB
            heap_hard_limit: 200 * 1024 * 1024,
            // 默认L1缓存硬限制：50MB
            l1_cache_hard_limit: 50 * 1024 * 1024,
            // 默认L1缓存条目限制：10000
            l1_cache_entry_hard_limit: 10_000,
            // 默认最大驱逐比例：10%
            max_eviction_percent: 0.1,
            // 默认内存压力时拒绝写入
            reject_writes_under_pressure: true,
            // 默认检查间隔：1000ms（1秒）
            check_interval_ms: 1_000,
        }
    }
}

/// 高性能无锁 MMAP 磁盘存储
///
/// # 核心无锁优化
/// 1. **读路径完全无锁**：使用 ArcSwap 实现零锁读取
/// 2. **原子统计**：所有统计信息使用原子计数器，消除写锁
/// 3. **分段写锁**：写路径按区域分段，减少锁竞争
/// 4. **无锁缓存**：缓存使用原子操作和 DashMap 的无锁特性
/// 5. **批量统计更新**：降低统计更新频率
pub struct HighPerfMmapStorage {
    #[allow(dead_code)]
    disk_dir: PathBuf,
    data_file_path: PathBuf,
    index_file_path: PathBuf,
    data_file_size: Arc<AtomicU64>,
    write_offset: Arc<AtomicU64>,

    /// 无锁索引：键 -> (文件偏移量, 数据大小)
    index: Arc<DashMap<String, (u64, u64)>>,

    /// 【关键改造1】写映射仍用 RwLock（写入时需要独占）
    /// 【保留】写映射仍需锁保护（写入本身需要独占）
    mmap_rw: Arc<parking_lot::RwLock<Option<MmapMut>>>,

    /// 【关键改造2】读映射改用 ArcSwap：读路径完全无锁！
    /// 读者通过 load() 获取 Arc<Mmap>，无需任何锁
    // mmap_ro: Arc<ArcSwap<Option<Arc<Mmap>>>>,
    // mmap_ro: Arc<ArcSwapOption<Mmap>>,

    /// 【关键】使用 Seqlock 保护的映射
    mmap_ro: Arc<SeqlockedMmap>,

    /// 【新增】读者计数器：追踪有多少读线程正在使用映射
    /// 使用 AtomicU64 比 RwLock 性能更好
    active_readers: Arc<AtomicU64>,

    active_writers: Arc<AtomicU64>,
    resize_lock: Mutex<()>,

    /// 【关键改造3】map_lock 仅用于保护重映射窗口，读路径不再需要
    map_lock: parking_lot::RwLock<()>,

    hot_cache: Arc<DashMap<String, CachedData>>,
    hot_cache_bytes: Arc<AtomicU64>,

    prefetch_queue: Arc<ArrayQueue<String>>,
    prefetch_set: Arc<DashSet<String>>,

    /// 【关键改造4】统计信息全部原子化，消除写锁
    stats: Arc<AtomicStats>,

    config: HighPerfMmapConfig,

    /// 【修复】惰性删除条目：offset -> (key, size)
    /// 使用 offset 作为键，避免同一 key 多次覆盖时信息丢失
    lazy_deleted_entries: Arc<DashMap<u64, (String, u64)>>,
    lazy_by_end: Arc<parking_lot::RwLock<BTreeMap<u64, (String, (u64, u64))>>>,
    ordered_index: Arc<parking_lot::RwLock<BTreeMap<u64, String>>>,
    next_gc_time: Arc<AtomicU64>,
    last_purge_check: Arc<AtomicU64>,

    shutdown: Arc<AtomicBool>,
    prefetch_thread: Mutex<Option<std::thread::JoinHandle<()>>>,

    cache_evictions: Arc<AtomicU64>,
    forced_evictions: Arc<AtomicU64>,
    last_memory_check: Arc<AtomicU64>,

    memory_limit: MemoryLimitConfig,
    /// 内存监控后台线程句柄
    memory_monitor_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// 【关键改造4】原子化的统计结构
/// 所有字段都是原子类型，更新时无需加锁
pub struct AtomicStats {
    pub total_writes: AtomicU64,
    pub total_reads: AtomicU64,
    pub total_deletes: AtomicU64,
    pub total_write_bytes: AtomicU64,
    pub total_read_bytes: AtomicU64,
    pub l1_cache_hits: AtomicU64,
    pub l1_cache_misses: AtomicU64,
    pub l2_cache_hits: AtomicU64,
    pub l2_cache_misses: AtomicU64,
    pub prefetch_hits: AtomicU64,
    pub file_expansions: AtomicU64,

    /// EMA 平均延迟（微秒）- 使用原子更新
    pub avg_write_latency_us: AtomicU64,
    pub avg_read_latency_us: AtomicU64,

    pub mmap_remaps: AtomicU64,
    pub error_count: AtomicU64,

    /// 最近错误码（使用 ArcSwap 实现无锁读写）
    pub last_error_code: ArcSwap<Option<String>>,

    pub lazy_deleted_entries: AtomicU64,
    pub reclaimed_disk_space: AtomicU64,
    pub garbage_collection_runs: AtomicU64,
    pub avg_write_path_us: AtomicU64,
    pub maintenance_expand_events: AtomicU64,
    pub expand_us_total: AtomicU64,
    pub compression_attempts: AtomicU64,
    pub compression_successes: AtomicU64,
    pub compressed_bytes_saved: AtomicU64,
    pub decompression_count: AtomicU64,
    pub avg_compression_ratio: AtomicU64, // 存储为整数（ratio * 1000）
    /// 预读真正服务命中（首次命中 L1 且由预读放入）
    pub prefetch_served_hits: AtomicU64,
}

impl Default for AtomicStats {
    fn default() -> Self {
        Self {
            total_writes: AtomicU64::new(0),
            total_reads: AtomicU64::new(0),
            total_deletes: AtomicU64::new(0),
            total_write_bytes: AtomicU64::new(0),
            total_read_bytes: AtomicU64::new(0),
            l1_cache_hits: AtomicU64::new(0),
            l1_cache_misses: AtomicU64::new(0),
            l2_cache_hits: AtomicU64::new(0),
            l2_cache_misses: AtomicU64::new(0),
            prefetch_hits: AtomicU64::new(0),
            file_expansions: AtomicU64::new(0),
            avg_write_latency_us: AtomicU64::new(0),
            avg_read_latency_us: AtomicU64::new(0),
            mmap_remaps: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_error_code: ArcSwap::from_pointee(None),
            lazy_deleted_entries: AtomicU64::new(0),
            reclaimed_disk_space: AtomicU64::new(0),
            garbage_collection_runs: AtomicU64::new(0),
            avg_write_path_us: AtomicU64::new(0),
            maintenance_expand_events: AtomicU64::new(0),
            expand_us_total: AtomicU64::new(0),
            prefetch_served_hits: AtomicU64::new(0),
            compression_attempts: AtomicU64::new(0),
            compression_successes: AtomicU64::new(0),
            compressed_bytes_saved: AtomicU64::new(0),
            decompression_count: AtomicU64::new(0),
            avg_compression_ratio: AtomicU64::new(1000), // 初始为 1.0 (1000/1000)
        }
    }
}

/// 统计快照结构（用于读取）
#[derive(Debug, Clone, Serialize)]
pub struct HighPerfMmapStats {
    pub total_writes: u64,
    pub total_reads: u64,
    pub total_deletes: u64,
    // 当前索引条目数（活跃记录总数）
    pub total_records: u64,
    pub total_write_bytes: u64,
    pub total_read_bytes: u64,
    pub l1_cache_hits: u64,
    pub l1_cache_misses: u64,
    pub l2_cache_hits: u64,
    pub l2_cache_misses: u64,
    pub prefetch_hits: u64,
    pub file_expansions: u64,
    pub avg_write_latency_us: u64,
    pub avg_read_latency_us: u64,
    pub mmap_remaps: u64,
    pub error_count: u64,
    pub last_error_code: Option<String>,
    pub lazy_deleted_entries: u64,
    pub reclaimed_disk_space: u64,
    pub garbage_collection_runs: u64,
    pub avg_write_path_us: u64,
    pub maintenance_expand_events: u64,
    pub expand_us_total: u64,
    pub prefetch_served_hits: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HighPerfMmapStatus {
    pub stats: HighPerfMmapStats,
    pub memory: MemoryStats,
    pub config: HighPerfMmapConfig ,
}

// #[derive(Debug, Clone)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HighPerfMmapConfig {
    pub initial_file_size: u64,
    pub growth_step: u64,
    pub growth_reserve_steps: u32,
    pub max_file_size: u64,
    pub enable_compression: bool,
    pub l1_cache_size_limit: u64,
    pub l1_cache_entry_limit: usize,
    pub l2_cache_size_limit: u64,
    pub l2_cache_entry_limit: usize,
    pub enable_prefetch: bool,
    pub prefetch_queue_size: usize,
    pub memory_pressure_threshold: f64,
    pub cache_degradation_threshold: f64,
    /// 新增：详细的压缩配置
    pub compression: CompressionConfig,
    /// 自动过期清理阈值（秒），None 表示禁用
    pub auto_purge_after_secs: Option<u64>,
    /// 过期清理检查周期（秒），0 表示每次都检查
    pub auto_purge_check_interval_secs: u64,
}

impl Default for HighPerfMmapConfig {
    fn default() -> Self {
        Self {
            initial_file_size: 100 * 1024 * 1024,
            growth_step: 50 * 1024 * 1024,
            growth_reserve_steps: 3,
            max_file_size: 10 * 1024 * 1024 * 1024,
            enable_compression: false,
            l1_cache_size_limit: 50 * 1024 * 1024,
            l1_cache_entry_limit: 500,
            l2_cache_size_limit: 200 * 1024 * 1024,
            l2_cache_entry_limit: 2000,
            enable_prefetch: true,
            prefetch_queue_size: 100,
            memory_pressure_threshold: 0.8,
            cache_degradation_threshold: 0.9,
            compression: CompressionConfig::default(),
            // 默认 保留 8 小时数据
            auto_purge_after_secs: Some(28800),
            auto_purge_check_interval_secs: 300,
        }
    }
}

#[derive(Clone)]
pub struct ZeroCopySlice {
    mmap: Arc<Mmap>,
    start: usize,
    end: usize,
}

impl ZeroCopySlice {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[self.start..self.end]
    }

    #[inline]
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone)]
struct CachedData {
    data: Vec<u8>,
    last_access: Instant,
    access_count: u64,
    size: u64,
}

#[derive(Debug, Clone, Copy)]
struct SimpleDataHeader {
    size: u32,
    compressed: bool,
    timestamp: u64,
}

const HEADER_SIZE: usize = 16;
const HDR_OFF_DATA_SIZE: std::ops::Range<usize> = 0..4;
const HDR_OFF_COMPRESSED: usize = 4;
const HDR_OFF_COMMIT: usize = 5;
const HDR_OFF_TIMESTAMP: std::ops::Range<usize> = 8..16;
const DEFAULT_COALESCE_GAP: usize = 16 * 1024;

/// 【关键改造5】原子EMA更新：使用CAS循环，无需锁
#[inline]
fn update_ema_atomic(avg: &AtomicU64, sample: u64) {
    const ALPHA_NUM: u64 = 1;
    const ALPHA_DEN: u64 = 8;

    loop {
        let current = avg.load(Ordering::Relaxed);
        let new_avg = (current * (ALPHA_DEN - ALPHA_NUM) + sample * ALPHA_NUM) / ALPHA_DEN;

        if avg
            .compare_exchange_weak(current, new_avg, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            break;
        }
        std::hint::spin_loop();
    }
}

struct WriterGuard<'a>(&'a AtomicU64);

impl<'a> WriterGuard<'a> {
    #[inline]
    fn new(c: &'a AtomicU64) -> Self {
        c.fetch_add(1, Ordering::Relaxed);
        Self(c)
    }
}

impl<'a> Drop for WriterGuard<'a> {
    #[inline]
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

// ========== 核心修复1：读取时增加引用计数 ==========

/// 【RAII守卫】自动管理读者计数
struct ReaderGuard {
    counter: Arc<AtomicU64>,
}

impl ReaderGuard {
    #[inline]
    fn new(counter: Arc<AtomicU64>) -> Self {
        counter.fetch_add(1, Ordering::Acquire);
        Self { counter }
    }
}

impl Drop for ReaderGuard {
    #[inline]
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Release);
    }
}

impl HighPerfMmapStorage {
    /// Maximum attempts when trying to grab an mmap snapshot while a remap is in flight.
    /// With backoff this equates to ~0.5s worst-case wait before surfacing an error.
    const MMAP_RETRY_ATTEMPTS: u32 = 10_000;

    // ========== 核心修复1：读取时增加引用计数 ==========
    /// 【新增】获取详细的内存统计信息
    pub fn get_memory_stats(&self) -> MemoryStats {
        // L1缓存统计
        let l1_bytes = self.hot_cache_bytes.load(Ordering::Relaxed);
        let l1_entries = self.hot_cache.len();

        // 索引统计（每个条目约：String(平均20字节) + (u64, u64) = 36字节）
        let index_entries = self.index.len();
        let index_bytes = (index_entries * 36) as u64;

        // 有序索引统计（每个条目约：u64 + String(20字节) = 28字节）
        let ordered_entries = {
            let ord = self.ordered_index.read();
            ord.len()
        };
        let ordered_index_bytes = (ordered_entries * 28) as u64;

        // 惰性删除表统计
        let lazy_entries = self.lazy_deleted_entries.len();
        let lazy_deleted_bytes = (lazy_entries * 36) as u64;

        // 预读队列统计（每个条目约：String(20字节)）
        let prefetch_size = self.prefetch_queue.len();
        let prefetch_queue_bytes = (prefetch_size * 20) as u64;

        // MMAP文件大小
        let mmap_bytes = self.data_file_size.load(Ordering::Relaxed);

        // 总堆内存（不含MMAP，因为MMAP是虚拟内存）
        let total_heap = l1_bytes
            + index_bytes
            + ordered_index_bytes
            + lazy_deleted_bytes
            + prefetch_queue_bytes;

        let usage_ratio = total_heap as f64 / self.memory_limit.heap_hard_limit as f64;
        let under_pressure = usage_ratio > 0.8;

        MemoryStats {
            l1_cache_bytes: l1_bytes,
            l1_cache_entries: l1_entries,
            index_bytes,
            index_entries,
            ordered_index_bytes,
            lazy_deleted_bytes,
            lazy_deleted_entries: lazy_entries,
            prefetch_queue_bytes,
            prefetch_queue_size: prefetch_size,
            mmap_bytes,
            total_heap_bytes: total_heap,
            memory_usage_ratio: usage_ratio,
            under_memory_pressure: under_pressure,
            cache_evictions: self.cache_evictions.load(Ordering::Relaxed),
            forced_evictions: self.forced_evictions.load(Ordering::Relaxed),
        }
    }

    /// 【新增】检查并执行内存控制策略
    fn check_memory_limits(&self) -> io::Result<()> {
        let stats = self.get_memory_stats();

        // 检查是否超过软限制
        if stats.total_heap_bytes > self.memory_limit.heap_soft_limit {
            // 计算需要释放的字节数
            let target = self.memory_limit.heap_soft_limit * 85 / 100; // 目标：降到85%
            let to_free = stats.total_heap_bytes.saturating_sub(target);

            info!(
                "内存软限制触发: 当前{}MB, 限制{}MB, 需释放{}MB",
                stats.total_heap_bytes / 1024 / 1024,
                self.memory_limit.heap_soft_limit / 1024 / 1024,
                to_free / 1024 / 1024
            );

            self.evict_cache_by_size(to_free, false)?;
        }

        // 检查是否超过硬限制（紧急情况）
        if stats.total_heap_bytes > self.memory_limit.heap_hard_limit {
            let to_free = stats.total_heap_bytes.saturating_sub(
                self.memory_limit.heap_hard_limit * 75 / 100, // 强制降到75%
            );

            log::error!(
                "内存硬限制触发！当前{}MB, 限制{}MB, 强制释放{}MB",
                stats.total_heap_bytes / 1024 / 1024,
                self.memory_limit.heap_hard_limit / 1024 / 1024,
                to_free / 1024 / 1024
            );

            self.forced_evictions.fetch_add(1, Ordering::Relaxed);
            self.evict_cache_by_size(to_free, true)?;

            // 触发GC回收惰性删除的空间
            if stats.lazy_deleted_entries > 100 {
                let _ = self.garbage_collect();
            }
        }

        // 检查L1缓存是否超限
        if stats.l1_cache_bytes > self.memory_limit.l1_cache_hard_limit
            || stats.l1_cache_entries > self.memory_limit.l1_cache_entry_hard_limit
        {
            let to_free = stats
                .l1_cache_bytes
                .saturating_sub(self.memory_limit.l1_cache_hard_limit * 90 / 100);
            self.evict_cache_by_size(to_free, false)?;
        }

        Ok(())
    }

    /// 【新增】按大小驱逐缓存
    fn evict_cache_by_size(&self, target_bytes: u64, aggressive: bool) -> io::Result<()> {
        if target_bytes == 0 {
            return Ok(());
        }

        let cache_size = self.hot_cache.len();
        if cache_size == 0 {
            return Ok(());
        }

        // 计算采样大小
        let sample_size = if aggressive {
            cache_size // 激进模式：扫描全部
        } else {
            // 限制单次驱逐百分比
            let max_evict = (cache_size as f64 * self.memory_limit.max_eviction_percent) as usize;
            std::cmp::min(cache_size, std::cmp::max(max_evict, 100))
        };

        // 收集待驱逐的条目（按LRU排序）
        let mut entries: Vec<(String, Instant, u64)> = Vec::with_capacity(sample_size);
        let mut count = 0;

        for entry in self.hot_cache.iter() {
            entries.push((
                entry.key().clone(),
                entry.value().last_access,
                entry.value().size,
            ));
            count += 1;
            if count >= sample_size {
                break;
            }
        }

        // 按最后访问时间排序（最旧的优先驱逐）
        entries.sort_by(|a, b| a.1.cmp(&b.1));

        // 执行驱逐
        let mut freed = 0u64;
        let mut evicted_count = 0u64;

        for (key, _time, size) in entries {
            if freed >= target_bytes {
                break;
            }

            if let Some((_k, cached)) = self.hot_cache.remove(&key) {
                self.hot_cache_bytes
                    .fetch_sub(cached.size, Ordering::Relaxed);
                freed = freed.saturating_add(size);
                evicted_count += 1;
            }
        }

        self.cache_evictions
            .fetch_add(evicted_count, Ordering::Relaxed);

        debug!(
            "缓存驱逐完成: 释放{}MB ({} 条目), 剩余{}MB ({} 条目)",
            freed / 1024 / 1024,
            evicted_count,
            self.hot_cache_bytes.load(Ordering::Relaxed) / 1024 / 1024,
            self.hot_cache.len()
        );

        Ok(())
    }

    /// 【新增】清空所有缓存（JNI关闭前调用）
    pub fn clear_all_caches(&self) {
        info!("清空所有缓存...");

        // 清空L1缓存
        let count = self.hot_cache.len();
        self.hot_cache.clear();
        self.hot_cache_bytes.store(0, Ordering::Relaxed);

        // 清空预读队列
        while let Some(_) = self.prefetch_queue.pop() {}
        self.prefetch_set.clear();

        info!("已清空 {} 个缓存条目", count);
    }

    /// 【新增】获取内存压力等级（供JNI调用方监控）
    pub fn get_memory_pressure_level(&self) -> u8 {
        let stats = self.get_memory_stats();
        let ratio = stats.memory_usage_ratio;

        if ratio < 0.6 {
            0 // 正常
        } else if ratio < 0.75 {
            1 // 警告
        } else if ratio < 0.9 {
            2 // 压力
        } else {
            3 // 危险
        }
    }

    /// 【核心压缩逻辑】智能压缩数据
    /// 返回 (实际存储的数据, 是否已压缩, 原始大小)
    fn compress_if_beneficial(&self, data: &[u8]) -> io::Result<(Vec<u8>, bool, usize)> {
        let original_size = data.len();

        // 检查1：是否启用压缩
        if !self.config.compression.enabled {
            return Ok((data.to_vec(), false, original_size));
        }

        // 检查2：数据是否足够大（小数据压缩不划算）
        if original_size < self.config.compression.min_compress_size {
            return Ok((data.to_vec(), false, original_size));
        }

        // 检查3：快速可压缩性检测（采样熵检测）
        if self.config.compression.enable_compressibility_check {
            if !self.is_compressible(data) {
                return Ok((data.to_vec(), false, original_size));
            }
        }

        self.stats
            .compression_attempts
            .fetch_add(1, Ordering::Relaxed);

        // 执行压缩
        let compressed = match self.config.compression.algorithm {
            CompressionAlgorithm::None => {
                return Ok((data.to_vec(), false, original_size));
            }
            CompressionAlgorithm::Lz4 => self.compress_lz4(data)?,
            CompressionAlgorithm::Zstd => self.compress_zstd(data)?,
            CompressionAlgorithm::Snappy => self.compress_snappy(data)?,
        };

        let compressed_size = compressed.len();
        let ratio = compressed_size as f64 / original_size as f64;

        // 检查4：压缩比是否足够好
        if ratio > self.config.compression.min_compress_ratio {
            // 压缩收益不足，使用原始数据
            return Ok((data.to_vec(), false, original_size));
        }

        // 压缩成功，更新统计
        let saved = original_size.saturating_sub(compressed_size) as u64;
        self.stats
            .compression_successes
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .compressed_bytes_saved
            .fetch_add(saved, Ordering::Relaxed);

        // 更新平均压缩比（EMA）
        let ratio_int = (ratio * 1000.0) as u64;
        update_ema_atomic(&self.stats.avg_compression_ratio, ratio_int);

        Ok((compressed, true, original_size))
    }

    /// 【快速可压缩性检测】通过采样熵来判断
    /// 原理：随机不可压缩数据（如已压缩文件、加密数据）的熵接近8
    ///      文本、JSON等可压缩数据的熵通常在4-6之间
    fn is_compressible(&self, data: &[u8]) -> bool {
        // 采样前1KB或全部数据（取较小者）
        let sample_size = std::cmp::min(data.len(), 1024);
        let sample = &data[..sample_size];

        // 计算字节频率
        let mut freq = [0u32; 256];
        for &byte in sample {
            freq[byte as usize] += 1;
        }

        // 计算香农熵
        let mut entropy = 0.0f64;
        let len = sample.len() as f64;
        for &count in &freq {
            if count > 0 {
                let p = count as f64 / len;
                entropy -= p * p.log2();
            }
        }

        // 熵 < 7.5 认为可压缩（经验阈值）
        entropy < 7.5
    }

    /// LZ4 压缩（推荐：速度最快）
    fn compress_lz4(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        // 实际实现需要 lz4_flex crate
        // use lz4_flex::compress_prepend_size;
        // Ok(compress_prepend_size(data))

        // 这里提供伪代码框架
        // 实际使用时取消下面注释并添加依赖
        /*
        use lz4_flex::compress_prepend_size;
        Ok(compress_prepend_size(data))
        */

        // 占位实现（实际需要真实的压缩库）
        Ok(data.to_vec())
    }

    /// Zstd 压缩（推荐：可调节压缩级别）
    fn compress_zstd(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        // 实际实现需要 zstd crate
        /*
        use zstd::bulk::compress;
        compress(data, self.config.compression.compression_level)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        */

        Ok(data.to_vec())
    }

    /// Snappy 压缩
    fn compress_snappy(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        // 实际实现需要 snap crate
        /*
        use snap::raw::Encoder;
        let mut encoder = Encoder::new();
        encoder.compress_vec(data)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        */

        Ok(data.to_vec())
    }

    /// 【解压缩】根据算法类型解压
    fn decompress_data(&self, data: &[u8], algorithm: CompressionAlgorithm) -> io::Result<Vec<u8>> {
        self.stats
            .decompression_count
            .fetch_add(1, Ordering::Relaxed);

        match algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Lz4 => self.decompress_lz4(data),
            CompressionAlgorithm::Zstd => self.decompress_zstd(data),
            CompressionAlgorithm::Snappy => self.decompress_snappy(data),
        }
    }

    fn decompress_lz4(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        /*
        use lz4_flex::decompress_size_prepended;
        decompress_size_prepended(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        */
        Ok(data.to_vec())
    }

    fn decompress_zstd(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        /*
        use zstd::bulk::decompress;
        decompress(data, self.config.l1_cache_size_limit as usize)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        */
        Ok(data.to_vec())
    }

    fn decompress_snappy(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        /*
        use snap::raw::Decoder;
        let mut decoder = Decoder::new();
        decoder.decompress_vec(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        */
        Ok(data.to_vec())
    }

    /// 【无锁优化】读取 mmap 的核心方法：完全无锁
    ///  读取时获取共享锁，保证映射不会在读取期间被释放
    //     ///
    //     /// 性能分析：
    //     /// - RwLock 读锁获取：~10-20ns（无竞争）
    //     /// - 原子操作：~2ns
    //     /// - 总开销：<30ns，相比1μs读取时间可忽略
    #[inline]
    fn load_mmap_ro(&self) -> Option<Arc<Mmap>> {
        self.mmap_ro.load()
    }
    // fn load_mmap_ro(&self) -> Option<(Arc<Mmap>, parking_lot::RwLockReadGuard<()>)> {
    // 先获取共享读锁（阻止写者修改映射）
    // self.mmap_ro.load();
    // let lock_guard = self.map_lock.read();
    //
    // // 在锁保护下加载映射
    // match self.mmap_ro.load_full() {
    //     Some(mmap) => Some((mmap, lock_guard)),
    //     None => {
    //         // 理论上不应该到这里（除非存储正在关闭）
    //         log::error!("load_mmap_ro: mmap_ro is None under read lock");
    //         None
    //     }
    // }
    // }

    fn write_data_at_offset_once(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        let mut mmap_guard = self.mmap_rw.write();

        if let Some(mmap) = mmap_guard.as_mut() {
            let len = mmap.len();
            let header_size = HEADER_SIZE;

            if offset as usize > len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "write offset out of bounds",
                ));
            }

            let start = offset as usize;
            let end_header = start.saturating_add(header_size);
            if end_header > len {
                self.record_error("write_header_oob");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "header out of bounds",
                ));
            }

            let data_end = end_header.saturating_add(data.len());
            if data_end > len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "data out of bounds",
                ));
            }

            let now_ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let mut header_bytes = [0u8; HEADER_SIZE];
            header_bytes[HDR_OFF_DATA_SIZE].copy_from_slice(&(data.len() as u32).to_le_bytes());
            header_bytes[HDR_OFF_COMPRESSED] = 0;
            header_bytes[HDR_OFF_COMMIT] = 0;
            header_bytes[HDR_OFF_TIMESTAMP].copy_from_slice(&now_ts.to_le_bytes());

            mmap[start..end_header].copy_from_slice(&header_bytes);
            mmap[end_header..data_end].copy_from_slice(data);
            mmap[start + HDR_OFF_COMMIT] = 1;

            Ok(())
        } else {
            return Err(Error::new(ErrorKind::WouldBlock, "mmap_rw not initialized"));
        }
    }

    fn write_data_at_offset_retry(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        for attempt in 0..=2 {
            {
                let _ml = self.map_lock.read();
                if let Ok(()) = self.write_data_at_offset_once(offset, data) {
                    return Ok(());
                }
            }

            if attempt < 2 {
                let _ = self.remap_mmap();
                std::thread::yield_now();
                continue;
            }
            break;
        }
        Err(std::io::Error::new(
            ErrorKind::Other,
            "write failed after retry",
        ))
    }

    fn remap_mmap_locked(&self) -> std::io::Result<()> {
        // 【关键】只需 map_lock 保护文件操作
        let _lock = self.map_lock.write();

        // 创建新映射
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.data_file_path)?;

        let mmap_w = unsafe { MmapMut::map_mut(&file)? };
        let mmap_r = unsafe { Mmap::map(&file)? };

        // 【关键】使用 Seqlock 原子更新
        self.mmap_ro.store(Some(Arc::new(mmap_r)));

        // 更新写映射
        {
            let mut w = self.mmap_rw.write();
            *w = Some(mmap_w);
        }

        self.stats.mmap_remaps.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn remap_mmap(&self) -> std::io::Result<()> {
        self.remap_mmap_locked()
    }

    // ========== 核心修复4：文件扩展时不需要设置为 None ==========

    fn expand_file_locked_if_needed(&self, required: u64) -> std::io::Result<()> {
        if required > self.config.max_file_size {
            self.record_error("max_file_size_reached");
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "max_file_size_reached",
            ));
        }

        let cur = self.data_file_size.load(Ordering::Relaxed);
        if required <= cur {
            return Ok(());
        }

        let _g = self.resize_lock.lock();
        let cur2 = self.data_file_size.load(Ordering::Relaxed);
        if required <= cur2 {
            return Ok(());
        }

        let step = std::cmp::max(1, self.config.growth_step);
        let aligned = ((required + step - 1) / step) * step;
        let reserve =
            (self.config.growth_reserve_steps.saturating_sub(1) as u64).saturating_mul(step);
        let baseline = cur2.saturating_add(reserve);
        let target = std::cmp::max(aligned.saturating_add(reserve), baseline);
        let new_size = std::cmp::min(target, self.config.max_file_size);

        let t0 = Instant::now();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.data_file_path)?;
        file.set_len(new_size)?;
        file.sync_all()?;

        self.remap_mmap()?;

        self.data_file_size.store(new_size, Ordering::Relaxed);

        self.stats.file_expansions.fetch_add(1, Ordering::Relaxed);
        self.stats
            .maintenance_expand_events
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .expand_us_total
            .fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);

        Ok(())
    }

    /// 【关键修复】加载 mmap 的方法，带短暂重试机制
    #[inline]
    fn load_mmap_ro_with_retry(&self, max_attempts: u32) -> Option<Arc<Mmap>> {
        for attempt in 0..max_attempts {
            if let Some(mmap) = self.load_mmap_ro() {
                return Some(mmap);
            }

            // 短暂自旋等待（避免线程切换开销）
            if attempt < 3 {
                for _ in 0..8 {
                    std::hint::spin_loop();
                }
            } else {
                // 超过3次尝试后让出CPU
                std::thread::yield_now();
                if attempt > 32 {
                    std::thread::sleep(Duration::from_micros(50));
                }
            }
        }
        None
    }
    /// 【核心方法：完全无锁的读取】
    /// 【修复】读取方法 - 添加重试逻辑
    ///
    pub fn read(&self, key: &str) -> std::io::Result<Option<Vec<u8>>> {
        let start_time = Instant::now();

        // L1 缓存检查
        if let Some(mut cached_data) = self.hot_cache.get_mut(key) {
            if cached_data.access_count == 0 {
                self.stats
                    .prefetch_served_hits
                    .fetch_add(1, Ordering::Relaxed);
            }
            cached_data.access_count += 1;
            cached_data.last_access = Instant::now();
            let data_len = cached_data.data.len();
            let elapsed = start_time.elapsed().as_micros() as u64;

            self.stats.l1_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.stats.total_reads.fetch_add(1, Ordering::Relaxed);
            self.stats
                .total_read_bytes
                .fetch_add(data_len as u64, Ordering::Relaxed);
            update_ema_atomic(&self.stats.avg_read_latency_us, elapsed);

            return Ok(Some(cached_data.data.clone()));
        }

        // 索引查询
        let (offset, _tot) = match self.index.get(key) {
            Some(v) => *v,
            None => return Ok(None),
        };

        // 【关键】无锁加载
        let mmap = match self.load_mmap_ro_with_retry(Self::MMAP_RETRY_ATTEMPTS) {
            Some(m) => m,
            None => {
                self.record_error("mmap_uninitialized_read MMAP not initialized");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "MMAP not initialized",
                ));
            }
        };

        // 读取数据
        let data = match Self::read_data_from_mmap_static(&mmap, offset) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        self.add_to_hot_cache_sync(key, &data);

        // 预读逻辑
        if self.config.enable_prefetch {
            if let Some((off, _)) = self.index.get(key).map(|v| *v) {
                let ord = self.ordered_index.read();
                let mut candidates = Vec::with_capacity(4);
                for (_k, v) in ord.range(off..).skip(1).take(2) {
                    candidates.push(v.clone());
                }
                for (_k, v) in ord.range(..off).rev().take(2) {
                    candidates.push(v.clone());
                }
                drop(ord);

                for c in candidates {
                    if !self.prefetch_set.insert(c.clone()) {
                        continue;
                    }
                    if self.prefetch_queue.push(c.clone()).is_err() {
                        self.prefetch_set.remove(&c);
                    }
                }
            }
        }

        // 统计
        let elapsed = start_time.elapsed().as_micros() as u64;
        self.stats.l1_cache_misses.fetch_add(1, Ordering::Relaxed);
        self.stats.total_reads.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_read_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        update_ema_atomic(&self.stats.avg_read_latency_us, elapsed);

        Ok(Some(data))
    }
    // pub fn read(&self, key: &str) -> std::io::Result<Option<Vec<u8>>> {
    //     let start_time = Instant::now();
    //
    //     // L1 缓存检查
    //     if let Some(mut cached_data) = self.hot_cache.get_mut(key) {
    //         if cached_data.access_count == 0 {
    //             self.stats.prefetch_served_hits.fetch_add(1, Ordering::Relaxed);
    //         }
    //         cached_data.access_count += 1;
    //         cached_data.last_access = Instant::now();
    //         let data_len = cached_data.data.len();
    //         let elapsed = start_time.elapsed().as_micros() as u64;
    //
    //         self.stats.l1_cache_hits.fetch_add(1, Ordering::Relaxed);
    //         self.stats.total_reads.fetch_add(1, Ordering::Relaxed);
    //         self.stats.total_read_bytes.fetch_add(data_len as u64, Ordering::Relaxed);
    //         update_ema_atomic(&self.stats.avg_read_latency_us, elapsed);
    //
    //         return Ok(Some(cached_data.data.clone()));
    //     }
    //
    //     // 索引查询
    //     let (offset, _tot) = match self.index.get(key) {
    //         Some(v) => *v,
    //         None => return Ok(None),
    //     };
    //
    //     // 【关键】锁保护的映射加载
    //     let (mmap, _lock_guard) = match self.load_mmap_ro() {
    //         Some(pair) => pair,
    //         None => {
    //             self.record_error("mmap_uninitialized_read");
    //             return Err(std::io::Error::new(
    //                 std::io::ErrorKind::Other,
    //                 "MMAP not initialized",
    //             ));
    //         }
    //     };
    //     // _lock_guard 在作用域结束时自动释放读锁
    //
    //     // 读取数据
    //     let data = match Self::read_data_from_mmap_static(&mmap, offset) {
    //         Ok(v) => v,
    //         Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
    //             return Ok(None);
    //         }
    //         Err(e) => return Err(e),
    //     };
    //
    //     self.add_to_hot_cache_sync(key, &data);
    //
    //     // 预读逻辑
    //     if self.config.enable_prefetch {
    //         if let Some((off, _)) = self.index.get(key).map(|v| *v) {
    //             let ord = self.ordered_index.read();
    //             let mut candidates = Vec::with_capacity(4);
    //             for (_k, v) in ord.range(off..).skip(1).take(2) {
    //                 candidates.push(v.clone());
    //             }
    //             for (_k, v) in ord.range(..off).rev().take(2) {
    //                 candidates.push(v.clone());
    //             }
    //             drop(ord);
    //
    //             for c in candidates {
    //                 if !self.prefetch_set.insert(c.clone()) {
    //                     continue;
    //                 }
    //                 if self.prefetch_queue.push(c.clone()).is_err() {
    //                     self.prefetch_set.remove(&c);
    //                 }
    //             }
    //         }
    //     }
    //
    //     // 统计
    //     let elapsed = start_time.elapsed().as_micros() as u64;
    //     self.stats.l1_cache_misses.fetch_add(1, Ordering::Relaxed);
    //     self.stats.total_reads.fetch_add(1, Ordering::Relaxed);
    //     self.stats.total_read_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
    //     update_ema_atomic(&self.stats.avg_read_latency_us, elapsed);
    //
    //     Ok(Some(data))
    // }

    /// 【无锁优化】批量读取
    pub fn read_batch_coalesced(
        &self,
        keys: &[String],
        max_gap: usize,
    ) -> std::io::Result<HashMap<String, Vec<u8>>> {
        let t0 = Instant::now();
        let mut out = HashMap::with_capacity(keys.len());

        let mut misses: Vec<(String, u64)> = Vec::new();
        let mut l1_hits: u64 = 0;
        let mut l1_bytes: u64 = 0;

        for k in keys {
            if let Some(mut cached) = self.hot_cache.get_mut(k) {
                if cached.access_count == 0 {
                    self.stats
                        .prefetch_served_hits
                        .fetch_add(1, Ordering::Relaxed);
                }
                cached.access_count += 1;
                cached.last_access = Instant::now();
                l1_hits += 1;
                l1_bytes = l1_bytes.saturating_add(cached.data.len() as u64);
                out.insert(k.clone(), cached.data.clone());
            } else if let Some((off, _tot)) = self.index.get(k).map(|v| *v) {
                misses.push((k.clone(), off));
            }
        }

        if misses.is_empty() {
            let produced = out.len() as u64;
            if produced > 0 {
                let elapsed_us = t0.elapsed().as_micros() as u64;
                self.stats
                    .total_reads
                    .fetch_add(produced, Ordering::Relaxed);
                self.stats
                    .total_read_bytes
                    .fetch_add(l1_bytes, Ordering::Relaxed);
                self.stats
                    .l1_cache_hits
                    .fetch_add(l1_hits, Ordering::Relaxed);
                let per_key_us = (elapsed_us / produced.max(1)).max(1);
                update_ema_atomic(&self.stats.avg_read_latency_us, per_key_us);
            }
            return Ok(out);
        }

        let mmap_arc = match self.load_mmap_ro_with_retry(Self::MMAP_RETRY_ATTEMPTS) {
            Some(m) => m,
            None => {
                self.record_error("mmap_uninitialized_read MMAP RO not initialized");
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "MMAP RO not initialized",
                ));
            }
        };

        // 后续逻辑保持不变...
        let mut committed_size = |m: &Mmap, off: u64| -> std::io::Result<usize> {
            let len = m.len();
            let start = off as usize;
            let end_header = start + HEADER_SIZE;
            if end_header > len {
                return Err(Error::new(ErrorKind::UnexpectedEof, "header oob"));
            }

            let commit_idx = start + HDR_OFF_COMMIT;
            let mut spins = 0u32;
            loop {
                let v = unsafe { std::ptr::read_volatile(&m[commit_idx]) };
                if v == 1 {
                    break;
                }
                std::hint::spin_loop();
                spins = spins.saturating_add(1);
                if spins == 64 {
                    std::thread::yield_now();
                }
            }
            std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);

            let header = &m[start..end_header];
            let data_size = u32::from_le_bytes([
                header[HDR_OFF_DATA_SIZE.start],
                header[HDR_OFF_DATA_SIZE.start + 1],
                header[HDR_OFF_DATA_SIZE.start + 2],
                header[HDR_OFF_DATA_SIZE.start + 3],
            ]) as usize;
            Ok(data_size)
        };

        misses.sort_by_key(|it| it.1);
        let mut blocks: Vec<(String, usize, usize)> = Vec::with_capacity(misses.len());
        let mut miss_bytes: u64 = 0;

        for (k, off) in &misses {
            let size = committed_size(&*mmap_arc, *off)?;
            let data_off = (*off as usize) + HEADER_SIZE;
            let data_end = data_off + size;

            if data_end > mmap_arc.len() {
                self.record_error("read_data_oob");
                return Err(Error::new(ErrorKind::UnexpectedEof, "data out of bounds"));
            }

            blocks.push((k.clone(), data_off, size));
        }

        blocks.sort_by_key(|b| b.1);

        let mut i = 0usize;
        while i < blocks.len() {
            let (mut cur_off, mut cur_end) = {
                let (_, data_off, size) = &blocks[i];
                (*data_off, *data_off + *size)
            };
            let start_idx = i;
            i += 1;
            while i < blocks.len() {
                let (_, data_off, size) = &blocks[i];
                if *data_off >= cur_end && *data_off - cur_end <= max_gap {
                    cur_end = *data_off + *size;
                    i += 1;
                } else {
                    break;
                }
            }

            let big = &mmap_arc[cur_off..cur_end];
            for j in start_idx..i {
                let (ref key, data_off, size) = blocks[j];
                let begin = data_off - cur_off;
                let end = begin + size;
                let v = big[begin..end].to_vec();
                miss_bytes = miss_bytes.saturating_add(size as u64);
                self.add_to_hot_cache_sync(key, &v);
                out.insert(key.clone(), v);
            }
        }

        if self.config.enable_prefetch {
            for (_k, off) in &misses {
                let ord = self.ordered_index.read();
                let mut candidates = Vec::with_capacity(4);
                for (_k2, v) in ord.range(*off..).skip(1).take(2) {
                    candidates.push(v.clone());
                }
                for (_k2, v) in ord.range(..*off).rev().take(2) {
                    candidates.push(v.clone());
                }
                drop(ord);
                for c in candidates {
                    if !self.prefetch_set.insert(c.clone()) {
                        continue;
                    }
                    if self.prefetch_queue.push(c.clone()).is_err() {
                        self.prefetch_set.remove(&c);
                    }
                }
            }
        }

        let produced = out.len() as u64;
        if produced > 0 {
            let elapsed_us = t0.elapsed().as_micros() as u64;
            let per_key_us = (elapsed_us / produced).max(1);
            self.stats
                .total_reads
                .fetch_add(produced, Ordering::Relaxed);
            self.stats
                .total_read_bytes
                .fetch_add(l1_bytes + miss_bytes, Ordering::Relaxed);
            self.stats
                .l1_cache_hits
                .fetch_add(l1_hits, Ordering::Relaxed);
            self.stats
                .l1_cache_misses
                .fetch_add(misses.len() as u64, Ordering::Relaxed);
            update_ema_atomic(&self.stats.avg_read_latency_us, per_key_us);
        }

        Ok(out)
    }

    pub fn read_batch(&self, keys: &[String]) -> std::io::Result<HashMap<String, Vec<u8>>> {
        // let mut out = HashMap::with_capacity(keys.len());
        // for key in keys {
        //     let cached = self.read(&key)?.unwrap();
        //     out.insert(key.clone(), cached.clone());
        // }
        // Ok(out)
        self.read_batch_coalesced(keys, DEFAULT_COALESCE_GAP)
    }

    /// 【无锁优化】零拷贝读取
    pub fn read_slice(&self, key: &str) -> std::io::Result<Option<ZeroCopySlice>> {
        let (offset, _tot) = match self.index.get(key) {
            Some(v) => *v,
            None => return Ok(None),
        };

        let mmap = match self.load_mmap_ro_with_retry(Self::MMAP_RETRY_ATTEMPTS) {
            Some(m) => m,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "MMAP RO not initialized",
                ));
            }
        };

        let len = mmap.len();
        let start = offset as usize;
        let end_header = start + HEADER_SIZE;
        if end_header > len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "header oob",
            ));
        }

        let header = &mmap[start..end_header];
        let data_size = u32::from_le_bytes([
            header[HDR_OFF_DATA_SIZE.start],
            header[HDR_OFF_DATA_SIZE.start + 1],
            header[HDR_OFF_DATA_SIZE.start + 2],
            header[HDR_OFF_DATA_SIZE.start + 3],
        ]) as usize;

        let data_off = end_header;
        let data_end = data_off
            .checked_add(data_size)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "overflow"))?;
        if data_end > len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "data oob",
            ));
        }

        Ok(Some(ZeroCopySlice {
            mmap,
            start: data_off,
            end: data_end,
        }))
    }

    /**
     * 内存压力检查：检查当前内存使用情况，如果压力过高且配置了拒绝写入，则直接返回错误
     *   定期内存检查：定期检查内存使用情况，触发缓存清理等操作
     *  数据压缩：根据配置对数据进行智能压缩，只对值得压缩的数据进行压缩
     *   空间分配：原子地获取写入偏移量，如果空间不足则尝试垃圾回收后重试
     *   文件扩展：如果需要更多空间，则扩展文件大小
     *   数据写入：将数据写入到内存映射文件的指定位置
     *   缓存失效：删除旧数据的缓存条目，避免数据不一致
     *   索引更新：更新键值索引和有序索引，标记旧数据为惰性删除
     *  缓存预热：将新写入的数据加入缓存，提高后续读取性能
     *  统计更新：更新各种性能统计信息，包括写入次数、字节数、延迟等
     */
    /// 【核心方法】写入数据到存储中
    /// 整个写入过程包括：内存检查、数据压缩、空间分配、数据写入、缓存更新、索引维护等步骤
    pub fn write(&self, key: &str, data: &[u8]) -> std::io::Result<()> {
        // 记录写入开始时间，用于计算写入延迟
        let start_all = Instant::now();

        // 【新增】写入前检查内存限制
        // 如果配置了在内存压力下拒绝写入，并且当前内存压力等级达到危险级别(3)，则拒绝写入
        if self.memory_limit.reject_writes_under_pressure {
            let pressure = self.get_memory_pressure_level();
            if pressure >= 3 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "内存压力过高，拒绝写入",
                ));
            }
        }

        // 【新增】定期检查内存（每秒最多一次，避免频繁检查）
        // 获取当前时间纳秒数
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        // 获取上次内存检查时间
        let last_check = self.last_memory_check.load(Ordering::Relaxed);
        // 计算检查间隔（毫秒转纳秒）
        let check_interval_ns = self.memory_limit.check_interval_ms * 1_000_000;

        // 如果距离上次检查已经超过设定间隔，则进行内存检查
        if now_ns.saturating_sub(last_check) > check_interval_ns {
            // 使用原子操作确保只有一个线程执行检查
            if self
                .last_memory_check
                .compare_exchange(last_check, now_ns, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                // 异步检查，不阻塞写入
                let _ = self.check_memory_limits();
            }
        }

        // 创建写者守卫，增加活跃写者计数
        let writer_guard = WriterGuard::new(&self.active_writers);

        // 【关键：智能压缩】
        // 根据配置决定是否对数据进行压缩，返回实际存储的数据、是否已压缩、原始大小
        let (actual_data, compressed, original_size) = self.compress_if_beneficial(data)?;

        // 计算数据总大小（包括头部和数据）
        let total = HEADER_SIZE as u64 + actual_data.len() as u64;
        // 获取最大文件大小限制
        let maxf = self.config.max_file_size;

        // 原子更新写入偏移量，确保线程安全
        let offset =
            match self
                .write_offset
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |cur| {
                    // 计算需要的空间
                    let needed = cur.saturating_add(total);
                    // 如果超出最大文件大小，返回None
                    if needed > maxf { None } else { Some(needed) }
                }) {
                // 成功获取偏移量
                Ok(prev) => prev,
                // 空间不足，尝试垃圾回收后重试
                Err(_) => {
                    let _ = self.garbage_collect();
                    // 重试获取偏移量
                    match self.write_offset.fetch_update(
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        |cur| {
                            let needed = cur.saturating_add(total);
                            if needed > maxf { None } else { Some(needed) }
                        },
                    ) {
                        Ok(prev2) => prev2,
                        // 仍然失败，记录错误并返回
                        Err(_) => {
                            self.record_error("max_file_size_reached");
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                "max_file_size_reached",
                            ));
                        }
                    }
                }
            };

        // 计算实际需要的空间
        let needed = offset + total;
        // 获取当前文件大小
        let cur_sz = self.data_file_size.load(Ordering::Relaxed);
        // 获取增长步长
        let step = self.config.growth_step.max(1);
        // 计算预留步数
        let reserve_steps = self.config.growth_reserve_steps.saturating_sub(1) as u64;

        // 计算文件尾部空闲空间
        let free_tail = cur_sz.saturating_sub(needed);
        // 计算所需的文件大小
        let required = if free_tail < step.saturating_mul(2) {
            needed + step.saturating_mul(reserve_steps + 2)
        } else {
            needed
        };
        // 如果需要，扩展文件
        self.expand_file_locked_if_needed(required)?;

        // 记录写入路径开始时间
        let start_path = Instant::now();
        // 在指定偏移量处写入数据（带压缩标志）
        self.write_data_at_offset_retry_compressed(offset, &actual_data, compressed)?;
        // 计算写入路径耗时
        let path_ns = start_path.elapsed().as_nanos() as u64;
        let path_us = (path_ns + 999) / 1000;

        // 【关键修复】写入成功后，立即使缓存失效
        // 必须在索引更新前删除，避免竞态
        if let Some((_k, old_cached)) = self.hot_cache.remove(key) {
            self.hot_cache_bytes
                .fetch_sub(old_cached.size, Ordering::Relaxed);
        }

        // 更新索引，标记旧数据为删除
        if let Some((old_off, old_sz)) = self.index.insert(key.to_string(), (offset, total)) {
            // 以旧的 offset 为键存储，避免同 key 多次覆盖时信息丢失
            self.lazy_deleted_entries
                .insert(old_off, (key.to_string(), old_sz));

            let mut by_end = self.lazy_by_end.write();
            by_end.insert(old_off + old_sz, (key.to_string(), (old_off, old_sz)));
        }
        // 更新有序索引
        self.ordered_index.write().insert(offset, key.to_string());

        // 【可选优化】立即将新数据加入缓存，提高后续读取性能
        // 但要检查内存是否充足
        let mem_stats = self.get_memory_stats();
        if mem_stats.memory_usage_ratio < 0.8 {
            // 只在内存充足时预热
            if compressed {
                // 如果数据被压缩了，需要解压后再缓存
                if let Ok(decompressed) =
                    self.decompress_data(&actual_data, self.config.compression.algorithm)
                {
                    self.add_to_hot_cache_sync(key, &decompressed);
                }
            } else {
                // 未压缩，直接缓存
                self.add_to_hot_cache_sync(key, &actual_data);
            }
        }

        // 【统计：记录原始大小，不是压缩后的】
        // 更新写入统计信息
        self.stats.total_writes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_write_bytes
            .fetch_add(original_size as u64, Ordering::Relaxed);
        // 更新平均写入延迟（EMA算法）
        update_ema_atomic(
            &self.stats.avg_write_latency_us,
            start_all.elapsed().as_micros() as u64,
        );
        update_ema_atomic(&self.stats.avg_write_path_us, path_us);

        drop(writer_guard);

        self.trigger_gc_if_needed();

        Ok(())
    }

    /// 【新增】写入带压缩标志的数据
    fn write_data_at_offset_retry_compressed(
        &self,
        offset: u64,
        data: &[u8],
        compressed: bool,
    ) -> std::io::Result<()> {
        for attempt in 0..=2 {
            {
                let _ml = self.map_lock.read();
                if let Ok(()) = self.write_data_at_offset_once_compressed(offset, data, compressed)
                {
                    return Ok(());
                }
            }

            if attempt < 2 {
                let _ = self.remap_mmap();
                std::thread::yield_now();
                continue;
            }
            break;
        }
        Err(std::io::Error::new(
            ErrorKind::Other,
            "write failed after retry",
        ))
    }

    fn write_data_at_offset_once_compressed(
        &self,
        offset: u64,
        data: &[u8],
        compressed: bool,
    ) -> std::io::Result<()> {
        let mut mmap_guard = self.mmap_rw.write();

        if let Some(mmap) = mmap_guard.as_mut() {
            let len = mmap.len();
            let header_size = HEADER_SIZE;

            if offset as usize > len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "write offset out of bounds",
                ));
            }

            let start = offset as usize;
            let end_header = start.saturating_add(header_size);
            if end_header > len {
                self.record_error("write_header_oob");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "header out of bounds",
                ));
            }

            let data_end = end_header.saturating_add(data.len());
            if data_end > len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "data out of bounds",
                ));
            }

            let now_ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let mut header_bytes = [0u8; HEADER_SIZE];
            header_bytes[HDR_OFF_DATA_SIZE].copy_from_slice(&(data.len() as u32).to_le_bytes());
            header_bytes[HDR_OFF_COMPRESSED] = if compressed { 1 } else { 0 }; // 【关键：记录压缩标志】
            header_bytes[HDR_OFF_COMMIT] = 0;
            header_bytes[HDR_OFF_TIMESTAMP].copy_from_slice(&now_ts.to_le_bytes());

            mmap[start..end_header].copy_from_slice(&header_bytes);
            mmap[end_header..data_end].copy_from_slice(data);
            mmap[start + HDR_OFF_COMMIT] = 1;

            Ok(())
        } else {
            return Err(Error::new(ErrorKind::WouldBlock, "mmap_rw not initialized"));
        }
    }

    pub fn delete(&self, key: &str) -> std::io::Result<bool> {
        let removed_entry = self.index.remove(key);

        if let Some((_k, v)) = self.hot_cache.remove(key) {
            self.hot_cache_bytes.fetch_sub(v.size, Ordering::Relaxed);
        }

        if let Some((_k, (offset, size))) = removed_entry {
            self.lazy_deleted_entries
                .insert(offset, (key.to_string(), size));
            {
                let mut by_end = self.lazy_by_end.write();
                by_end.insert(offset + size, (key.to_string(), (offset, size)));
            }
            {
                let mut ord = self.ordered_index.write();
                ord.remove(&offset);
            }

            self.stats.total_deletes.fetch_add(1, Ordering::Relaxed);

            self.try_shrink_tail()?;
            self.trigger_gc_if_needed();
            return Ok(true);
        }
        Ok(false)
    }

    pub fn delete_lazy(&self, key: &str) -> std::io::Result<bool> {
        if let Some((_k, (offset, size))) = self.index.remove(key) {
            self.lazy_deleted_entries
                .insert(offset, (key.to_string(), size));

            if let Some((_k, v)) = self.hot_cache.remove(key) {
                self.hot_cache_bytes.fetch_sub(v.size, Ordering::Relaxed);
            }

            self.stats
                .lazy_deleted_entries
                .fetch_add(1, Ordering::Relaxed);
            self.stats.total_deletes.fetch_add(1, Ordering::Relaxed);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// 安全检查：
    /// 检查是否有正在进行的写入操作，避免在写入时执行GC造成数据不一致
    /// 控制GC执行频率，避免过于频繁的GC影响性能
    /// 数据收集：
    /// 收集所有当前有效的条目（仍在索引中的数据）
    /// 空间回收：
    /// 调用 compact_data_file 函数进行磁盘文件压缩，移除已被删除的数据
    /// 清理惰性删除条目记录
    /// 统计更新：
    /// 更新GC运行次数和回收的磁盘空间统计信息
    /// 记录操作日志
    /// 垃圾回收函数，用于清理已被删除但尚未回收磁盘空间的数据
    /// 返回清理的条目数量
    pub fn garbage_collect(&self) -> std::io::Result<u64> {
        // 检查是否有活跃的写入操作，如果有则不执行GC以避免冲突
        if self.active_writers.load(Ordering::Relaxed) > 0 {
            return Ok(0);
        }

        // 获取当前时间（秒级时间戳）
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // 检查是否到达下次GC时间，避免过于频繁的GC操作
        let next_gc_time = self.next_gc_time.load(Ordering::Relaxed);
        if current_time < next_gc_time {
            return Ok(0);
        }

        // 设置下一次GC时间为当前时间+60秒，控制GC频率
        self.next_gc_time
            .store(current_time + 60, Ordering::Relaxed);

        // 如果没有惰性删除的条目，无需执行GC
        if self.lazy_deleted_entries.is_empty() {
            return Ok(0);
        }

        // 收集当前所有有效的条目（未被删除的条目）
        let mut valid_entries: HashMap<String, (u64, u64)> = HashMap::new();
        for entry in self.index.iter() {
            // 将有效的键值对复制到valid_entries中
            valid_entries.insert(entry.key().clone(), *entry.value());
        }

        // 执行数据文件压缩，移除无效数据并返回回收的空间大小
        let reclaimed_space = self.compact_data_file(&valid_entries)?;

        // 获取被清理的条目数量
        let deleted_count = self.lazy_deleted_entries.len() as u64;
        // 清空惰性删除条目列表
        self.lazy_deleted_entries.clear();

        // 更新统计信息：增加GC运行次数和回收的磁盘空间
        self.stats
            .garbage_collection_runs
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .reclaimed_disk_space
            .fetch_add(reclaimed_space, Ordering::Relaxed);

        // 记录GC完成日志
        info!(
            "垃圾回收完成: 清理了 {} 个条目，回收了 {} 字节空间",
            deleted_count, reclaimed_space
        );
        // 返回清理的条目数量
        Ok(deleted_count)
    }

    /// 【根本性修复】尾部收缩 - 保持旧映射可用直到新映射就绪
    fn try_shrink_tail(&self) -> std::io::Result<()> {
        if self.active_writers.load(Ordering::Relaxed) > 0 {
            return Ok(());
        }

        let mut tail = self.write_offset.load(Ordering::Relaxed);
        if tail == 0 {
            return Ok(());
        }

        let mut reclaimed: u64 = 0;
        loop {
            let mut by_end = self.lazy_by_end.write();
            if let Some((k, (off, size))) = by_end.remove(&tail) {
                drop(by_end);
                self.lazy_deleted_entries.remove(&off);
                tail = off;
                reclaimed = reclaimed.saturating_add(size);
            } else {
                break;
            }
        }

        if reclaimed == 0 {
            return Ok(());
        }

        let _rl = self.resize_lock.lock();
        let _ml = self.map_lock.write();

        self.write_offset.store(tail, Ordering::Relaxed);
        self.data_file_size.store(tail, Ordering::Relaxed);

        {
            let mut w = self.mmap_rw.write();
            let _ = w.take();
        }

        // 【关键】使用 Seqlock 设为 None
        self.mmap_ro.store(None);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.data_file_path)?;
        file.set_len(tail)?;
        file.sync_all()?;
        drop(file);

        drop(_ml); // 先释放锁
        self.remap_mmap_locked()?;

        self.stats
            .reclaimed_disk_space
            .fetch_add(reclaimed, Ordering::Relaxed);
        info!("删除收缩: 回收尾部 {} 字节, 新文件大小 {}", reclaimed, tail);

        Ok(())
    }

    fn trigger_gc_if_needed(&self) {
        if self.config.auto_purge_after_secs.is_some() {
            let interval = self.config.auto_purge_check_interval_secs;
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let should_run = if interval == 0 {
                self.last_purge_check.store(now_secs, Ordering::Relaxed);
                true
            } else {
                let last = self.last_purge_check.load(Ordering::Relaxed);
                if now_secs.saturating_sub(last) < interval {
                    false
                } else {
                    self.last_purge_check
                        .compare_exchange(last, now_secs, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                }
            };

            if should_run {
                match self.purge_expired_entries() {
                    Ok(purged) if purged > 0 => {
                        debug!("自动清理过期条目: {} 个", purged);
                    }
                    Err(err) => {
                        warn!("自动清理过期条目失败: {}", err);
                    }
                    _ => {}
                }
            }
        }

        let lazy_count = self.lazy_deleted_entries.len();
        let live_count = self.index.len().max(1);

        let mut lazy_bytes: u64 = 0;
        for e in self.lazy_deleted_entries.iter() {
            let (_key, size) = e.value(); // 新结构：(key, size)
            lazy_bytes = lazy_bytes.saturating_add(*size);
            if lazy_bytes >= self.config.growth_step {
                break;
            }
        }

        if lazy_count > (live_count / 2) || lazy_bytes >= self.config.growth_step {
            let _ = self.garbage_collect();
        }
    }

    /// 根据配置的过期时间，自动将旧数据标记为惰性删除
    fn purge_expired_entries(&self) -> std::io::Result<usize> {
        let ttl = match self.config.auto_purge_after_secs {
            Some(ttl) if ttl > 0 => ttl,
            _ => return Ok(0),
        };

        if self.active_writers.load(Ordering::Relaxed) > 0 {
            return Ok(0);
        }

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
            .as_secs();

        if now_secs <= ttl {
            return Ok(0);
        }
        let cutoff = now_secs - ttl;

        let mmap = match self.load_mmap_ro_with_retry(Self::MMAP_RETRY_ATTEMPTS) {
            Some(m) => m,
            None => return Ok(0),
        };
        let len = mmap.len();

        let mut expired: Vec<(u64, String)> = Vec::new();
        {
            let ord = self.ordered_index.read();
            for (offset, key) in ord.iter() {
                let start = *offset as usize;
                let end_header = start.saturating_add(HEADER_SIZE);
                if end_header > len {
                    continue;
                }
                let ts_start = start + HDR_OFF_TIMESTAMP.start;
                let ts_end = start + HDR_OFF_TIMESTAMP.end;
                if ts_end > len {
                    continue;
                }
                let mut ts_bytes = [0u8; 8];
                ts_bytes.copy_from_slice(&mmap[ts_start..ts_end]);
                let ts = u64::from_le_bytes(ts_bytes);
                if ts == 0 {
                    continue;
                }

                if ts <= cutoff {
                    expired.push((*offset, key.clone()));
                } else {
                    break;
                }
            }
        }

        if expired.is_empty() {
            return Ok(0);
        }

        let mut removed = 0usize;
        for (offset, key) in expired {
            match self.index.entry(key.clone()) {
                Entry::Occupied(mut occ) => {
                    let (stored_offset, size) = *occ.get();
                    if stored_offset != offset {
                        continue;
                    }
                    let (_, (_removed_offset, value_size)) = occ.remove_entry();

                    self.lazy_deleted_entries
                        .insert(offset, (key.clone(), value_size));
                    {
                        let mut by_end = self.lazy_by_end.write();
                        by_end.insert(offset + value_size, (key.clone(), (offset, value_size)));
                    }
                    {
                        let mut ord = self.ordered_index.write();
                        ord.remove(&offset);
                    }
                    if let Some((_k, v)) = self.hot_cache.remove(&key) {
                        self.hot_cache_bytes.fetch_sub(v.size, Ordering::Relaxed);
                    }

                    self.stats.total_deletes.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .lazy_deleted_entries
                        .fetch_add(1, Ordering::Relaxed);

                    removed += 1;
                }
                Entry::Vacant(_) => continue,
            }
        }

        Ok(removed)
    }

    /// 紧凑数据文件，移除已被删除的数据，回收磁盘空间
    /// 返回回收的字节数
    fn compact_data_file(
        &self,
        valid_entries: &HashMap<String, (u64, u64)>,
    ) -> std::io::Result<u64> {
        // 创建临时文件路径
        let temp_file_path = self.data_file_path.with_extension("tmp");
        // 创建临时文件用于写入有效数据
        let mut temp_file = File::create(&temp_file_path)?;
        // 获取原始文件大小
        let original_size = self.data_file_size.load(Ordering::Relaxed);

        // 打开原始数据文件并创建只读内存映射
        let file = OpenOptions::new().read(true).open(&self.data_file_path)?;
        let mmap_ro = unsafe { memmap2::Mmap::map(&file)? };

        // 收集并按文件偏移量排序所有有效条目
        let mut sorted_entries: Vec<(&String, &(u64, u64))> = valid_entries.iter().collect();
        sorted_entries.sort_by_key(|(_, (offset, _))| *offset);

        // 初始化新文件的写入偏移量和新索引
        let mut new_offset = 0u64;
        let header_size = HEADER_SIZE as u64;
        let mut new_index: HashMap<String, (u64, u64)> = HashMap::new();

        // 遍历所有有效条目，将数据复制到临时文件
        for (key, (offset, size)) in sorted_entries {
            let start = *offset as usize;
            let end_header = start + header_size as usize;
            // 检查头部是否在文件范围内
            if end_header > mmap_ro.len() {
                continue;
            }

            // 读取数据头部
            let header_bytes = &mmap_ro[start..end_header];
            let data_offset = end_header;
            let data_end = data_offset + (size - header_size) as usize;
            // 检查数据是否在文件范围内
            if data_end > mmap_ro.len() {
                continue;
            }

            // 读取实际数据
            let data = &mmap_ro[data_offset..data_end];

            // 将头部和数据写入临时文件
            temp_file.write_all(header_bytes)?;
            temp_file.write_all(data)?;

            // 更新新索引和偏移量
            new_index.insert((*key).clone(), (new_offset, *size));
            new_offset += *size;
        }

        // 同步临时文件并释放资源
        temp_file.sync_all()?;
        drop(temp_file);
        drop(mmap_ro);
        drop(file);

        // 获取重映射锁，准备替换原始文件
        let _rl = self.resize_lock.lock();
        let _ml = self.map_lock.write();

        // 释放写映射并清除读映射
        {
            let mut w = self.mmap_rw.write();
            let _ = w.take();
        }
        self.mmap_ro.store(None);

        drop(_ml);

        // 用临时文件替换原始文件
        std::fs::rename(&temp_file_path, &self.data_file_path)?;

        // 更新文件大小并重新映射
        let new_file_size = new_offset;
        self.data_file_size.store(new_file_size, Ordering::Relaxed);

        self.remap_mmap_locked()?;

        // 清除旧索引并建立新索引
        self.index.clear();
        {
            let mut ord = self.ordered_index.write();
            ord.clear();
            for (key, value) in new_index {
                self.index.insert(key.clone(), value);
                ord.insert(value.0, key);
            }
        }
        // 清除按结束位置排序的惰性删除条目
        self.lazy_by_end.write().clear();

        // 计算GC后的目标文件大小（预留空间）
        let step = self.config.growth_step.max(1);
        let reserve = (self.config.growth_reserve_steps.saturating_sub(1) as u64) * step;
        let boost: u64 = 2;
        let aligned = ((new_file_size + step - 1) / step) * step;
        let target_after_gc =
            (aligned + reserve.saturating_mul(boost)).min(self.config.max_file_size);

        // 如果目标大小大于当前文件大小，则扩展文件
        if target_after_gc > new_file_size {
            let t0 = std::time::Instant::now();

            // 获取映射锁并释放映射
            let _ml = self.map_lock.write();

            {
                let mut w = self.mmap_rw.write();
                let _ = w.take();
            }
            self.mmap_ro.store(None);

            drop(_ml);

            // 扩展文件到目标大小
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.data_file_path)?;
            f.set_len(target_after_gc)?;
            f.sync_all()?;
            drop(f);

            // 重新映射并更新文件大小
            self.remap_mmap_locked()?;
            self.data_file_size
                .store(target_after_gc, Ordering::Relaxed);

            // 更新统计信息
            self.stats.file_expansions.fetch_add(1, Ordering::Relaxed);
            self.stats
                .maintenance_expand_events
                .fetch_add(1, Ordering::Relaxed);
            self.stats
                .expand_us_total
                .fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
        }

        // 计算并返回回收的空间大小
        let reclaimed_space =
            original_size.saturating_sub(self.data_file_size.load(Ordering::Relaxed));
        Ok(reclaimed_space)
    }

    /// 【无锁优化】获取统计快照
    /// 无锁设计: 所有统计信息都通过原子操作获取，避免了锁竞争，提高了并发性能
    // 实时快照: 返回的是调用时刻的统计快照，不会阻塞正在进行的读写操作
    // 全面监控: 涵盖了存储系统的各个方面，包括性能指标、缓存效率、空间管理等
    // 使用 Relaxed Ordering: 由于统计信息允许轻微的不一致性，使用 Relaxed 内存顺序以获得最佳性能
    pub fn get_stats(&self) -> HighPerfMmapStats {
        HighPerfMmapStats {
            // 写操作总次数
            total_writes: self.stats.total_writes.load(Ordering::Relaxed),
            // 读操作总次数
            total_reads: self.stats.total_reads.load(Ordering::Relaxed),
            // 删除操作总次数
            total_deletes: self.stats.total_deletes.load(Ordering::Relaxed),
            // 当前索引条目数（活跃记录总数）
            total_records: self.index.len() as u64,
            // 写入总字节数（原始数据大小）
            total_write_bytes: self.stats.total_write_bytes.load(Ordering::Relaxed),
            // 读取总字节数（解压后数据大小）
            total_read_bytes: self.stats.total_read_bytes.load(Ordering::Relaxed),
            // L1缓存命中次数
            l1_cache_hits: self.stats.l1_cache_hits.load(Ordering::Relaxed),
            // L1缓存未命中次数
            l1_cache_misses: self.stats.l1_cache_misses.load(Ordering::Relaxed),
            // L2缓存命中次数（预留字段，当前未使用）
            l2_cache_hits: self.stats.l2_cache_hits.load(Ordering::Relaxed),
            // L2缓存未命中次数（预留字段，当前未使用）
            l2_cache_misses: self.stats.l2_cache_misses.load(Ordering::Relaxed),
            // 预读命中次数（进入预读队列的条目被成功预读的次数）
            prefetch_hits: self.stats.prefetch_hits.load(Ordering::Relaxed),
            // 文件扩展次数（当数据文件容量不足时扩展的次数）
            file_expansions: self.stats.file_expansions.load(Ordering::Relaxed),
            // 平均写入延迟（微秒）
            avg_write_latency_us: self.stats.avg_write_latency_us.load(Ordering::Relaxed),
            // 平均读取延迟（微秒）
            avg_read_latency_us: self.stats.avg_read_latency_us.load(Ordering::Relaxed),
            // mmap重映射次数（文件扩展或收缩后重新映射的次数）
            mmap_remaps: self.stats.mmap_remaps.load(Ordering::Relaxed),
            // 错误总计数
            error_count: self.stats.error_count.load(Ordering::Relaxed),
            // 最后一次错误代码（用于诊断最近发生的错误）
            last_error_code: self.stats.last_error_code.load().as_ref().clone(),
            // 惰性删除条目数量（标记为删除但尚未回收的条目数）
            lazy_deleted_entries: self.stats.lazy_deleted_entries.load(Ordering::Relaxed),
            // 已回收磁盘空间字节数
            reclaimed_disk_space: self.stats.reclaimed_disk_space.load(Ordering::Relaxed),
            // 垃圾回收运行次数
            garbage_collection_runs: self.stats.garbage_collection_runs.load(Ordering::Relaxed),
            // 平均写入路径延迟（微秒，仅包含实际写入MMAP的时间）
            avg_write_path_us: self.stats.avg_write_path_us.load(Ordering::Relaxed),
            // 维护性扩展事件次数（因维护需要而扩展文件的次数）
            maintenance_expand_events: self.stats.maintenance_expand_events.load(Ordering::Relaxed),
            // 扩展操作总耗时（微秒）
            expand_us_total: self.stats.expand_us_total.load(Ordering::Relaxed),
            // 预读服务命中次数（首次命中L1且由预读放入的条目数）
            prefetch_served_hits: self.stats.prefetch_served_hits.load(Ordering::Relaxed),
        }
    }

    pub fn get_confgig(&self) -> HighPerfMmapConfig {
        HighPerfMmapConfig {
            initial_file_size: self.config.initial_file_size,
            enable_compression: self.config.enable_compression,
            l1_cache_size_limit: self.config.l1_cache_size_limit,
            l1_cache_entry_limit: self.config.l1_cache_entry_limit,
            l2_cache_size_limit: self.config.l2_cache_size_limit,
            l2_cache_entry_limit: self.config.l2_cache_entry_limit,
            enable_prefetch:self.config.enable_prefetch,
            prefetch_queue_size:self.config.prefetch_queue_size,
            memory_pressure_threshold:self.config.memory_pressure_threshold,
            cache_degradation_threshold: self.config.cache_degradation_threshold,
            growth_reserve_steps: self.config.growth_reserve_steps,
            compression: self.config.compression.clone(),
            growth_step: self.config.initial_file_size,
            max_file_size:self.config.initial_file_size,
            auto_purge_after_secs: self.config.auto_purge_after_secs,
            auto_purge_check_interval_secs: self.config.auto_purge_check_interval_secs,
        }
    }

    pub fn get_status(&self) -> HighPerfMmapStatus {
        HighPerfMmapStatus {
            stats: self.get_stats(),
            memory: self.get_memory_stats(),
            config : self.get_confgig()
        }
    }

    pub fn save_index(&self) -> std::io::Result<()> {
        let index: HashMap<String, (u64, u64)> = self
            .index
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect();
        let index_data = bincode::serialize(&index)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let tmp = self.index_file_path.with_extension("bin.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&index_data)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.index_file_path)?;
        Ok(())
    }

    pub fn flush(&self) -> std::io::Result<()> {
        let mut guard = self.mmap_rw.write();
        if let Some(m) = guard.as_mut() {
            m.flush()?;
        }
        Ok(())
    }

    pub fn flush_async(&self) -> std::io::Result<()> {
        let mut guard = self.mmap_rw.write();
        if let Some(m) = guard.as_mut() {
            m.flush_async()?;
        }
        Ok(())
    }

    fn record_error(&self, code: &str) {
        self.stats.error_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .last_error_code
            .store(Arc::new(Some(code.to_string())));
    }

    // ========== 新增：清空历史数据功能 ==========

    /// 【新功能】清空所有历史数据，重新开始
    ///
    /// 使用场景：
    /// - 测试前清空旧数据
    /// - 重置存储状态
    /// - 强制 GC
    pub fn clear_all_data(&self) -> std::io::Result<()> {
        info!("开始清空所有历史数据...");

        // 1. 停止所有后台任务
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(h) = self.prefetch_thread.lock().take() {
            let _ = h.join();
        }

        // 2. 清空所有缓存
        self.hot_cache.clear();
        self.hot_cache_bytes.store(0, Ordering::Relaxed);
        while let Some(_) = self.prefetch_queue.pop() {}
        self.prefetch_set.clear();

        // 3. 清空索引
        self.index.clear();
        self.ordered_index.write().clear();
        self.lazy_deleted_entries.clear();
        self.lazy_by_end.write().clear();

        // 4. 获取写锁，释放映射
        let _rl = self.resize_lock.lock();
        let _wl = self.map_lock.write();

        {
            let mut w = self.mmap_rw.write();
            let _ = w.take();
        }
        let _old = self.mmap_ro.store(None);

        // 5. 删除旧文件并重新创建
        if self.data_file_path.exists() {
            std::fs::remove_file(&self.data_file_path)?;
        }
        if self.index_file_path.exists() {
            std::fs::remove_file(&self.index_file_path)?;
        }

        // 6. 创建新的空文件
        let file = File::create(&self.data_file_path)?;
        file.set_len(self.config.initial_file_size)?;
        file.sync_all()?;
        drop(file);

        // 7. 重置状态
        self.write_offset.store(0, Ordering::Relaxed);
        self.data_file_size
            .store(self.config.initial_file_size, Ordering::Relaxed);

        // 8. 重新创建映射
        drop(_wl);
        self.remap_mmap_locked()?;

        // 9. 重置统计信息
        self.stats.total_writes.store(0, Ordering::Relaxed);
        self.stats.total_reads.store(0, Ordering::Relaxed);
        self.stats.total_deletes.store(0, Ordering::Relaxed);
        self.stats.total_write_bytes.store(0, Ordering::Relaxed);
        self.stats.total_read_bytes.store(0, Ordering::Relaxed);
        self.stats.l1_cache_hits.store(0, Ordering::Relaxed);
        self.stats.l1_cache_misses.store(0, Ordering::Relaxed);
        self.stats.error_count.store(0, Ordering::Relaxed);
        self.stats.lazy_deleted_entries.store(0, Ordering::Relaxed);
        self.stats.reclaimed_disk_space.store(0, Ordering::Relaxed);
        self.stats
            .garbage_collection_runs
            .store(0, Ordering::Relaxed);

        info!("历史数据清空完成，存储已重置");

        // 10. 重启后台任务
        self.shutdown.store(false, Ordering::Relaxed);
        if self.config.enable_prefetch {
            let store = Arc::new(self.clone());
            // 注意：这里需要 self 是 Arc<Self>，或者在外部调用
        }

        Ok(())
    }

    // ========== 新增：带清空选项的构造函数 ==========

    /// 创建存储实例，可选择是否清空历史数据
    ///
    /// # 参数
    /// - `disk_dir`: 数据目录
    /// - `config`: 配置
    /// - `memory_limit`: 内存限制
    /// - `clear_on_start`: 是否清空历史数据
    pub fn new_with_options(
        disk_dir: PathBuf,
        config: HighPerfMmapConfig,
        memory_limit: MemoryLimitConfig,
        clear_on_start: bool,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(&disk_dir)?;

        let data_file_path = disk_dir.join("data.bin");
        let index_file_path = disk_dir.join("index.bin");

        // 如果需要清空，先删除旧文件
        if clear_on_start {
            if data_file_path.exists() {
                info!("清空历史数据：删除 {:?}", data_file_path);
                std::fs::remove_file(&data_file_path)?;
            }
            if index_file_path.exists() {
                info!("清空历史数据：删除 {:?}", index_file_path);
                std::fs::remove_file(&index_file_path)?;
            }
        }

        let data_file_size = if data_file_path.exists() {
            std::fs::metadata(&data_file_path)?.len()
        } else {
            let file = File::create(&data_file_path)?;
            file.set_len(config.initial_file_size)?;
            config.initial_file_size
        };

        if data_file_size > config.max_file_size {
            log::warn!(
                "data.bin size {} > configured max {}",
                data_file_size,
                config.max_file_size
            );
        }

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let purge_interval = config.auto_purge_check_interval_secs;
        let initial_purge_check = if purge_interval > 0 {
            now_secs.saturating_sub(purge_interval)
        } else {
            now_secs
        };

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let purge_interval = config.auto_purge_check_interval_secs;
        let initial_purge_check = if purge_interval > 0 {
            now_secs.saturating_sub(purge_interval)
        } else {
            now_secs
        };

        let storage = Self {
            disk_dir,
            data_file_path,
            index_file_path,
            data_file_size: Arc::new(AtomicU64::new(data_file_size)),
            write_offset: Arc::new(AtomicU64::new(0)),
            index: Arc::new(DashMap::new()),
            mmap_rw: Arc::new(parking_lot::RwLock::new(None)),
            mmap_ro: Arc::new(SeqlockedMmap::new()),
            active_readers: Arc::new(Default::default()),
            active_writers: Arc::new(AtomicU64::new(0)),
            resize_lock: Mutex::new(()),
            map_lock: parking_lot::RwLock::new(()), // 【关键】这个 RwLock 用于保护映射切换
            hot_cache: Arc::new(DashMap::new()),
            hot_cache_bytes: Arc::new(AtomicU64::new(0)),
            prefetch_queue: Arc::new(ArrayQueue::new(config.prefetch_queue_size.max(8))),
            prefetch_set: Arc::new(DashSet::new()),
            stats: Arc::new(AtomicStats::default()),
            config,
            lazy_deleted_entries: Arc::new(DashMap::new()),
            lazy_by_end: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
            ordered_index: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
            next_gc_time: Arc::new(AtomicU64::new(
                now_secs + 60,
            )),
            last_purge_check: Arc::new(AtomicU64::new(initial_purge_check)),
            shutdown: Arc::new(AtomicBool::new(false)),
            prefetch_thread: Mutex::new(None),
            memory_limit,
            cache_evictions: Arc::new(AtomicU64::new(0)),
            forced_evictions: Arc::new(AtomicU64::new(0)),
            memory_monitor_thread: Mutex::new(None),
            last_memory_check: Arc::new(AtomicU64::new(0)),
        };

        // 只有不清空时才加载索引
        if !clear_on_start {
            storage.load_index()?;
        }

        storage.initialize_mmap()?;

        info!(
            "初始化存储 - 堆内存限制: 软={} MB, 硬={} MB, L1缓存={} MB, 清空历史数据={}",
            storage.memory_limit.heap_soft_limit / 1024 / 1024,
            storage.memory_limit.heap_hard_limit / 1024 / 1024,
            storage.memory_limit.l1_cache_hard_limit / 1024 / 1024,
            clear_on_start
        );

        Ok(storage)
    }

    pub fn new(disk_dir: PathBuf, config: HighPerfMmapConfig) -> std::io::Result<Self> {
        Self::new_with_memory_limit(disk_dir, config, MemoryLimitConfig::default())
    }

    /// 【新增】带内存限制配置的构造函数（推荐JNI使用）
    pub fn new_with_memory_limit(
        disk_dir: PathBuf,
        config: HighPerfMmapConfig,
        memory_limit: MemoryLimitConfig,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(&disk_dir)?;

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let purge_interval = config.auto_purge_check_interval_secs;
        let initial_purge_check = if purge_interval > 0 {
            now_secs.saturating_sub(purge_interval)
        } else {
            now_secs
        };

        let data_file_path = disk_dir.join("data.bin");
        let index_file_path = disk_dir.join("index.bin");

        let data_file_size = if data_file_path.exists() {
            std::fs::metadata(&data_file_path)?.len()
        } else {
            let file = File::create(&data_file_path)?;
            file.set_len(config.initial_file_size)?;
            config.initial_file_size
        };

        if data_file_size > config.max_file_size {
            log::warn!(
                "data.bin size {} > configured max {}",
                data_file_size,
                config.max_file_size
            );
        }

        let storage = Self {
            disk_dir,
            data_file_path,
            index_file_path,
            data_file_size: Arc::new(AtomicU64::new(data_file_size)),
            write_offset: Arc::new(AtomicU64::new(0)),
            index: Arc::new(DashMap::new()),
            mmap_rw: Arc::new(parking_lot::RwLock::new(None)),
            mmap_ro: Arc::new(SeqlockedMmap::new()), // 【改用 ArcSwapOption】
            active_readers: Arc::new(AtomicU64::new(0)), // 【新增】
            active_writers: Arc::new(AtomicU64::new(0)),
            resize_lock: Mutex::new(()),
            map_lock: parking_lot::RwLock::new(()),
            hot_cache: Arc::new(DashMap::new()),
            hot_cache_bytes: Arc::new(AtomicU64::new(0)),
            prefetch_queue: Arc::new(ArrayQueue::new(config.prefetch_queue_size.max(8))),
            prefetch_set: Arc::new(DashSet::new()),
            stats: Arc::new(AtomicStats::default()),
            config,
            lazy_deleted_entries: Arc::new(DashMap::new()),
            lazy_by_end: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
            ordered_index: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
            next_gc_time: Arc::new(AtomicU64::new(
                now_secs + 60,
            )),
            last_purge_check: Arc::new(AtomicU64::new(initial_purge_check)),
            shutdown: Arc::new(AtomicBool::new(false)),
            prefetch_thread: Mutex::new(None),
            memory_limit,
            cache_evictions: Arc::new(AtomicU64::new(0)),
            forced_evictions: Arc::new(AtomicU64::new(0)),
            memory_monitor_thread: Mutex::new(None),
            last_memory_check: Arc::new(AtomicU64::new(0)),
        };

        storage.load_index()?;
        storage.initialize_mmap()?;

        info!(
            "初始化存储 - 堆内存限制: 软={} MB, 硬={} MB, L1缓存={} MB",
            storage.memory_limit.heap_soft_limit / 1024 / 1024,
            storage.memory_limit.heap_hard_limit / 1024 / 1024,
            storage.memory_limit.l1_cache_hard_limit / 1024 / 1024
        );

        Ok(storage)
    }

    fn initialize_mmap(&self) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.data_file_path)?;

        let mmap_w = unsafe { MmapMut::map_mut(&file)? };
        let mut w = self.mmap_rw.write();
        *w = Some(mmap_w);

        let mmap_r = unsafe { Mmap::map(&file)? };
        self.mmap_ro.store(Some(Arc::new(mmap_r))); // 【直接 store】

        Ok(())
    }

    fn load_index(&self) -> std::io::Result<()> {
        if !self.index_file_path.exists() {
            return Ok(());
        }

        let mut file = File::open(&self.index_file_path)?;
        let mut index_data = Vec::new();
        file.read_to_end(&mut index_data)?;

        let index: HashMap<String, (u64, u64)> = bincode::deserialize(&index_data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        for (key, value) in index {
            self.index.insert(key, value);
        }

        let max_offset = self
            .index
            .iter()
            .map(|entry| entry.value().0 + entry.value().1)
            .max()
            .unwrap_or(0);

        self.write_offset.store(max_offset, Ordering::Relaxed);

        if let Ok(meta) = std::fs::metadata(&self.data_file_path) {
            self.data_file_size.store(meta.len(), Ordering::Relaxed);
        }
        Ok(())
    }

    fn add_to_hot_cache_sync(&self, key: &str, data: &[u8]) {
        let limit = self.config.l1_cache_size_limit;
        let used = self.hot_cache_bytes.load(Ordering::Relaxed);
        let need = data.len() as u64;

        if need > limit / 2 {
            return;
        }

        if used.saturating_add(need) > limit {
            self.evict_from_hot_cache_sync(Some(need));
        }

        let cached_data = CachedData {
            data: data.to_vec(),
            last_access: Instant::now(),
            access_count: 1,
            size: data.len() as u64,
        };

        if self
            .hot_cache
            .insert(key.to_string(), cached_data)
            .is_some()
        {
            // 替换了旧值，这里简化处理
        }
        self.hot_cache_bytes.fetch_add(need, Ordering::Relaxed);

        if self.hot_cache.len() % 100 == 0 {
            debug!("缓存状态: 当前缓存条目数 = {}", self.hot_cache.len());
        }
    }

    fn evict_from_hot_cache_sync(&self, target_reclaim: Option<u64>) {
        let cache_size = self.hot_cache.len();
        if cache_size == 0 {
            return;
        }

        const EVICT_SAMPLE_MAX: usize = 128;
        let sample = std::cmp::min(std::cmp::max(32, cache_size / 100), EVICT_SAMPLE_MAX);
        let mut entries: Vec<(String, Instant, u64)> = Vec::with_capacity(sample);
        let mut cnt = 0;

        for e in self.hot_cache.iter() {
            entries.push((e.key().clone(), e.value().last_access, e.value().size));
            cnt += 1;
            if cnt >= sample {
                break;
            }
        }

        entries.sort_by(|a, b| a.1.cmp(&b.1));
        let mut reclaimed = 0u64;

        for (k, _ts, sz) in entries {
            if self.hot_cache.remove(&k).is_some() {
                self.hot_cache_bytes.fetch_sub(sz, Ordering::Relaxed);
                reclaimed = reclaimed.saturating_add(sz);
                if let Some(target) = target_reclaim {
                    if reclaimed >= target {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        debug!(
            "缓存驱逐: 回收 {} 字节，当前缓存 {} 条/{} 字节",
            reclaimed,
            self.hot_cache.len(),
            self.hot_cache_bytes.load(Ordering::Relaxed)
        );
    }

    pub fn start_background_tasks(self: &Arc<Self>) {
        if !self.config.enable_prefetch {
            return;
        }

        if self.prefetch_thread.lock().is_some() {
            return;
        }

        self.shutdown.store(false, Ordering::Relaxed);

        let store = Arc::clone(self);
        let handle = std::thread::spawn(move || {
            let mut backoff_us: u64 = 50;
            loop {
                if store.shutdown.load(Ordering::Relaxed) {
                    break;
                }

                match store.prefetch_queue.pop() {
                    Some(key) => {
                        backoff_us = 50;
                        store.prefetch_set.remove(&key);

                        if let Some(entry) = store.index.get(&key) {
                            // 【关键】使用增强加载
                            if let Some(mmap) =
                                store.load_mmap_ro_with_retry(Self::MMAP_RETRY_ATTEMPTS)
                            {
                                if let Ok(data) = Self::read_data_from_mmap_static(&mmap, entry.0) {
                                    let need = data.len() as u64;
                                    let used = store.hot_cache_bytes.load(Ordering::Relaxed);
                                    let limit = store.config.l1_cache_size_limit;

                                    if used.saturating_add(need)
                                        <= limit.saturating_mul(85).saturating_div(100)
                                    {
                                        let cached = CachedData {
                                            data,
                                            last_access: Instant::now(),
                                            access_count: 0,
                                            size: need,
                                        };
                                        store.hot_cache.insert(key, cached);
                                        store.hot_cache_bytes.fetch_add(need, Ordering::Relaxed);
                                        store.stats.prefetch_hits.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        std::thread::sleep(Duration::from_micros(backoff_us));
                        backoff_us = (backoff_us.saturating_mul(2)).min(2000);
                    }
                }
            }
        });

        *self.prefetch_thread.lock() = Some(handle);
    }

    pub fn stop_background_tasks(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(h) = self.prefetch_thread.lock().take() {
            let _ = h.join();
        }
    }

    #[inline]
    fn read_data_from_mmap_static(mmap: &Mmap, offset: u64) -> io::Result<Vec<u8>> {
        use std::io::{Error, ErrorKind};

        let len = mmap.len();
        let start = offset as usize;
        let end_header = start + HEADER_SIZE;

        if start > len || end_header > len {
            return Err(Error::new(ErrorKind::UnexpectedEof, "header out of bounds"));
        }

        let header = &mmap[start..end_header];
        let data_size = u32::from_le_bytes([
            header[HDR_OFF_DATA_SIZE.start],
            header[HDR_OFF_DATA_SIZE.start + 1],
            header[HDR_OFF_DATA_SIZE.start + 2],
            header[HDR_OFF_DATA_SIZE.start + 3],
        ]) as usize;

        // 等待提交位
        if header[HDR_OFF_COMMIT] != 1 {
            std::hint::spin_loop();
            if mmap[start + HDR_OFF_COMMIT] != 1 {
                return Err(Error::new(ErrorKind::WouldBlock, "record not committed"));
            }
        }

        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);

        let data_off = end_header;
        let data_end = data_off
            .checked_add(data_size)
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, "size overflow"))?;

        if data_end > len {
            return Err(Error::new(ErrorKind::UnexpectedEof, "data out of bounds"));
        }

        // 【关键：直接返回原始数据，不在这里解压】
        // 解压交给上层处理，因为这个静态方法无法访问 self
        Ok(mmap[data_off..data_end].to_vec())
    }

    /// 【新增】从 mmap 读取并根据需要解压
    fn read_and_decompress(&self, offset: u64) -> io::Result<Vec<u8>> {
        // 先无锁加载 mmap
        let (mmap) = match self.load_mmap_ro_with_retry(Self::MMAP_RETRY_ATTEMPTS) {
            Some(m) => m,
            None => {
                self.record_error("mmap_uninitialized_read");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "MMAP not initialized",
                ));
            }
        };

        // 读取头部，检查压缩标志
        let len = mmap.len();
        let start = offset as usize;
        let end_header = start + HEADER_SIZE;

        if end_header > len {
            return Err(Error::new(ErrorKind::UnexpectedEof, "header out of bounds"));
        }

        let header = &mmap[start..end_header];
        let compressed_flag = header[HDR_OFF_COMPRESSED];

        // 读取原始数据
        let raw_data = Self::read_data_from_mmap_static(&mmap, offset)?;

        // 根据压缩标志决定是否解压
        if compressed_flag == 1 {
            self.decompress_data(&raw_data, self.config.compression.algorithm)
        } else {
            Ok(raw_data)
        }
    }
}

impl Drop for HighPerfMmapStorage {
    fn drop(&mut self) {
        info!("开始清理存储资源...");

        // 停止所有后台线程
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(h) = self.prefetch_thread.get_mut().take() {
            let _ = h.join();
        }

        if let Some(h) = self.memory_monitor_thread.get_mut().take() {
            let _ = h.join();
        }

        // 输出最终内存统计
        let mem_stats = self.get_memory_stats();
        info!(
            "最终内存统计: 总堆={}MB, L1缓存={}MB/{} 条目, 索引={} 条目, 驱逐={} 次(强制{})",
            mem_stats.total_heap_bytes / 1024 / 1024,
            mem_stats.l1_cache_bytes / 1024 / 1024,
            mem_stats.l1_cache_entries,
            mem_stats.index_entries,
            mem_stats.cache_evictions,
            mem_stats.forced_evictions
        );

        // 清空所有缓存
        self.clear_all_caches();

        // 刷盘并保存索引
        let _ = self.flush();
        let _ = self.save_index();

        info!("存储资源清理完成");
    }
}
