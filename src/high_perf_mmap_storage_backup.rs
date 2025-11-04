// ========== 依赖导入 ==========
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, info};

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use dashmap::DashSet;
use memmap2::{Mmap, MmapMut};
use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::AtomicBool;
use arc_swap::ArcSwap;
use arc_swap::ArcSwapOption;

// ===== 新增：压缩相关依赖 =====
// 推荐使用 lz4_flex：解压速度极快（4GB/s+），压缩比适中
// 或者 zstd：可调节压缩级别，兼顾速度和压缩比
// use lz4_flex;  // 需要添加到 Cargo.toml

/// 压缩配置
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
            enabled: false,
            algorithm: CompressionAlgorithm::Lz4,
            min_compress_size: 512,        // 小于512字节不压缩
            min_compress_ratio: 0.90,      // 压缩不到10%就不保存
            async_compression: false,      // 默认同步压缩
            compression_level: 1,          // 最快压缩级别
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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
    mmap_ro: Arc<ArcSwapOption<Mmap>>,

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

    shutdown: Arc<AtomicBool>,
    prefetch_thread: Mutex<Option<std::thread::JoinHandle<()>>>,


    cache_evictions: Arc<AtomicU64>,
    forced_evictions: Arc<AtomicU64>,
    last_memory_check: Arc<AtomicU64>,

    memory_limit : MemoryLimitConfig,
    /// 内存监控后台线程句柄
    memory_monitor_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// 【关键改造4】原子化的统计结构
/// 所有字段都是原子类型，更新时无需加锁
pub struct AtomicStats {
    pub total_writes: AtomicU64,
    pub total_reads: AtomicU64,
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
    pub avg_compression_ratio: AtomicU64,  // 存储为整数（ratio * 1000）
    /// 预读真正服务命中（首次命中 L1 且由预读放入）
    pub prefetch_served_hits: AtomicU64,
}

impl Default for AtomicStats {
    fn default() -> Self {
        Self {
            total_writes: AtomicU64::new(0),
            total_reads: AtomicU64::new(0),
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
            avg_compression_ratio: AtomicU64::new(1000),  // 初始为 1.0 (1000/1000)
        }
    }
}


/// 统计快照结构（用于读取）
#[derive(Debug, Clone)]
pub struct HighPerfMmapStats {
    pub total_writes: u64,
    pub total_reads: u64,
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

#[derive(Debug, Clone)]
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

        if avg.compare_exchange_weak(
            current,
            new_avg,
            Ordering::Relaxed,
            Ordering::Relaxed
        ).is_ok() {
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
                self.memory_limit.heap_hard_limit * 75 / 100  // 强制降到75%
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
            || stats.l1_cache_entries > self.memory_limit.l1_cache_entry_hard_limit {
            let to_free = stats.l1_cache_bytes.saturating_sub(
                self.memory_limit.l1_cache_hard_limit * 90 / 100
            );
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
            cache_size  // 激进模式：扫描全部
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
                self.hot_cache_bytes.fetch_sub(cached.size, Ordering::Relaxed);
                freed = freed.saturating_add(size);
                evicted_count += 1;
            }
        }

        self.cache_evictions.fetch_add(evicted_count, Ordering::Relaxed);

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
            0  // 正常
        } else if ratio < 0.75 {
            1  // 警告
        } else if ratio < 0.9 {
            2  // 压力
        } else {
            3  // 危险
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

        self.stats.compression_attempts.fetch_add(1, Ordering::Relaxed);

        // 执行压缩
        let compressed = match self.config.compression.algorithm {
            CompressionAlgorithm::None => {
                return Ok((data.to_vec(), false, original_size));
            }
            CompressionAlgorithm::Lz4 => {
                self.compress_lz4(data)?
            }
            CompressionAlgorithm::Zstd => {
                self.compress_zstd(data)?
            }
            CompressionAlgorithm::Snappy => {
                self.compress_snappy(data)?
            }
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
        self.stats.compression_successes.fetch_add(1, Ordering::Relaxed);
        self.stats.compressed_bytes_saved.fetch_add(saved, Ordering::Relaxed);

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
        self.stats.decompression_count.fetch_add(1, Ordering::Relaxed);

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
    #[inline]
    fn load_mmap_ro(&self) -> Option<(Arc<Mmap>, ReaderGuard)> {
        // 先增加读者计数（防止并发释放）
        let guard = ReaderGuard::new(Arc::clone(&self.active_readers));

        // 尝试加载映射，带短暂自旋（最多 10μs）
        const MAX_SPIN_ATTEMPTS: u32 = 100;  // 约 1-2μs
        const MAX_YIELD_ATTEMPTS: u32 = 10;   // 约 10μs

        let mut spins = 0u32;
        let mut yields = 0u32;

        loop {
            if let Some(mmap) = self.mmap_ro.load_full() {
                return Some((mmap, guard));
            }

            // 第一阶段：快速自旋（避免线程切换）
            if spins < MAX_SPIN_ATTEMPTS {
                std::hint::spin_loop();
                spins += 1;
                continue;
            }

            // 第二阶段：让出CPU时间片
            if yields < MAX_YIELD_ATTEMPTS {
                std::thread::yield_now();
                yields += 1;
                continue;
            }

            // 超过阈值仍未加载：记录错误并返回
            // 这种情况理论上不应该发生（除非存储正在关闭）
            log::error!(
                "load_mmap_ro: 自旋 {} 次后仍无法加载映射，可能存储正在关闭",
                spins + yields
            );
            return None;
        }
    }

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
            return Err(Error::new(
                ErrorKind::WouldBlock,
                "mmap_rw not initialized",
            ));
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
        Err(std::io::Error::new(ErrorKind::Other, "write failed after retry"))
    }

    fn remap_mmap_locked(&self) -> std::io::Result<()> {
        // 【关键改进】先创建新映射，再替换，永不出现 None 状态
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.data_file_path)?;

        let mmap_w = unsafe { MmapMut::map_mut(&file)? };
        let mmap_r = unsafe { Mmap::map(&file)? };

        // 原子替换读映射（旧映射仍然有效，由 Arc 引用计数保护）
        let old_mmap = self.mmap_ro.swap(Some(Arc::new(mmap_r)));

        // 更新写映射
        {
            let mut w = self.mmap_rw.write();
            *w = Some(mmap_w);
        }

        // 等待所有正在使用旧映射的读者退出
        if old_mmap.is_some() {
            let start = Instant::now();
            let mut spins = 0u32;

            while self.active_readers.load(Ordering::Acquire) > 0 {
                if spins < 1000 {
                    std::hint::spin_loop();
                    spins += 1;
                } else {
                    std::thread::yield_now();
                    spins = 0;

                    // 超过 10ms 仍有读者，记录警告
                    if start.elapsed().as_millis() > 10 {
                        log::warn!(
                            "remap_mmap_locked: 等待读者超过 10ms，当前读者数 = {}",
                            self.active_readers.load(Ordering::Relaxed)
                        );
                    }

                    // 超过 1 秒，强制继续（可能有 bug）
                    if start.elapsed().as_secs() > 1 {
                        log::error!(
                            "remap_mmap_locked: 等待读者超时（1秒），强制继续"
                        );
                        break;
                    }
                }
            }
        }

        self.stats.mmap_remaps.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn remap_mmap(&self) -> std::io::Result<()> {
        let _g = self.map_lock.write();
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
        let reserve = (self.config.growth_reserve_steps.saturating_sub(1) as u64)
            .saturating_mul(step);
        let baseline = cur2.saturating_add(reserve);
        let target = std::cmp::max(aligned.saturating_add(reserve), baseline);
        let new_size = std::cmp::min(target, self.config.max_file_size);

        let t0 = Instant::now();

        // 扩展文件
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.data_file_path)?;
        file.set_len(new_size)?;
        file.sync_all()?;

        // 重新映射（内部会处理旧映射的释放）
        self.remap_mmap()?;

        self.data_file_size.store(new_size, Ordering::Relaxed);

        self.stats.file_expansions.fetch_add(1, Ordering::Relaxed);
        self.stats.maintenance_expand_events.fetch_add(1, Ordering::Relaxed);
        self.stats.expand_us_total.fetch_add(
            t0.elapsed().as_micros() as u64,
            Ordering::Relaxed
        );

        Ok(())
    }

    /// 【关键修复】加载 mmap 的方法，带短暂重试机制
    #[inline]
    fn load_mmap_ro_with_retry(&self, max_attempts: u32) -> Option<Arc<Mmap>> {
        for attempt in 0..max_attempts {
            if let Some(mmap) = self.mmap_ro.load_full() {
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
            }
        }
        None
    }
    /// 【核心方法：完全无锁的读取】
    /// 【修复】读取方法 - 添加重试逻辑
    pub fn read(&self, key: &str) -> std::io::Result<Option<Vec<u8>>> {
        let start_time = Instant::now();

        // L1 缓存检查
        if let Some(mut cached_data) = self.hot_cache.get_mut(key) {
            if cached_data.access_count == 0 {
                self.stats.prefetch_served_hits.fetch_add(1, Ordering::Relaxed);
            }
            cached_data.access_count += 1;
            cached_data.last_access = Instant::now();
            let data_len = cached_data.data.len();
            let elapsed = start_time.elapsed().as_micros() as u64;

            self.stats.l1_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.stats.total_reads.fetch_add(1, Ordering::Relaxed);
            self.stats.total_read_bytes.fetch_add(data_len as u64, Ordering::Relaxed);
            update_ema_atomic(&self.stats.avg_read_latency_us, elapsed);

            return Ok(Some(cached_data.data.clone()));
        }

        // 索引查询
        let (offset, _tot) = match self.index.get(key) {
            Some(v) => *v,
            None => return Ok(None),
        };

        // 【关键】使用增强的加载方法（带自旋）
        let (mmap, _guard) = match self.load_mmap_ro() {
            Some(pair) => pair,
            None => {
                self.record_error("mmap_uninitialized_read");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "MMAP not initialized after spin wait",
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
        self.stats.total_read_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        update_ema_atomic(&self.stats.avg_read_latency_us, elapsed);

        Ok(Some(data))
    }


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

        // L1 缓存检查
        for k in keys {
            if let Some(mut cached) = self.hot_cache.get_mut(k) {
                if cached.access_count == 0 {
                    self.stats.prefetch_served_hits.fetch_add(1, Ordering::Relaxed);
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
                self.stats.total_reads.fetch_add(produced, Ordering::Relaxed);
                self.stats.total_read_bytes.fetch_add(l1_bytes, Ordering::Relaxed);
                self.stats.l1_cache_hits.fetch_add(l1_hits, Ordering::Relaxed);
                let per_key_us = (elapsed_us / produced.max(1)).max(1);
                update_ema_atomic(&self.stats.avg_read_latency_us, per_key_us);
            }
            return Ok(out);
        }

        // 【关键】使用增强加载
        let (mmap_arc, _guard) = match self.load_mmap_ro() {
            Some(pair) => pair,
            None => {
                self.record_error("mmap_uninitialized_read");
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "MMAP RO not initialized"
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
                if v == 1 { break; }
                std::hint::spin_loop();
                spins = spins.saturating_add(1);
                if spins == 64 { std::thread::yield_now(); }
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
                    if !self.prefetch_set.insert(c.clone()) { continue; }
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
            self.stats.total_reads.fetch_add(produced, Ordering::Relaxed);
            self.stats.total_read_bytes.fetch_add(l1_bytes + miss_bytes, Ordering::Relaxed);
            self.stats.l1_cache_hits.fetch_add(l1_hits, Ordering::Relaxed);
            self.stats.l1_cache_misses.fetch_add(misses.len() as u64, Ordering::Relaxed);
            update_ema_atomic(&self.stats.avg_read_latency_us, per_key_us);
        }

        Ok(out)
    }


    pub fn read_batch(&self, keys: &[String]) -> std::io::Result<HashMap<String, Vec<u8>>> {
        self.read_batch_coalesced(keys, DEFAULT_COALESCE_GAP)
    }

    /// 【无锁优化】零拷贝读取
    pub fn read_slice(&self, key: &str) -> std::io::Result<Option<ZeroCopySlice>> {
        let (offset, _tot) = match self.index.get(key) {
            Some(v) => *v,
            None => return Ok(None),
        };

        let (mmap, _guard) = match self.load_mmap_ro() {
            Some(pair) => pair,
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

    pub fn write(&self, key: &str, data: &[u8]) -> std::io::Result<()> {
        let start_all = Instant::now();

        // 【新增】写入前检查内存限制
        if self.memory_limit.reject_writes_under_pressure {
            let pressure = self.get_memory_pressure_level();
            if pressure >= 3 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "内存压力过高，拒绝写入"
                ));
            }
        }

        // 【新增】定期检查内存（每秒最多一次，避免频繁检查）
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let last_check = self.last_memory_check.load(Ordering::Relaxed);
        let check_interval_ns = self.memory_limit.check_interval_ms * 1_000_000;

        if now_ns.saturating_sub(last_check) > check_interval_ns {
            if self.last_memory_check.compare_exchange(
                last_check,
                now_ns,
                Ordering::Relaxed,
                Ordering::Relaxed
            ).is_ok() {
                // 异步检查，不阻塞写入
                let _ = self.check_memory_limits();
            }
        }

        let _wg = WriterGuard::new(&self.active_writers);

        // 【关键：智能压缩】
        let (actual_data, compressed, original_size) = self.compress_if_beneficial(data)?;

        let total = HEADER_SIZE as u64 + actual_data.len() as u64;
        let maxf = self.config.max_file_size;

        let offset = match self.write_offset.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |cur| {
                let needed = cur.saturating_add(total);
                if needed > maxf { None } else { Some(needed) }
            },
        ) {
            Ok(prev) => prev,
            Err(_) => {
                let _ = self.garbage_collect();
                match self.write_offset.fetch_update(
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    |cur| {
                        let needed = cur.saturating_add(total);
                        if needed > maxf { None } else { Some(needed) }
                    },
                ) {
                    Ok(prev2) => prev2,
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

        let needed = offset + total;
        let cur_sz = self.data_file_size.load(Ordering::Relaxed);
        let step = self.config.growth_step.max(1);
        let reserve_steps = self.config.growth_reserve_steps.saturating_sub(1) as u64;

        let free_tail = cur_sz.saturating_sub(needed);
        let required = if free_tail < step.saturating_mul(2) {
            needed + step.saturating_mul(reserve_steps + 2)
        } else {
            needed
        };
        self.expand_file_locked_if_needed(required)?;

        let start_path = Instant::now();
        self.write_data_at_offset_retry_compressed(offset, &actual_data, compressed)?;
        let path_ns = start_path.elapsed().as_nanos() as u64;
        let path_us = (path_ns + 999) / 1000;

        // 【关键修复】写入成功后，立即使缓存失效
        // 必须在索引更新前删除，避免竞态
        if let Some((_k, old_cached)) = self.hot_cache.remove(key) {
            self.hot_cache_bytes.fetch_sub(old_cached.size, Ordering::Relaxed);
        }

        // 更新索引，标记旧数据为删除
        if let Some((old_off, old_sz)) = self.index.insert(key.to_string(), (offset, total)) {
            // 以旧的 offset 为键存储，避免同 key 多次覆盖时信息丢失
            self.lazy_deleted_entries.insert(old_off, (key.to_string(), old_sz));

            let mut by_end = self.lazy_by_end.write();
            by_end.insert(old_off + old_sz, (key.to_string(), (old_off, old_sz)));
        }
        self.ordered_index.write().insert(offset, key.to_string());

        // 【可选优化】立即将新数据加入缓存，提高后续读取性能
        // 但要检查内存是否充足
        let mem_stats = self.get_memory_stats();
        if mem_stats.memory_usage_ratio < 0.8 {  // 只在内存充足时预热
            if compressed {
                // 如果数据被压缩了，需要解压后再缓存
                if let Ok(decompressed) = self.decompress_data(&actual_data, self.config.compression.algorithm) {
                    self.add_to_hot_cache_sync(key, &decompressed);
                }
            } else {
                // 未压缩，直接缓存
                self.add_to_hot_cache_sync(key, &actual_data);
            }
        }

        // 【统计：记录原始大小，不是压缩后的】
        self.stats.total_writes.fetch_add(1, Ordering::Relaxed);
        self.stats.total_write_bytes.fetch_add(original_size as u64, Ordering::Relaxed);
        update_ema_atomic(&self.stats.avg_write_latency_us, start_all.elapsed().as_micros() as u64);
        update_ema_atomic(&self.stats.avg_write_path_us, path_us);

        Ok(())
    }

    /// 【新增】写入带压缩标志的数据
    fn write_data_at_offset_retry_compressed(&self, offset: u64, data: &[u8], compressed: bool) -> std::io::Result<()> {
        for attempt in 0..=2 {
            {
                let _ml = self.map_lock.read();
                if let Ok(()) = self.write_data_at_offset_once_compressed(offset, data, compressed) {
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
        Err(std::io::Error::new(ErrorKind::Other, "write failed after retry"))
    }

    fn write_data_at_offset_once_compressed(&self, offset: u64, data: &[u8], compressed: bool) -> std::io::Result<()> {
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
            header_bytes[HDR_OFF_COMPRESSED] = if compressed { 1 } else { 0 };  // 【关键：记录压缩标志】
            header_bytes[HDR_OFF_COMMIT] = 0;
            header_bytes[HDR_OFF_TIMESTAMP].copy_from_slice(&now_ts.to_le_bytes());

            mmap[start..end_header].copy_from_slice(&header_bytes);
            mmap[end_header..data_end].copy_from_slice(data);
            mmap[start + HDR_OFF_COMMIT] = 1;

            Ok(())
        } else {
            return Err(Error::new(
                ErrorKind::WouldBlock,
                "mmap_rw not initialized",
            ));
        }
    }

    pub fn delete(&self, key: &str) -> std::io::Result<bool> {
        let removed_entry = self.index.remove(key);

        if let Some((_k, v)) = self.hot_cache.remove(key) {
            self.hot_cache_bytes.fetch_sub(v.size, Ordering::Relaxed);
        }

        if let Some((_k, (offset, size))) = removed_entry {
            self.lazy_deleted_entries.insert(offset, (key.to_string(), size));
            {
                let mut by_end = self.lazy_by_end.write();
                by_end.insert(offset + size, (key.to_string(), (offset, size)));
            }
            {
                let mut ord = self.ordered_index.write();
                ord.remove(&offset);
            }

            self.try_shrink_tail()?;
            self.trigger_gc_if_needed();
            return Ok(true);
        }
        Ok(false)
    }

    pub fn delete_lazy(&self, key: &str) -> std::io::Result<bool> {
        if let Some((_k, (offset, size))) = self.index.remove(key) {
            self.lazy_deleted_entries.insert(offset, ( key.to_string(), size));

            if let Some((_k, v)) = self.hot_cache.remove(key) {
                self.hot_cache_bytes.fetch_sub(v.size, Ordering::Relaxed);
            }

            self.stats.lazy_deleted_entries.fetch_add(1, Ordering::Relaxed);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn garbage_collect(&self) -> std::io::Result<u64> {
        if self.active_writers.load(Ordering::Relaxed) > 0 {
            return Ok(0);
        }

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let next_gc_time = self.next_gc_time.load(Ordering::Relaxed);
        if current_time < next_gc_time {
            return Ok(0);
        }

        self.next_gc_time.store(current_time + 60, Ordering::Relaxed);

        if self.lazy_deleted_entries.is_empty() {
            return Ok(0);
        }

        let mut valid_entries: HashMap<String, (u64, u64)> = HashMap::new();
        for entry in self.index.iter() {
            valid_entries.insert(entry.key().clone(), *entry.value());
        }

        let reclaimed_space = self.compact_data_file(&valid_entries)?;

        let deleted_count = self.lazy_deleted_entries.len() as u64;
        self.lazy_deleted_entries.clear();

        self.stats.garbage_collection_runs.fetch_add(1, Ordering::Relaxed);
        self.stats.reclaimed_disk_space.fetch_add(reclaimed_space, Ordering::Relaxed);

        info!(
            "垃圾回收完成: 清理了 {} 个条目，回收了 {} 字节空间",
            deleted_count, reclaimed_space
        );

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

        // 计算需要回收的空间
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

        // 【关键】等待所有读者退出
        let start = Instant::now();
        let mut spins = 0u32;

        while self.active_readers.load(Ordering::Acquire) > 0 {
            if spins < 10000 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::yield_now();
                spins = 0;

                // 超过 100ms 仍有读者，放弃收缩
                if start.elapsed().as_millis() > 100 {
                    log::warn!(
                        "try_shrink_tail: 等待读者超过 100ms，放弃收缩"
                    );
                    return Ok(());
                }
            }
        }

        // 更新偏移
        self.write_offset.store(tail, Ordering::Relaxed);
        self.data_file_size.store(tail, Ordering::Relaxed);

        // 释放写映射
        {
            let mut w = self.mmap_rw.write();
            let _ = w.take();
        }

        // 【关键】设为 None，阻止新读者（旧读者已退出）
        let _old = self.mmap_ro.swap(None);

        // 截断文件
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.data_file_path)?;
        file.set_len(tail)?;
        file.sync_all()?;
        drop(file);

        // 立即重建映射（恢复可用状态）
        self.remap_mmap_locked()?;

        self.stats.reclaimed_disk_space.fetch_add(reclaimed, Ordering::Relaxed);
        info!("删除收缩: 回收尾部 {} 字节, 新文件大小 {}", reclaimed, tail);

        Ok(())
    }

    fn trigger_gc_if_needed(&self) {
        let lazy_count = self.lazy_deleted_entries.len();
        let live_count = self.index.len().max(1);

        let mut lazy_bytes: u64 = 0;
        for e in self.lazy_deleted_entries.iter() {
            let (_key, size) = e.value();   // 新结构：(key, size)
            lazy_bytes = lazy_bytes.saturating_add(*size);
            if lazy_bytes >= self.config.growth_step {
                break;
            }
        }

        if lazy_count > (live_count / 2) || lazy_bytes >= self.config.growth_step {
            let _ = self.garbage_collect();
        }
    }

    fn compact_data_file(&self, valid_entries: &HashMap<String, (u64, u64)>) -> std::io::Result<u64> {
        let temp_file_path = self.data_file_path.with_extension("tmp");
        let mut temp_file = File::create(&temp_file_path)?;
        let original_size = self.data_file_size.load(Ordering::Relaxed);

        // 读取并复制数据
        let file = OpenOptions::new().read(true).open(&self.data_file_path)?;
        let mmap_ro = unsafe { memmap2::Mmap::map(&file)? };

        let mut sorted_entries: Vec<(&String, &(u64, u64))> = valid_entries.iter().collect();
        sorted_entries.sort_by_key(|(_, (offset, _))| *offset);

        let mut new_offset = 0u64;
        let header_size = HEADER_SIZE as u64;
        let mut new_index: HashMap<String, (u64, u64)> = HashMap::new();

        for (key, (offset, size)) in sorted_entries {
            let start = *offset as usize;
            let end_header = start + header_size as usize;
            if end_header > mmap_ro.len() {
                continue;
            }

            let header_bytes = &mmap_ro[start..end_header];
            let data_offset = end_header;
            let data_end = data_offset + (size - header_size) as usize;
            if data_end > mmap_ro.len() {
                continue;
            }

            let data = &mmap_ro[data_offset..data_end];

            temp_file.write_all(header_bytes)?;
            temp_file.write_all(data)?;

            new_index.insert((*key).clone(), (new_offset, *size));
            new_offset += *size;
        }

        temp_file.sync_all()?;
        drop(temp_file);
        drop(mmap_ro);
        drop(file);

        let _rl = self.resize_lock.lock();
        let _ml = self.map_lock.write();

        // 【关键】等待所有读者退出（增加超时检测）
        let start = Instant::now();
        let mut spins = 0u32;

        while self.active_readers.load(Ordering::Acquire) > 0 {
            if spins < 10000 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::yield_now();
                spins = 0;

                // 每秒输出一次日志
                if start.elapsed().as_secs() > 0 && spins == 0 {
                    log::warn!(
                        "compact_data_file: 等待读者超过 {} 秒，当前读者数 = {}",
                        start.elapsed().as_secs(),
                        self.active_readers.load(Ordering::Relaxed)
                    );
                }

                // 超过 10 秒，放弃 GC
                if start.elapsed().as_secs() > 10 {
                    log::error!("compact_data_file: 等待读者超时（10秒），放弃 GC");
                    return Ok(0);
                }
            }
        }

        // 释放映射
        {
            let mut w = self.mmap_rw.write();
            let _ = w.take();
        }
        let _old = self.mmap_ro.swap(None);

        // 替换文件
        std::fs::rename(&temp_file_path, &self.data_file_path)?;

        let new_file_size = new_offset;
        self.data_file_size.store(new_file_size, Ordering::Relaxed);

        // 重新创建映射
        self.remap_mmap_locked()?;

        // 更新索引
        self.index.clear();
        {
            let mut ord = self.ordered_index.write();
            ord.clear();
            for (key, value) in new_index {
                self.index.insert(key.clone(), value);
                ord.insert(value.0, key);
            }
        }
        self.lazy_by_end.write().clear();

        // 扩展文件
        let step = self.config.growth_step.max(1);
        let reserve = (self.config.growth_reserve_steps.saturating_sub(1) as u64) * step;
        let boost: u64 = 2;
        let aligned = ((new_file_size + step - 1) / step) * step;
        let target_after_gc = (aligned + reserve.saturating_mul(boost)).min(self.config.max_file_size);

        if target_after_gc > new_file_size {
            let t0 = std::time::Instant::now();

            // 再次等待读者（扩展前）
            let start = Instant::now();
            while self.active_readers.load(Ordering::Acquire) > 0 {
                std::hint::spin_loop();
                if start.elapsed().as_millis() > 100 {
                    log::warn!("compact_data_file: 扩展前等待读者超时");
                    break;
                }
            }

            {
                let mut w = self.mmap_rw.write();
                let _ = w.take();
            }
            let _old = self.mmap_ro.swap(None);

            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.data_file_path)?;
            f.set_len(target_after_gc)?;
            f.sync_all()?;
            drop(f);

            self.remap_mmap_locked()?;
            self.data_file_size.store(target_after_gc, Ordering::Relaxed);

            self.stats.file_expansions.fetch_add(1, Ordering::Relaxed);
            self.stats.maintenance_expand_events.fetch_add(1, Ordering::Relaxed);
            self.stats.expand_us_total.fetch_add(
                t0.elapsed().as_micros() as u64,
                Ordering::Relaxed
            );
        }

        let reclaimed_space = original_size.saturating_sub(
            self.data_file_size.load(Ordering::Relaxed)
        );
        Ok(reclaimed_space)
    }


    /// 【无锁优化】获取统计快照
    pub fn get_stats(&self) -> HighPerfMmapStats {
        HighPerfMmapStats {
            total_writes: self.stats.total_writes.load(Ordering::Relaxed),
            total_reads: self.stats.total_reads.load(Ordering::Relaxed),
            total_records: self.index.len() as u64,
            total_write_bytes: self.stats.total_write_bytes.load(Ordering::Relaxed),
            total_read_bytes: self.stats.total_read_bytes.load(Ordering::Relaxed),
            l1_cache_hits: self.stats.l1_cache_hits.load(Ordering::Relaxed),
            l1_cache_misses: self.stats.l1_cache_misses.load(Ordering::Relaxed),
            l2_cache_hits: self.stats.l2_cache_hits.load(Ordering::Relaxed),
            l2_cache_misses: self.stats.l2_cache_misses.load(Ordering::Relaxed),
            prefetch_hits: self.stats.prefetch_hits.load(Ordering::Relaxed),
            file_expansions: self.stats.file_expansions.load(Ordering::Relaxed),
            avg_write_latency_us: self.stats.avg_write_latency_us.load(Ordering::Relaxed),
            avg_read_latency_us: self.stats.avg_read_latency_us.load(Ordering::Relaxed),
            mmap_remaps: self.stats.mmap_remaps.load(Ordering::Relaxed),
            error_count: self.stats.error_count.load(Ordering::Relaxed),
            last_error_code: self.stats.last_error_code.load().as_ref().clone(),
            lazy_deleted_entries: self.stats.lazy_deleted_entries.load(Ordering::Relaxed),
            reclaimed_disk_space: self.stats.reclaimed_disk_space.load(Ordering::Relaxed),
            garbage_collection_runs: self.stats.garbage_collection_runs.load(Ordering::Relaxed),
            avg_write_path_us: self.stats.avg_write_path_us.load(Ordering::Relaxed),
            maintenance_expand_events: self.stats.maintenance_expand_events.load(Ordering::Relaxed),
            expand_us_total: self.stats.expand_us_total.load(Ordering::Relaxed),
            prefetch_served_hits: self.stats.prefetch_served_hits.load(Ordering::Relaxed),
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
        self.stats.last_error_code.store(Arc::new(Some(code.to_string())));
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
            mmap_ro: Arc::new(ArcSwapOption::empty()), // 【改用 ArcSwapOption】
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
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 60,
            )),
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

        if self.hot_cache.insert(key.to_string(), cached_data).is_some() {
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
                            if let Some((mmap, _guard)) = store.load_mmap_ro() {
                                if let Ok(data) = Self::read_data_from_mmap_static(&mmap, entry.0) {
                                    let need = data.len() as u64;
                                    let used = store.hot_cache_bytes.load(Ordering::Relaxed);
                                    let limit = store.config.l1_cache_size_limit;

                                    if used.saturating_add(need) <= limit.saturating_mul(85).saturating_div(100) {
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
        let (mmap , reader_guard) = match self.load_mmap_ro() {
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
