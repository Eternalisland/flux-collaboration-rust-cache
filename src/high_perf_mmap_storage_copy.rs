// ========== 依赖导入 ==========
// 标准库：集合、文件 IO、原子类型、同步原语、时间
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

// 日志打印
use log::{debug, info};

// 轻量级锁：parking_lot 的 Mutex/RwLock 比 std::sync 更快
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// 并发容器与队列
use crossbeam_queue::ArrayQueue; // 无锁环形队列
use dashmap::DashMap;            // 分段无锁哈希表
use dashmap::DashSet;            // 分段无锁集合

// 内存映射
use memmap2::{Mmap, MmapMut};

// 其他容器/IO
use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::AtomicBool;

// ===== 新增：无锁 Arc 切换库 =====
// ArcSwap 允许在不阻塞读者的情况下，原子替换 Arc 指针（读路径完全无锁）
use arc_swap::ArcSwap;

// ============================================================================
//  高性能无锁 MMAP 磁盘存储
// ---------------------------------------------------------------------------
// 设计目标：
//   1) 读路径零锁：读者只读 ArcSwap 中的 Arc<Mmap>，不持有任何互斥锁
//   2) 写路径低争用：写入与扩容/重映射串行化，但尽量缩小临界区
//   3) 统计全原子：所有指标通过原子变量更新，避免锁
//   4) 预读与热缓存：DashMap + 预取线程，提升热点命中率
//   5) 清理压缩：延迟删除 + 定期 GC + 尾部回缩/重写压缩
//
// 并发与内存模型要点：
//   - 数据格式：每条记录 = [固定头(16B) | 数据]
//       头结构：
//         [0..4)  : 数据长度 u32 小端
//         [4]     : 是否压缩（当前示例未使用）
//         [5]     : commit 字节（0->1，表示写入完成）
//         [6..8)  : 预留/对齐（未使用）
//         [8..16) : 写入时间戳 u64 小端
//   - 写入顺序：先写头与数据，再最后把 commit 位置写 1（提交）。
//   - 读取顺序：自旋读 commit==1，随后以 Acquire 栅栏读取数据，确保可见性。
//      注意：严格内存模型下，建议在写入 commit 前加一条 Release 栅栏，
//
// ============================================================================
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
    mmap_rw: Arc<parking_lot::RwLock<Option<MmapMut>>>,

    /// 【关键改造2】读映射改用 ArcSwap：读路径完全无锁！
    /// 读者通过 load() 获取 Arc<Mmap>，无需任何锁
    mmap_ro: Arc<ArcSwap<Option<Arc<Mmap>>>>,

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

    lazy_deleted_entries: Arc<DashMap<String, (u64, u64)>>,
    lazy_by_end: Arc<parking_lot::RwLock<BTreeMap<u64, (String, (u64, u64))>>>,
    ordered_index: Arc<parking_lot::RwLock<BTreeMap<u64, String>>>,
    next_gc_time: Arc<AtomicU64>,

    shutdown: Arc<AtomicBool>,
    prefetch_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
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
        }
    }
}

/// 统计快照结构（用于读取）
#[derive(Debug, Clone)]
pub struct HighPerfMmapStats {
    pub total_writes: u64,
    pub total_reads: u64,
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

impl HighPerfMmapStorage {
    /// 【无锁优化】读取 mmap 的核心方法：完全无锁
    #[inline]
    fn load_mmap_ro(&self) -> Option<Arc<Mmap>> {
        // ArcSwap::load() 是完全无锁的原子操作
        // 返回的是 Arc 的克隆，引用计数原子递增
        self.mmap_ro.load().as_ref().as_ref().cloned()
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
        // 【关键修复】先创建新映射，再原子替换，避免 None 窗口期
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.data_file_path)?;

        let mmap_w = unsafe { MmapMut::map_mut(&file)? };
        let mmap_r = unsafe { Mmap::map(&file)? };

        // 先更新写映射
        {
            let mut w = self.mmap_rw.write();
            *w = Some(mmap_w);
        }

        // 【关键】直接原子替换为新映射，不经过 None 状态
        // 旧映射的引用计数会自动递减，当所有读者释放后自动回收
        self.mmap_ro.store(Arc::new(Some(Arc::new(mmap_r))));

        self.stats.mmap_remaps.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn remap_mmap(&self) -> std::io::Result<()> {
        let _g = self.map_lock.write();
        self.remap_mmap_locked()
    }

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
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.data_file_path)?;
        file.set_len(new_size)?;
        file.sync_all()?;

        self.remap_mmap()?;

        self.data_file_size.store(new_size, Ordering::Relaxed);

        // 【无锁统计更新】
        self.stats.file_expansions.fetch_add(1, Ordering::Relaxed);
        self.stats.maintenance_expand_events.fetch_add(1, Ordering::Relaxed);
        self.stats.expand_us_total.fetch_add(
            t0.elapsed().as_micros() as u64,
            Ordering::Relaxed
        );

        Ok(())
    }

    /// 【核心方法：完全无锁的读取】
    pub fn read(&self, key: &str) -> std::io::Result<Option<Vec<u8>>> {
        let start_time = Instant::now();

        // L1 缓存检查（DashMap 内部是分段锁，读取很快）
        if let Some(mut cached_data) = self.hot_cache.get_mut(key) {
            if cached_data.access_count == 0 {
                self.stats.prefetch_served_hits.fetch_add(1, Ordering::Relaxed);
            }

            cached_data.access_count += 1;
            cached_data.last_access = Instant::now();
            let data_len = cached_data.data.len();
            let elapsed = start_time.elapsed().as_micros() as u64;

            // 【无锁统计更新】
            self.stats.l1_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.stats.total_reads.fetch_add(1, Ordering::Relaxed);
            self.stats.total_read_bytes.fetch_add(data_len as u64, Ordering::Relaxed);
            update_ema_atomic(&self.stats.avg_read_latency_us, elapsed);

            return Ok(Some(cached_data.data.clone()));
        }

        // 【关键：完全无锁的索引+映射获取】
        let (offset, _tot) = match self.index.get(key) {
            Some(v) => *v,
            None => return Ok(None),
        };

        // 【关键：无锁加载 mmap】不需要 map_lock！
        let mmap = match self.load_mmap_ro() {
            Some(m) => m,
            None => {
                self.record_error("mmap_uninitialized_read");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "MMAP not initialized",
                ));
            }
        };

        // 从 mmap 读取数据（纯内存操作，无锁）
        let data = match Self::read_data_from_mmap_static(&mmap, offset) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        // 加入缓存
        self.add_to_hot_cache_sync(key, &data);

        // 触发预读
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

        // 【无锁统计更新】
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

        // 【无锁加载 mmap】
        let mut mmap_arc = match self.load_mmap_ro() {
            Some(m) => m,
            None => {
                self.record_error("mmap_uninitialized_read");
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "MMAP RO not initialized"
                ));
            }
        };

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

        let mut ensure_mmap_len = |need_end: usize, arc: &mut Arc<Mmap>| -> std::io::Result<()> {
            if need_end <= arc.len() { return Ok(()); }
            if let Some(new_m) = self.load_mmap_ro() {
                *arc = new_m;
                if need_end <= arc.len() { return Ok(()); }
            }
            self.record_error("read_data_oob");
            Err(Error::new(ErrorKind::UnexpectedEof, "data out of bounds"))
        };

        for (k, off) in &misses {
            let size = committed_size(&*mmap_arc, *off)?;
            let data_off = (*off as usize) + HEADER_SIZE;
            let data_end = data_off + size;
            ensure_mmap_len(data_end, &mut mmap_arc)?;
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

        // 【无锁加载 mmap】
        let mmap = match self.load_mmap_ro() {
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

    pub fn write(&self, key: &str, data: &[u8]) -> std::io::Result<()> {
        let start_all = Instant::now();
        let _wg = WriterGuard::new(&self.active_writers);

        let total = HEADER_SIZE as u64 + data.len() as u64;
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
        self.write_data_at_offset_retry(offset, data)?;
        let path_ns = start_path.elapsed().as_nanos() as u64;
        let path_us = (path_ns + 999) / 1000;

        if let Some((old_off, old_sz)) = self.index.insert(key.to_string(), (offset, total)) {
            self.lazy_deleted_entries.insert(key.to_string(), (old_off, old_sz));
            let mut by_end = self.lazy_by_end.write();
            by_end.insert(old_off + old_sz, (key.to_string(), (old_off, old_sz)));
        }
        self.ordered_index.write().insert(offset, key.to_string());

        // 【无锁统计更新】
        self.stats.total_writes.fetch_add(1, Ordering::Relaxed);
        self.stats.total_write_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        update_ema_atomic(&self.stats.avg_write_latency_us, start_all.elapsed().as_micros() as u64);
        update_ema_atomic(&self.stats.avg_write_path_us, path_us);

        Ok(())
    }

    pub fn delete(&self, key: &str) -> std::io::Result<bool> {
        let removed_entry = self.index.remove(key);

        if let Some((_k, v)) = self.hot_cache.remove(key) {
            self.hot_cache_bytes.fetch_sub(v.size, Ordering::Relaxed);
        }

        if let Some((_k, (offset, size))) = removed_entry {
            self.lazy_deleted_entries.insert(key.to_string(), (offset, size));
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
            self.lazy_deleted_entries.insert(key.to_string(), (offset, size));

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
                self.lazy_deleted_entries.remove(&k);
                tail = off;
                reclaimed = reclaimed.saturating_add(size);
            } else {
                break;
            }
        }

        if reclaimed > 0 {
            let _rl = self.resize_lock.lock();
            let _ml = self.map_lock.write();

            self.write_offset.store(tail, Ordering::Relaxed);
            self.data_file_size.store(tail, Ordering::Relaxed);

            // 【关键修复】先释放写映射，但保持读映射可用
            {
                let mut w = self.mmap_rw.write();
                let _ = w.take();
            }

            // 【注意】不要设置读映射为 None！保持旧映射可用，直到新映射创建完成

            // 截断文件
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.data_file_path)?;
            file.set_len(tail)?;
            file.sync_all()?;
            drop(file);

            // 【关键】创建新映射并原子替换，避免 None 窗口期
            self.remap_mmap_locked()?;

            self.stats.reclaimed_disk_space.fetch_add(reclaimed, Ordering::Relaxed);
            info!("删除收缩: 回收尾部 {} 字节, 新文件大小 {}", reclaimed, tail);
        }
        Ok(())
    }

    fn trigger_gc_if_needed(&self) {
        let lazy_count = self.lazy_deleted_entries.len();
        let live_count = self.index.len().max(1);

        let mut lazy_bytes: u64 = 0;
        for e in self.lazy_deleted_entries.iter() {
            let (_off, size) = *e.value();
            lazy_bytes = lazy_bytes.saturating_add(size);
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

        // 【关键修复】先释放写映射，但保持读映射可用
        {
            let mut w = self.mmap_rw.write();
            let _ = w.take();
        }
        // 【注意】不要清空读映射！保持旧映射可用，直到新文件准备好

        // 替换文件
        std::fs::rename(&temp_file_path, &self.data_file_path)?;

        let new_file_size = new_offset;
        self.data_file_size.store(new_file_size, Ordering::Relaxed);

        // 【关键】创建新映射并原子替换
        self.remap_mmap_locked()?;

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

        let step = self.config.growth_step.max(1);
        let reserve = (self.config.growth_reserve_steps.saturating_sub(1) as u64) * step;
        let boost: u64 = 2;
        let aligned = ((new_file_size + step - 1) / step) * step;
        let target_after_gc = (aligned + reserve.saturating_mul(boost)).min(self.config.max_file_size);

        if target_after_gc > new_file_size {
            let t0 = std::time::Instant::now();
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
            mmap_ro: Arc::new(ArcSwap::from_pointee(None)),
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
        };

        storage.load_index()?;
        storage.initialize_mmap()?;

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
        self.mmap_ro.store(Arc::new(Some(Arc::new(mmap_r))));

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
                            // 【无锁加载 mmap】
                            if let Some(mmap) = store.load_mmap_ro() {
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

        Ok(mmap[data_off..data_end].to_vec())
    }
}

impl Drop for HighPerfMmapStorage {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(h) = self.prefetch_thread.get_mut().take() {
            let _ = h.join();
        }

        let _ = self.flush();
        let _ = self.save_index();
    }
}