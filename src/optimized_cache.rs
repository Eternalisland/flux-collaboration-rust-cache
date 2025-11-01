//! # 大数据场景优化缓存实现
//! 
//! 结合内存映射和智能缓存策略的高性能实现
//! 专门针对大数据（>1MB）场景优化

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use parking_lot::RwLock;
use memmap2::MmapMut;
use std::sync::atomic::{AtomicUsize, Ordering};
use serde::{Serialize, Deserialize};

/// 缓存状态枚举
#[derive(Debug, Clone, PartialEq)]
pub enum CacheState {
    /// 仅磁盘存储
    DiskOnly,
    /// 内存中且有映射
    MemoryMapped,
    /// 预热中
    Warming,
}

/// 文件元数据结构
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// 文件路径
    pub path: PathBuf,
    /// 文件大小
    pub size: u64,
    /// 最后访问时间
    pub last_access: Instant,
    /// 访问次数
    pub access_count: u64,
    /// 缓存状态
    pub state: CacheState,
    /// 创建时间
    pub created_at: Instant,
    /// 优先级评分（用于 LRU/LFU）
    pub priority_score: f64,
}

/// 优化缓存配置
#[derive(Debug, Clone)]
pub struct OptimizedCacheConfig {
    /// 磁盘目录
    pub disk_dir: PathBuf,
    /// 内存映射阈值（字节）
    pub memory_threshold: usize,
    /// 最大内存映射数
    pub max_memory_maps: usize,
    /// 预热阈值（访问次数）
    pub warmup_threshold: u64,
    /// 清理间隔
    pub cleanup_interval: Duration,
    /// 内存清理触发阈值
    pub memory_cleanup_threshold: f64,
}

impl Default for OptimizedCacheConfig {
    fn default() -> Self {
        Self {
            disk_dir: std::env::temp_dir().join("flux-optimized-cache"),
            memory_threshold: 500 * 1024 * 1024, // 500MB
            max_memory_maps: 100,
            warmup_threshold: 3,
            cleanup_interval: Duration::from_secs(300), // 5分钟
            memory_cleanup_threshold: 0.8, // 80%时触发清理
        }
    }
}

/// 缓存统计数据
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CacheStats {
    /// 总存储操作数
    pub total_operations: u64,
    /// 内存命中次数
    pub memory_hits: u64,
    /// 磁盘命中次数
    pub disk_hits: u64,
    /// 缓存未命中次数
    pub cache_misses: u64,
    /// 总存储字节数
    pub total_bytes: u64,
    /// 内存使用字节数
    pub memory_bytes: u64,
    /// 平均访问时间（微秒）
    pub avg_access_time_us: u64,
    /// 文件数量
    pub file_count: u64,
}

/// 大数据优化缓存
pub struct OptimizedCache {
    /// 配置
    config: OptimizedCacheConfig,
    /// 文件索引
    file_index: Arc<RwLock<HashMap<String, FileMetadata>>>,
    /// 活跃内存映射
    memory_maps: Arc<RwLock<HashMap<String, Arc<MmapMut>>>>,
    /// 当前内存使用量
    memory_usage: Arc<AtomicUsize>,
    /// LRU 队列（用于内存清理决策）
    lru_queue: Arc<RwLock<VecDeque<String>>>,
    /// 统计信息
    stats: Arc<Mutex<CacheStats>>,
    /// 清理任务运行状态
    cleanup_running: Arc<std::sync::atomic::AtomicBool>,
}

impl OptimizedCache {
    /// 创建新的优化缓存实例
    pub fn new(config: OptimizedCacheConfig) -> std::io::Result<Self> {
        // 确保目录存在
        std::fs::create_dir_all(&config.disk_dir)?;

        let cache = Self {
            config,
            file_index: Arc::new(RwLock::new(HashMap::new())),
            memory_maps: Arc::new(RwLock::new(HashMap::new())),
            memory_usage: Arc::new(AtomicUsize::new(0)),
            lru_queue: Arc::new(RwLock::new(VecDeque::new())),
            stats: Arc::new(Mutex::new(CacheStats::default())),
            cleanup_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        cache.start_background_tasks();
        Ok(cache)
    }

    /// 存储数据
    /// 
    /// # 参数
    /// - `key`: 数据标识
    /// - `data`: 数据内容
    /// 
    /// # 返回
    /// - `Ok(Some(bool))`: 存储成功，bool 表示是否预热到内存
    /// - `Err(std::io::Error)`: 存储失败
    pub fn store(&self, key: &str, data: &[u8]) -> std::io::Result<Option<bool>> {
        let start_time = Instant::now();
        
        // 生成文件路径
        let file_id = uuid::Uuid::new_v4().to_string();
        let file_path = self.config.disk_dir.join(format!("{}.cache", file_id));
        
        // 写入文件
        let mut file = File::create(&file_path)?;
        file.write_all(data)?;
        file.sync_all()?;
        
        let file_size = data.len() as u64;
        let data_size = data.len();
        
        // 创建元数据
        let mut metadata = FileMetadata {
            path: file_path,
            size: file_size,
            last_access: Instant::now(),
            access_count: 0,
            state: CacheState::DiskOnly,
            created_at: Instant::now(),
            priority_score: 1.0,
        };
        
        // 检查是否应该预热到内存
        let should_warmup = data_size <= (self.config.memory_threshold / 10) && // 小于总阈值的1/10
                           self.memory_usage.load(Ordering::Relaxed) + data_size <= self.config.memory_threshold;
        
        if should_warmup {
            match self.load_to_memory(key, &metadata.path) {
                Ok(_) => {
                    metadata.state = CacheState::MemoryMapped;
                    metadata.priority_score = 2.0; // 提高优先级
                }
                Err(_) => {
                    // 预热失败，保持磁盘状态
                    metadata.state = CacheState::DiskOnly;
                }
            }
        }
        
        // 更新索引
        {
            let mut index = self.file_index.write();
            index.insert(key.to_string(), metadata);
        }
        
        // 更新统计
        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_operations += 1;
            stats.total_bytes += file_size;
            stats.file_count = self.file_index.read().len() as u64;
            stats.memory_bytes = self.memory_usage.load(Ordering::Relaxed) as u64;
            
            let elapsed_us = start_time.elapsed().as_micros() as u64;
            stats.avg_access_time_us = (stats.avg_access_time_us + elapsed_us) / 2;
        }
        
        Ok(Some(should_warmup))
    }

    /// 读取数据
    /// 
    /// # 参数
    /// - `key`: 数据标识
    /// 
    /// # 返回
    /// - `Ok(Some(Vec<u8>))`: 成功读取数据
    /// - `Ok(None)`: 数据不存在
    /// - `Err(std::io::Error)`: 读取失败
    pub fn load(&self, key: &str) -> std::io::Result<Option<Vec<u8>>> {
        let start_time = Instant::now();
        
        // 获取并更新元数据
        let should_update_memory = {
            let mut index = self.file_index.write();
            if let Some(metadata) = index.get_mut(key) {
                metadata.last_access = Instant::now();
                metadata.access_count += 1;
                
                // 更新优先级评分（LRU + LFU 混合）
                let time_factor = metadata.last_access.duration_since(metadata.created_at).as_secs() as f64;
                let access_factor = metadata.access_count as f64;
                metadata.priority_score = access_factor / time_factor.max(1.0);
                
                metadata.clone()
            } else {
                return Ok(None); // 数据不存在
            }
        };
        
        // 尝试从内存映射读取
        let result = if let Some(mmap) = self.memory_maps.read().get(key) {
            // 内存命中
            let data = (&**mmap).to_vec();
            
            // 更新统计
            {
                let mut stats = self.stats.lock().unwrap();
                stats.memory_hits += 1;
            }
            
            Some(data)
        } else {
            // 磁盘读取
            let data = self.read_from_disk(&should_update_memory.path)?;
            
            // 更新统计
            {
                let mut stats = self.stats.lock().unwrap();
                stats.disk_hits += 1;
            }
            
            // 检查是否应该加载到内存
            let should_cache = self.should_cache_in_memory(&should_update_memory);
            if should_cache {
                let _ = self.load_to_memory(key, &should_update_memory.path);
            }
            
            Some(data)
        };
        
        // 更新 LRU 队列
        {
            let mut lru = self.lru_queue.write();
            lru.retain(|k| k != key); // 移除旧位置
            lru.push_back(key.to_string()); // 添加到末尾
        }
        
        // 更新统计
        {
            let mut stats = self.stats.lock().unwrap();
            let elapsed_us = start_time.elapsed().as_micros() as u64;
            stats.avg_access_time_us = (stats.avg_access_time_us + elapsed_us) / 2;
        }
        
        Ok(result)
    }
    
    /// 检查是否应该缓存到内存
    fn should_cache_in_memory(&self, metadata: &FileMetadata) -> bool {
        let current_memory = self.memory_usage.load(Ordering::Relaxed);
        let file_size = metadata.size as usize;
        let memory_maps_count = self.memory_maps.read().len();
        
        // 文件不大、内存充足、且访问频繁
        file_size <= self.config.memory_threshold / 20 && // 小于阈值1/20
        current_memory + file_size <= self.config.memory_threshold &&
        memory_maps_count < self.config.max_memory_maps &&
        metadata.access_count >= self.config.warmup_threshold
    }
    
    /// 加载文件到内存映射
    fn load_to_memory(&self, key: &str, path: &std::path::Path) -> std::io::Result<()> {
        // 检查内存限制
        if self.memory_usage.load(Ordering::Relaxed) >= self.config.memory_threshold {
            self.evict_memory_mappings();
        }
        
        // 创建内存映射
        let file = OpenOptions::new().read(true).write(false).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        
        let file_size = mmap.len();
        
        // 添加到活跃映射
        {
            let mut maps = self.memory_maps.write();
            maps.insert(key.to_string(), Arc::new(mmap));
            self.memory_usage.fetch_add(file_size, Ordering::Relaxed);
        }
        
        // 更新状态
        {
            let mut index = self.file_index.write();
            if let Some(metadata) = index.get_mut(key) {
                metadata.state = CacheState::MemoryMapped;
            }
        }
        
        Ok(())
    }
    
    /// 清理内存映射
    fn evict_memory_mappings(&self) {
        let mut maps = self.memory_maps.write();
        let mut index = self.file_index.write();
        let mut lru = self.lru_queue.write();
        
        // 获取要清理的键
        let to_remove: Vec<String> = lru.iter()
            .filter(|key| maps.contains_key(*key))
            .take(maps.len() / 3) // 清理1/3的映射
            .cloned()
            .collect();
        
        // 移除映射
        for key in to_remove {
            if let Some(mmap) = maps.remove(&key) {
                let size = mmap.len();
                self.memory_usage.fetch_sub(size, Ordering::Relaxed);
            }
            
            // 更新状态
            if let Some(metadata) = index.get_mut(&key) {
                metadata.state = CacheState::DiskOnly;
            }
        }
    }
    
    /// 从磁盘读取文件
    fn read_from_disk(&self, path: &std::path::Path) -> std::io::Result<Vec<u8>> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }
    
    /// 删除数据
    pub fn remove(&self, key: &str) -> std::io::Result<bool> {
        let metadata = {
            let mut index = self.file_index.write();
            index.remove(key).map(|meta| meta.path)
        };
        
        if let Some(path) = metadata {
            // 移除内存映射
            {
                let mut maps = self.memory_maps.write();
                if let Some(mmap) = maps.remove(key) {
                    let size = mmap.len();
                    self.memory_usage.fetch_sub(size, Ordering::Relaxed);
                }
            }
            
            // 删除文件
            std::fs::remove_file(path)?;
            
            // 更新 LRU 队列
            {
                let mut lru = self.lru_queue.write();
                lru.retain(|k| k != key);
            }
            
            // 更新统计
            {
                let mut stats = self.stats.lock().unwrap();
                stats.file_count = self.file_index.read().len() as u64;
                stats.memory_bytes = self.memory_usage.load(Ordering::Relaxed) as u64;
            }
            
            Ok(true)
        } else {
            Ok(false)
        }
    }
    
    /// 获取统计信息
    pub fn stats(&self) -> CacheStats {
        let mut stats = self.stats.lock().unwrap().clone();
        stats.memory_bytes = self.memory_usage.load(Ordering::Relaxed) as u64;
        stats.file_count = self.file_index.read().len() as u64;
        stats
    }
    
    /// 启动后台任务
    fn start_background_tasks(&self) {
        if self.cleanup_running.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            return;
        }
        
        let cleanup_interval = self.config.cleanup_interval;
        let file_index = Arc::clone(&self.file_index);
        let memory_maps = Arc::clone(&self.memory_maps);
        let memory_usage = Arc::clone(&self.memory_usage);
        let memory_threshold = self.config.memory_threshold;
        let memory_cleanup_threshold = self.config.memory_cleanup_threshold;
        
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(cleanup_interval);
                
                // 检查内存使用率
                let current_usage = memory_usage.load(Ordering::Relaxed);
                let usage_ratio = current_usage as f64 / memory_threshold as f64;
                
                if usage_ratio > memory_cleanup_threshold {
                    // 内存使用过高，清理部分映射
                    let mut maps = memory_maps.write();
                    let mut index_guard = file_index.write();
                    
                    // 按优先级排序，清理优先级低的
                    let mut candidates: Vec<(String, _)> = maps.iter()
                        .filter_map(|(key, _)| {
                            index_guard.get(key).map(|meta| (key.clone(), meta.priority_score))
                        })
                        .collect();

                    candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

                    let to_remove = candidates.len() / 3;
                    for (key, _) in candidates.iter().take(to_remove) {
                        if let Some(mmap) = maps.remove(key) {
                            let size = mmap.len();
                            memory_usage.fetch_sub(size, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
    }
}

// Clone trait is already derived via #[derive(Clone)]
