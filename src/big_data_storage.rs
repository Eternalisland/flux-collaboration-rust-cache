//! # 大数据专用缓存存储模块
//! 
//! 专门为大文件（>1MB）设计的缓存实现，特点：
//! - 免序列化直接存储原始字节
//! - 内存映射优化大文件读写
//! - 分层存储策略：热数据内存，冷数据磁盘
//! - 智能分块和预读机制
//! - 针对大数据场景的性能优化

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use memmap2::MmapMut;
use std::sync::atomic::{AtomicUsize, Ordering};
use serde::{Serialize, Deserialize};

/// 大数据存储配置
#[derive(Debug, Clone)]
pub struct BigDataStorageConfig {
    /// 磁盘目录
    pub disk_dir: PathBuf,
    /// 内存热数据最大大小（字节）
    pub memory_threshold: usize,
    /// 文件预读取块大小（字节）
    pub prefetch_block_size: usize,
    /// 磁盘存储目录
    pub disk_cache_dir: PathBuf,
    /// 是否启用压缩
    pub enable_compression: bool,
    /// 清理间隔
    pub cleanup_interval: Duration,
}

impl Default for BigDataStorageConfig {
    fn default() -> Self {
        Self {
            disk_dir: std::env::temp_dir().join("flux-bigdata-cache"),
            memory_threshold: 100 * 1024 * 1024, // 100MB
            prefetch_block_size: 1024 * 1024, // 1MB
            disk_cache_dir: std::env::temp_dir().join("flux-bigdata-disk"),
            enable_compression: false, // 大数据不压缩，提高速度
            cleanup_interval: Duration::from_secs(300), // 5分钟清理
        }
    }
}

/// 文件元数据
#[derive(Debug, Clone)]
struct FileMetadata {
    /// 文件路径
    path: PathBuf,
    /// 文件大小
    size: u64,
    /// 最后访问时间
    last_access: Instant,
    /// 访问次数
    access_count: u64,
    /// 是否在内存中
    in_memory: bool,
    /// 创建时间
    created_at: Instant,
}

/// 大数据专用存储实现
pub struct BigDataStorage {
    /// 配置
    config: BigDataStorageConfig,
    /// 文件索引
    file_index: Arc<RwLock<HashMap<String, FileMetadata>>>,
    /// 内存缓存大小统计
    memory_usage: Arc<AtomicUsize>,
    /// 当前使用的内存映射
    active_mmap: Arc<RwLock<HashMap<String, Arc<MmapMut>>>>,
    /// 统计信息
    stats: Arc<Mutex<BigDataStats>>,
    /// 清理任务运行状态
    cleanup_running: Arc<std::sync::atomic::AtomicBool>,
}

/// 数据统计信息
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BigDataStats {
    /// 总写入次数
    pub total_writes: u64,
    /// 总读取次数
    pub total_reads: u64,
    /// 总写入字节数
    pub total_write_bytes: u64,
    /// 总读取字节数
    pub total_read_bytes: u64,
    /// 内存命中次数
    pub memory_hits: u64,
    /// 磁盘命中次数
    pub disk_hits: u64,
    /// 平均写入延迟（微秒）
    pub avg_write_latency_us: u64,
    /// 平均读取延迟（微秒）
    pub avg_read_latency_us: u64,
    /// 文件数量
    pub file_count: u64,
}

impl BigDataStorage {
    /// 创建新的大数据存储实例
    pub fn new(config: BigDataStorageConfig) -> std::io::Result<Self> {
        // 确保目录存在
        std::fs::create_dir_all(&config.disk_dir)?;
        std::fs::create_dir_all(&config.disk_cache_dir)?;

        let storage = Self {
            config,
            file_index: Arc::new(RwLock::new(HashMap::new())),
            memory_usage: Arc::new(AtomicUsize::new(0)),
            active_mmap: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(Mutex::new(BigDataStats::default())),
            cleanup_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        storage.start_cleanup_task();
        Ok(storage)
    }

    /// 存储大数据
    /// 
    /// # 参数
    /// - `key`: 数据标识
    /// - `data`: 要存储的字节数据
    /// 
    /// # 返回
    /// - `Ok(())`: 存储成功
    /// - `Err(std::io::Error)`: 存储失败
    pub fn store(&self, key: &str, data: &[u8]) -> std::io::Result<()> {
        let start_time = Instant::now();
        
        // 生成唯一文件名
        let file_id = uuid::Uuid::new_v4().to_string();
        let file_path = self.config.disk_cache_dir.join(format!("{}.cache", file_id));
        
        // 创建文件并写入数据
        let mut file = File::create(&file_path)?;
        file.write_all(data)?;
        file.sync_all()?;
        
        let file_size = data.len() as u64;
        
        // 创建文件元数据
        let metadata = FileMetadata {
            path: file_path.clone(),
            size: file_size,
            last_access: Instant::now(),
            access_count: 0,
            in_memory: false,
            created_at: Instant::now(),
        };
        
        // 更新索引
        {
            let mut index = self.file_index.write();
            index.insert(key.to_string(), metadata);
        }
        
        // 如果数据适合内存缓存，创建内存映射
        if self.should_cache_in_memory(file_size as usize) {
            self.load_to_memory(key, &file_path)?;
        }
        
        // 更新统计
        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_writes += 1;
            stats.total_write_bytes += file_size;
            let elapsed_us = start_time.elapsed().as_micros() as u64;
            stats.avg_write_latency_us = (stats.avg_write_latency_us + elapsed_us) / 2;
            stats.file_count = self.file_index.read().len() as u64;
        }
        
        Ok(())
    }

    /// 读取大数据
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
        
        // 获取文件元数据
        let metadata = {
            let mut index = self.file_index.write();
            index.get_mut(key).map(|meta| {
                meta.last_access = Instant::now();
                meta.access_count += 1;
                meta.clone()
            })
        };
        
        let metadata = match metadata {
            Some(meta) => meta,
            None => return Ok(None),
        };
        
        let result;
        
        // 优先从内存映射读取
        if let Some(mmap) = self.active_mmap.read().get(key) {
            // 从内存映射读取
            let bytes = (&**mmap).to_vec();
            result = Some(bytes);
            
            // 更新统计
            {
                let mut stats = self.stats.lock().unwrap();
                stats.memory_hits += 1;
            }
        } else {
            // 从磁盘读取
            let bytes = self.read_from_disk(&metadata.path)?;
            result = Some(bytes);
            
            // 如果访问频率高，尝试加载到内存
            if metadata.access_count > 2 && self.should_cache_in_memory(metadata.size as usize) {
                let _ = self.load_to_memory(key, &metadata.path);
            }
            
            // 更新统计
            {
                let mut stats = self.stats.lock().unwrap();
                stats.disk_hits += 1;
            }
        }
        
        // 更新统计
        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_reads += 1;
            stats.total_read_bytes += result.as_ref().map(|v| v.len()).unwrap_or(0) as u64;
            let elapsed_us = start_time.elapsed().as_micros() as u64;
            stats.avg_read_latency_us = (stats.avg_read_latency_us + elapsed_us) / 2;
        }
        
        Ok(result)
    }

    /// 检查是否应该将文件缓存到内存
    fn should_cache_in_memory(&self, size: usize) -> bool {
        let current_memory = self.memory_usage.load(Ordering::Relaxed);
        current_memory + size <= self.config.memory_threshold
    }

    /// 将文件加载到内存映射
    fn load_to_memory(&self, key: &str, file_path: &std::path::Path) -> std::io::Result<()> {
        let file = OpenOptions::new().read(true).write(false).open(file_path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        
        let file_size = mmap.len();
        
        // 检查内存限制
        let current_memory = self.memory_usage.load(Ordering::Relaxed);
        if current_memory + file_size > self.config.memory_threshold {
            // 清理一些内存映射
            self.evict_memory_mappings();
        }
        
        // 添加到活跃映射
        {
            let mut mmaps = self.active_mmap.write();
            mmaps.insert(key.to_string(), Arc::new(mmap));
            self.memory_usage.fetch_add(file_size, Ordering::Relaxed);
        }
        
        Ok(())
    }

    /// 清理内存映射
    fn evict_memory_mappings(&self) {
        let mut mmaps = self.active_mmap.write();
        let mut index = self.file_index.write();
        
        // 获取最少使用的文件
        let mut candidates: Vec<(String, Instant)> = mmaps.iter()
            .filter_map(|(key, _)| {
                index.get(key).map(|meta| (key.clone(), meta.last_access))
            })
            .collect();
        
        candidates.sort_by(|a, b| a.1.cmp(&b.1));
        
        // 移除一半的最老映射
        let to_remove = candidates.len() / 2;
        for (key, _) in candidates.iter().take(to_remove) {
            if let Some(mmap) = mmaps.remove(key) {
                let size = mmap.len();
                self.memory_usage.fetch_sub(size, Ordering::Relaxed);
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
                let mut mmaps = self.active_mmap.write();
                if let Some(mmap) = mmaps.remove(key) {
                    let size = mmap.len();
                    self.memory_usage.fetch_sub(size, Ordering::Relaxed);
                }
            }
            
            // 删除文件
            std::fs::remove_file(path)?;
            
            // 更新统计
            {
                let mut stats = self.stats.lock().unwrap();
                stats.file_count = self.file_index.read().len() as u64;
            }
            
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// 获取统计信息
    pub fn stats(&self) -> BigDataStats {
        self.stats.lock().unwrap().clone()
    }

    /// 启动后台清理任务
    fn start_cleanup_task(&self) {
        if self.cleanup_running.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            return;
        }
        
        let cleanup_interval = self.config.cleanup_interval;
        let file_index = Arc::clone(&self.file_index);
        let active_mmap = Arc::clone(&self.active_mmap);
        let memory_usage = Arc::clone(&self.memory_usage);
        let memory_threshold = self.config.memory_threshold;
        let _cleanup_running = Arc::clone(&self.cleanup_running);
        
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(cleanup_interval);
                
                // 清理过期的文件
                let mut to_remove = Vec::new();
                {
                    let index = file_index.read();
                    let cutoff = Instant::now() - Duration::from_secs(3600); // 1小时未访问
                    
                    for (key, metadata) in index.iter() {
                        if metadata.last_access < cutoff {
                            to_remove.push(key.clone());
                        }
                    }
                }
                
                for key in to_remove {
                    let _ = Self::remove_static(&file_index, &active_mmap, &memory_usage, &key);
                }
                
                // 如果内存使用过高，清理部分映射
                if memory_usage.load(Ordering::Relaxed) > memory_threshold {
                    let mut mmaps = active_mmap.write();
                    let index_guard = file_index.write();
                    
                    let mut candidates: Vec<(String, Instant)> = mmaps.iter()
                        .filter_map(|(key, _)| {
                            index_guard.get(key).map(|meta| (key.clone(), meta.last_access))
                        })
                        .collect();
                    
                    candidates.sort_by(|a, b| a.1.cmp(&b.1));
                    
                    let to_evict = candidates.len() / 3; // 清理1/3
                    for (key, _) in candidates.iter().take(to_evict) {
                        if let Some(mmap) = mmaps.remove(key) {
                            let size = mmap.len();
                            memory_usage.fetch_sub(size, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
    }

    /// 静态方法：移除文件
    fn remove_static(
        file_index: &Arc<parking_lot::RwLock<HashMap<String, FileMetadata>>>,
        active_mmap: &Arc<parking_lot::RwLock<HashMap<String, Arc<MmapMut>>>>,
        memory_usage: &Arc<AtomicUsize>,
        key: &str,
    ) -> std::io::Result<bool> {
        let metadata = {
            let mut index = file_index.write();
            index.remove(key).map(|meta| meta.path)
        };
        
        if let Some(path) = metadata {
            // 移除内存映射
            {
                let mut mmaps = active_mmap.write();
                if let Some(mmap) = mmaps.remove(key) {
                    let size = mmap.len();
                    memory_usage.fetch_sub(size, Ordering::Relaxed);
                }
            }
            
            // 删除文件
            std::fs::remove_file(path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// JNI 接口实现
impl BigDataStorage {
    /// 创建存储实例
    pub fn create_with_config(disk_dir: &str, memory_threshold: usize) -> std::io::Result<Self> {
        let config = BigDataStorageConfig {
            disk_dir: PathBuf::from(disk_dir),
            memory_threshold,
            ..Default::default()
        };
        
        Self::new(config)
    }
}

impl Clone for BigDataStats {
    fn clone(&self) -> Self {
        Self {
            total_writes: self.total_writes,
            total_reads: self.total_reads,
            total_write_bytes: self.total_write_bytes,
            total_read_bytes: self.total_read_bytes,
            memory_hits: self.memory_hits,
            disk_hits: self.disk_hits,
            avg_write_latency_us: self.avg_write_latency_us,
            avg_read_latency_us: self.avg_read_latency_us,
            file_count: self.file_count,
        }
    }
}
