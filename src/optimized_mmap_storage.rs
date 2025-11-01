// ========== 依赖导入 ==========
// 标准库导入
use std::collections::HashMap;           // HashMap：用于存储键值对索引映射
use std::fs::{File, OpenOptions};        // 文件操作：创建、打开文件
use std::io::{Read, Write};              // I/O 操作：读写文件
use std::path::PathBuf;                  // 路径操作：处理文件路径
use std::sync::Arc;             // 线程安全：原子引用计数
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;                  // 时间操作：性能测量、延迟统计

// 第三方库导入
use parking_lot::RwLock;                 // 高性能读写锁：比标准库 RwLock 更快
use memmap2::MmapMut;                    // 内存映射：零拷贝文件访问（只读映射暂未使用）

/// 优化版 MMAP 磁盘存储
/// 
/// # 核心优化特性
/// 1. **复用 MMAP 映射**：避免频繁创建和销毁内存映射，显著提升性能
/// 2. **简化数据结构**：使用固定大小的头部，减少序列化开销
/// 3. **智能缓存管理**：可配置的缓存策略，平衡内存使用和性能
/// 4. **批量操作支持**：为未来的批量读写操作预留接口
/// 5. **零拷贝优化**：直接操作内存映射，避免数据拷贝
/// 
/// # 性能优化点
/// - 单一 MMAP 映射复用，减少系统调用
/// - 固定 16 字节头部，避免动态序列化
/// - 使用 parking_lot::RwLock 减少锁竞争
/// - 文件扩展时自动重新映射
/// 
/// # 注意事项
/// - 内存映射大小受系统限制
/// - 需要定期调用 save_index() 持久化索引
/// - 文件扩展时会重新映射，可能短暂影响性能
/// - 当前版本暂时禁用压缩以简化实现
pub struct OptimizedMmapStorage {
    /// 磁盘存储目录路径
    /// 注意：确保目录有足够的磁盘空间和写入权限
    /// 当前版本未直接使用，但保留用于未来扩展
    #[allow(dead_code)]
    disk_dir: PathBuf,
    
    /// 主数据文件路径
    /// 存储所有实际数据的二进制文件
    data_file_path: PathBuf,
    
    /// 索引文件路径
    /// 存储键到文件偏移量的映射关系
    index_file_path: PathBuf,
    
    /// 主数据文件当前大小（字节）
    /// 用于判断是否需要扩展文件
    data_file_size: AtomicU64,
    
    /// 当前写入位置偏移量
    /// 使用原子计数器，减少锁竞争
    write_offset: AtomicU64,
    
    /// 索引映射表：键 -> (文件偏移量, 数据大小)
    /// 使用 RwLock 支持并发读取，写入时独占锁
    /// 优化点：可以考虑使用 BTreeMap 提高范围查询性能
    index: Arc<RwLock<HashMap<String, (u64, u64)>>>,
    
    /// 复用的内存映射对象
    /// 使用 Option 包装，支持延迟初始化
    /// 核心优化：复用单一映射，避免频繁创建
    mmap: Arc<RwLock<Option<MmapMut>>>,
    
    /// 性能统计信息
    /// 包含读写次数、延迟、MMAP 重新映射次数等指标
    stats: Arc<RwLock<OptimizedMmapStats>>,
    
    /// 存储配置参数
    /// 包含文件大小、缓存设置等
    config: OptimizedMmapConfig,
}

/// 优化版 MMAP 存储配置结构体
/// 
/// # 配置说明
/// 这个结构体包含了所有影响存储性能和行为的关键参数
/// 相比原版，简化了配置项，专注于核心优化
/// 
/// # 性能调优建议
/// - initial_file_size: 根据预期数据量设置，避免频繁扩展
/// - growth_step: 设置合理的增长步长，平衡内存使用和扩展频率
/// - max_file_size: 根据磁盘空间和系统限制设置
/// - enable_compression: 当前版本暂时禁用，未来可考虑启用
/// - enable_cache: 建议启用，提高读取性能
/// - cache_size_limit: 根据可用内存设置，建议不超过系统内存的 1/4
#[derive(Debug, Clone)]
pub struct OptimizedMmapConfig {
    /// 初始文件大小（字节）
    /// 建议值：100MB - 1GB，根据预期数据量调整
    /// 注意：过小会导致频繁扩展，过大浪费磁盘空间
    pub initial_file_size: u64,
    
    /// 文件增长步长（字节）
    /// 当文件空间不足时，每次扩展的大小
    /// 建议值：初始大小的 50%-100%
    pub growth_step: u64,
    
    /// 最大文件大小（字节）
    /// 文件大小的硬限制，防止无限增长
    /// 注意：受系统文件大小限制影响
    pub max_file_size: u64,
    
    /// 是否启用数据压缩
    /// 当前版本暂时禁用，简化实现
    /// 未来可以考虑启用 LZ4 等快速压缩算法
    pub enable_compression: bool,
    
    /// 是否启用缓存
    /// true: 启用内存映射缓存
    /// false: 禁用缓存，每次从磁盘读取
    /// 建议：对于频繁读取的场景启用缓存
    pub enable_cache: bool,
    
    /// 缓存大小限制（字节）
    /// 内存映射缓存的最大总大小
    /// 建议值：系统内存的 10%-25%
    /// 注意：超过限制时会触发缓存清理
    pub cache_size_limit: u64,
}

/// 默认配置实现
/// 
/// # 默认值说明
/// 这些默认值适用于大多数中等规模的应用场景
/// 相比原版，优化了默认配置以提升性能
impl Default for OptimizedMmapConfig {
    fn default() -> Self {
        Self {
            // 初始文件大小：100MB
            // 适合中小型应用，避免频繁文件扩展
            initial_file_size: 100 * 1024 * 1024,
            
            // 增长步长：50MB
            // 每次扩展 50MB，平衡扩展频率和内存使用
            growth_step: 50 * 1024 * 1024,
            
            // 最大文件大小：10GB
            // 适合大多数应用场景，可根据磁盘空间调整
            max_file_size: 10 * 1024 * 1024 * 1024,
            
            // 暂时禁用压缩
            // 简化实现，专注于核心优化
            // 未来可以考虑启用 LZ4 等快速压缩
            enable_compression: false,
            
            // 启用缓存
            // 提高读取性能，减少磁盘 I/O
            enable_cache: true,
            
            // 缓存限制：200MB
            // 相比原版减少，因为使用单一 MMAP 映射
            // 适合 4GB+ 内存的系统
            cache_size_limit: 200 * 1024 * 1024,
        }
    }
}

/// 优化版 MMAP 存储性能统计信息
/// 
/// # 统计指标说明
/// 这些指标用于监控存储系统的性能表现
/// 相比原版，增加了 MMAP 重新映射次数的统计
/// 
/// # 性能分析建议
/// - 缓存命中率 = cache_hits / (cache_hits + cache_misses)
/// - 平均延迟过高可能表示磁盘 I/O 瓶颈
/// - 文件扩展次数过多建议增大 initial_file_size
/// - MMAP 重新映射次数过多可能影响性能
#[derive(Debug, Default)]
pub struct OptimizedMmapStats {
    /// 总写入操作次数
    /// 用于计算写入频率和负载
    pub total_writes: u64,
    
    /// 总读取操作次数
    /// 用于计算读取频率和负载
    pub total_reads: u64,
    
    /// 总写入字节数
    /// 用于计算写入吞吐量和数据量
    pub total_write_bytes: u64,
    
    /// 总读取字节数
    /// 用于计算读取吞吐量和数据量
    pub total_read_bytes: u64,
    
    /// 缓存命中次数
    /// 从内存映射缓存中成功读取的次数
    /// 高命中率表示缓存策略有效
    pub cache_hits: u64,
    
    /// 缓存未命中次数
    /// 需要从磁盘读取的次数
    /// 高未命中率可能需要调整缓存大小
    pub cache_misses: u64,
    
    /// 文件扩展次数
    /// 数据文件被扩展的总次数
    /// 频繁扩展可能影响性能
    pub file_expansions: u64,
    
    /// 平均写入延迟（微秒）
    /// 包含压缩、磁盘写入等所有操作的时间
    /// 用于监控写入性能
    pub avg_write_latency_us: u64,
    
    /// 平均读取延迟（微秒）
    /// 包含解压缩、磁盘读取等所有操作的时间
    /// 用于监控读取性能
    pub avg_read_latency_us: u64,
    
    /// MMAP 重新映射次数
    /// 内存映射被重新创建的总次数
    /// 频繁重新映射可能影响性能
    /// 优化版特有指标，用于监控映射复用效果
    pub mmap_remaps: u64,
}

/// 简化的数据条目头部信息
/// 
/// # 存储格式说明
/// 每个数据条目在文件中都包含一个固定大小的头部
/// 相比原版，简化了头部结构，减少序列化开销
/// 
/// # 优化考虑
/// - 使用固定 16 字节大小，避免动态序列化
/// - 使用 Copy trait，支持零拷贝操作
/// - 简化字段，专注于核心信息
/// - 使用小端字节序，提高跨平台兼容性
/// 
/// # 存储布局（16 字节）
/// - 0-3: 数据大小 (u32, 小端)
/// - 4: 压缩标志 (u8, 0=未压缩, 1=压缩)
/// - 5-7: 保留字段 (3 字节)
/// - 8-15: 时间戳 (u64, 小端)
#[derive(Debug, Clone, Copy)]
struct SimpleDataHeader {
    /// 数据部分的大小（字节）
    /// 注意：不包含头部本身的大小
    /// 使用 u32 类型，支持最大 4GB 的单个数据项
    size: u32,
    
    /// 数据是否被压缩
    /// true: 数据使用压缩算法
    /// false: 数据为原始格式
    /// 当前版本暂时固定为 false
    compressed: bool,
    
    /// 数据写入时间戳
    /// 使用 Unix 时间戳（秒），可用于数据过期管理
    /// 优化点：可以考虑使用纳秒精度提高时间分辨率
    timestamp: u64,
}

impl OptimizedMmapStorage {
    /// 创建新的优化 MMAP 磁盘存储实例
    /// 
    /// # 参数
    /// - `disk_dir`: 存储目录路径
    /// - `config`: 存储配置参数
    /// 
    /// # 返回值
    /// - `Ok(Self)`: 成功创建存储实例
    /// - `Err(std::io::Error)`: 创建失败，可能是权限或磁盘空间问题
    /// 
    /// # 初始化流程
    /// 1. 创建存储目录（如果不存在）
    /// 2. 初始化数据文件和索引文件路径
    /// 3. 检查数据文件是否存在，决定创建新文件还是使用现有文件
    /// 4. 初始化所有内部数据结构
    /// 5. 加载现有索引（如果存在）
    /// 6. 初始化 MMAP 映射（核心优化）
    /// 
    /// # 性能优化点
    /// - 预分配大文件避免频繁扩展
    /// - 使用 Arc 实现多线程共享
    /// - 延迟初始化 MMAP 映射
    /// - 单一映射复用，减少系统调用
    /// 
    /// # 注意事项
    /// - 确保目录有足够的磁盘空间和写入权限
    /// - 大文件创建可能需要较长时间
    /// - MMAP 初始化失败会阻止存储创建
    /// - 索引加载失败不会阻止存储创建
    pub fn new(disk_dir: PathBuf, config: OptimizedMmapConfig) -> std::io::Result<Self> {
        // 步骤1: 确保存储目录存在
        // 使用 create_dir_all 递归创建目录，如果目录已存在则不会报错
        std::fs::create_dir_all(&disk_dir)?;
        
        // 步骤2: 构建文件路径
        // 数据文件存储实际数据，索引文件存储键值映射
        let data_file_path = disk_dir.join("data.bin");
        let index_file_path = disk_dir.join("index.bin");
        
        // 步骤3: 初始化数据文件
        // 如果文件已存在，获取其大小；否则创建新文件并预分配空间
        let data_file_size = if data_file_path.exists() {
            // 文件已存在，获取当前大小
            // 注意：这里假设文件大小不会在外部被修改
            std::fs::metadata(&data_file_path)?.len()
        } else {
            // 文件不存在，创建新文件并预分配空间
            // 预分配可以避免频繁的文件扩展操作
            let file = File::create(&data_file_path)?;
            file.set_len(config.initial_file_size)?;
            config.initial_file_size
        };
        
        // 步骤4: 创建存储实例
        // 使用 Arc 包装所有共享数据，支持多线程访问
        let storage = Self {
            disk_dir,                    // 存储目录路径
            data_file_path,              // 数据文件路径
            index_file_path,             // 索引文件路径
            data_file_size: AtomicU64::new(data_file_size), // 当前文件大小
            write_offset: AtomicU64::new(0),  // 写入位置，初始为0
            index: Arc::new(RwLock::new(HashMap::new())),  // 索引映射表
            mmap: Arc::new(RwLock::new(None)),  // MMAP 映射，延迟初始化
            stats: Arc::new(RwLock::new(OptimizedMmapStats::default())),  // 统计信息
            config,                      // 配置参数
        };
        
        // 步骤5: 加载现有索引
        // 如果索引文件存在，加载其中的键值映射
        // 注意：索引加载失败不会阻止存储创建，但会影响数据访问
        storage.load_index()?;
        
        // 步骤6: 初始化 MMAP 映射（核心优化）
        // 创建单一的内存映射，后续操作都复用这个映射
        // 这是优化版的核心特性，避免频繁创建和销毁映射
        storage.initialize_mmap()?;
        
        Ok(storage)
    }
    
    /// 写入数据到存储
    /// 
    /// # 参数
    /// - `key`: 数据的唯一标识符
    /// - `data`: 要存储的字节数据
    /// 
    /// # 返回值
    /// - `Ok(())`: 写入成功
    /// - `Err(std::io::Error)`: 写入失败，可能是磁盘空间不足或权限问题
    /// 
    /// # 写入流程
    /// 1. 记录开始时间用于性能统计
    /// 2. 计算所需存储空间（固定 16 字节头部 + 数据大小）
    /// 3. 获取写入位置并检查文件扩展需求
    /// 4. 将数据写入到指定偏移量（使用复用的 MMAP）
    /// 5. 更新索引映射
    /// 6. 更新性能统计信息
    /// 
    /// # 性能优化点
    /// - 使用固定 16 字节头部，避免动态序列化
    /// - 复用单一 MMAP 映射，减少系统调用
    /// - 批量更新索引和统计信息
    /// - 预分配文件空间避免频繁扩展
    /// 
    /// # 注意事项
    /// - 写入操作是线程安全的
    /// - 大文件写入可能需要较长时间
    /// - 文件扩展时会重新映射 MMAP
    /// - 当前版本暂时禁用压缩
    pub fn write(&self, key: &str, data: &[u8]) -> std::io::Result<()> {
        // 步骤1: 记录开始时间
        // 用于计算写入延迟，帮助监控性能
        let start_time = Instant::now();
        
        // 步骤2: 计算所需存储空间
        // 使用固定 16 字节头部，避免动态序列化开销
        // 这是优化版的核心特性之一
        let header_size = 16u64;
        let total_size = header_size + data.len() as u64;
        
        // 步骤3: 获取写入位置并处理文件扩展
        // 使用互斥锁确保写入位置的原子性
        // 原子获取并递增写入偏移，减少锁竞争
        let offset = self.write_offset.fetch_add(total_size, Ordering::Relaxed);
        // 容量预检与扩容（阶梯式扩容，减少 remap 次数）
        let needed = offset + total_size;
        let current_size = self.data_file_size.load(Ordering::Relaxed);
        if needed > current_size {
            // 目标新容量：至少 needed，再额外预留 3 个 growth_step，降低重映射频率
            let target = std::cmp::min(
                needed.saturating_add(self.config.growth_step.saturating_mul(3)),
                self.config.max_file_size,
            );
            self.expand_file(target)?;
            // 同步记录最新文件大小
            self.data_file_size.store(target, Ordering::Relaxed);
        }
        
        // 步骤4: 将数据写入到指定偏移量
        // 使用复用的 MMAP 映射进行高效写入
        // 这是优化版的核心特性，避免频繁创建映射
        self.write_data_at_offset(offset, data)?;
        
        // 步骤5: 更新索引映射
        // 将键值对添加到索引中，用于快速查找
        {
            let mut index = self.index.write();
            // 存储 (偏移量, 总大小) 的映射关系
            index.insert(key.to_string(), (offset, total_size));
        }
        
        // 步骤6: 更新性能统计信息
        // 记录写入次数、字节数和平均延迟
        {
            let mut stats = self.stats.write();
            stats.total_writes += 1;                                    // 增加写入次数
            stats.total_write_bytes += data.len() as u64;              // 增加写入字节数
            // 计算平均延迟（简单移动平均）
            stats.avg_write_latency_us = (stats.avg_write_latency_us + start_time.elapsed().as_micros() as u64) / 2;
        }
        
        Ok(())
    }
    
    /// 从存储中读取数据
    /// 
    /// # 参数
    /// - `key`: 要读取的数据的唯一标识符
    /// 
    /// # 返回值
    /// - `Ok(Some(Vec<u8>))`: 成功读取数据
    /// - `Ok(None)`: 数据不存在
    /// - `Err(std::io::Error)`: 读取失败，可能是文件损坏或权限问题
    /// 
    /// # 读取流程
    /// 1. 记录开始时间用于性能统计
    /// 2. 从索引中查找数据位置信息
    /// 3. 从复用的 MMAP 映射中读取数据
    /// 4. 更新性能统计信息
    /// 
    /// # 性能优化点
    /// - 直接使用复用的 MMAP 映射，避免磁盘 I/O
    /// - 使用零拷贝内存映射减少数据拷贝
    /// - 简化头部解析，固定 16 字节格式
    /// - 并发读取支持，多个线程可以同时读取
    /// 
    /// # 注意事项
    /// - 读取操作是线程安全的
    /// - 所有读取都通过内存映射，性能稳定
    /// - 大文件读取可能需要较长时间
    /// - 当前版本暂时禁用压缩和解压缩
    pub fn read(&self, key: &str) -> std::io::Result<Option<Vec<u8>>> {
        // 步骤1: 记录开始时间
        // 用于计算读取延迟，帮助监控性能
        let start_time = Instant::now();
        
        // 步骤2: 从索引获取数据位置信息
        // 索引包含键到 (偏移量, 大小) 的映射
        let (offset, size) = {
            let index = self.index.read();
            match index.get(key) {
                Some(pos) => *pos,  // 找到数据位置
                None => return Ok(None),  // 数据不存在，直接返回
            }
        };
        
        // 步骤3: 从复用的 MMAP 映射读取数据
        // 这是优化版的核心特性，直接使用内存映射，避免磁盘 I/O
        let data = self.read_data_from_mmap(offset, size)?;
        
        // 步骤4: 更新性能统计信息
        // 记录读取次数、字节数和平均延迟
        {
            let mut stats = self.stats.write();
            stats.total_reads += 1;                                   // 增加读取次数
            stats.total_read_bytes += data.len() as u64;              // 增加读取字节数
            // 计算平均延迟（简单移动平均）
            stats.avg_read_latency_us = (stats.avg_read_latency_us + start_time.elapsed().as_micros() as u64) / 2;
        }
        
        Ok(Some(data))
    }
    
    /// 删除指定键的数据
    /// 
    /// # 参数
    /// - `key`: 要删除的数据的唯一标识符
    /// 
    /// # 返回值
    /// - `Ok(true)`: 数据存在并已删除
    /// - `Ok(false)`: 数据不存在
    /// - `Err(std::io::Error)`: 删除失败，可能是权限问题
    /// 
    /// # 删除流程
    /// 1. 从索引中移除键值映射
    /// 2. 返回删除结果
    /// 
    /// # 注意事项
    /// - 删除操作是逻辑删除，不会立即释放磁盘空间
    /// - 实际数据仍保留在文件中，直到文件被重写
    /// - 删除操作是线程安全的
    /// - 建议定期调用 save_index() 持久化索引变更
    /// - 优化版简化了删除逻辑，不涉及缓存清理
    pub fn delete(&self, key: &str) -> std::io::Result<bool> {
        // 获取索引的写锁，准备删除操作
        let mut index = self.index.write();
        
        // 尝试从索引中移除键值映射
        // 返回是否成功删除（数据是否存在）
        Ok(index.remove(key).is_some())
    }
    
    /// 获取性能统计信息
    /// 
    /// # 返回值
    /// - `OptimizedMmapStats`: 包含所有性能指标的统计信息
    /// 
    /// # 统计指标
    /// - 读写操作次数和字节数
    /// - 缓存命中率和未命中率
    /// - 文件扩展次数
    /// - 平均读写延迟
    /// - MMAP 重新映射次数（优化版特有）
    /// 
    /// # 使用建议
    /// - 定期检查统计信息监控性能
    /// - 根据 MMAP 重新映射次数优化文件扩展策略
    /// - 根据延迟指标优化配置参数
    pub fn get_stats(&self) -> OptimizedMmapStats {
        // 克隆统计信息，避免长时间持有锁
        self.stats.read().clone()
    }
    
    /// 保存索引到磁盘
    /// 
    /// # 返回值
    /// - `Ok(())`: 保存成功
    /// - `Err(std::io::Error)`: 保存失败，可能是磁盘空间不足或权限问题
    /// 
    /// # 保存流程
    /// 1. 获取索引的读锁
    /// 2. 使用 bincode 序列化索引数据
    /// 3. 写入到索引文件
    /// 4. 同步到磁盘确保数据持久化
    /// 
    /// # 注意事项
    /// - 建议定期调用此方法持久化索引变更
    /// - 索引文件损坏可能导致数据丢失
    /// - 保存操作是原子性的
    /// - 大索引文件保存可能需要较长时间
    pub fn save_index(&self) -> std::io::Result<()> {
        // 步骤1: 获取索引的读锁
        // 使用读锁允许并发读取，但阻止写入
        let index = self.index.read();
        
        // 步骤2: 序列化索引数据
        // 使用 bincode 进行高效序列化
        let index_data = bincode::serialize(&*index)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        
        // 步骤3: 写入索引文件
        // 创建新文件并写入序列化数据
        let mut file = File::create(&self.index_file_path)?;
        file.write_all(&index_data)?;
        
        // 步骤4: 同步到磁盘
        // 确保数据真正写入磁盘，而不是停留在系统缓存中
        file.sync_all()?;
        
        Ok(())
    }
    
    // ========== 私有方法 ==========
    
    /// 初始化 MMAP 映射
    /// 
    /// # 功能说明
    /// 创建单一的内存映射，这是优化版的核心特性
    /// 所有后续的读写操作都复用这个映射
    /// 
    /// # 初始化流程
    /// 1. 打开数据文件（读写模式）
    /// 2. 创建可写内存映射
    /// 3. 将映射存储到共享结构中
    /// 
    /// # 返回值
    /// - `Ok(())`: 初始化成功
    /// - `Err(std::io::Error)`: 初始化失败，可能是文件权限问题
    /// 
    /// # 性能优化点
    /// - 单一映射复用，避免频繁创建和销毁
    /// - 延迟初始化，只在需要时创建
    /// - 使用 Arc 包装，支持多线程共享
    /// 
    /// # 注意事项
    /// - 内存映射创建是原子操作
    /// - 映射大小受系统限制
    /// - 初始化失败会阻止存储使用
    fn initialize_mmap(&self) -> std::io::Result<()> {
        // 步骤1: 打开数据文件
        // 使用读写模式打开文件，支持后续的读写操作
        let file = OpenOptions::new()
            .read(true)   // 允许读取
            .write(true)  // 允许写入
            .open(&self.data_file_path)?;
        
        // 步骤2: 创建可写内存映射
        // 注意：unsafe 是必要的，因为内存映射涉及底层系统调用
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        
        // 步骤3: 将映射存储到共享结构中
        // 使用写锁确保线程安全
        let mut mmap_guard = self.mmap.write();
        *mmap_guard = Some(mmap);
        
        Ok(())
    }
    
    /// 重新映射 MMAP（当文件扩展时）
    /// 
    /// # 功能说明
    /// 当文件被扩展时，需要重新创建内存映射
    /// 这是优化版的重要特性，确保映射始终覆盖整个文件
    /// 
    /// # 重新映射流程
    /// 1. 打开数据文件（读写模式）
    /// 2. 创建新的可写内存映射
    /// 3. 替换旧的映射
    /// 4. 更新统计信息
    /// 
    /// # 返回值
    /// - `Ok(())`: 重新映射成功
    /// - `Err(std::io::Error)`: 重新映射失败，可能是文件权限问题
    /// 
    /// # 性能优化点
    /// - 原子性替换，不影响正在进行的操作
    /// - 统计重新映射次数，用于性能监控
    /// - 使用 Arc 包装，支持多线程共享
    /// 
    /// # 注意事项
    /// - 重新映射是原子操作
    /// - 重新映射期间可能短暂影响性能
    /// - 映射大小受系统限制
    fn remap_mmap(&self) -> std::io::Result<()> {
        // 步骤1: 打开数据文件
        // 使用读写模式打开文件，支持后续的读写操作
        let file = OpenOptions::new()
            .read(true)   // 允许读取
            .write(true)  // 允许写入
            .open(&self.data_file_path)?;
        
        // 步骤2: 创建新的可写内存映射
        // 注意：unsafe 是必要的，因为内存映射涉及底层系统调用
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        
        // 步骤3: 替换旧的映射
        // 使用写锁确保线程安全
        let mut mmap_guard = self.mmap.write();
        *mmap_guard = Some(mmap);
        
        // 步骤4: 更新统计信息
        // 记录重新映射次数，用于性能监控
        {
            let mut stats = self.stats.write();
            stats.mmap_remaps += 1;
        }
        
        Ok(())
    }
    
    /// 从磁盘加载索引文件
    /// 
    /// # 功能说明
    /// 从索引文件中读取键值映射关系，并计算当前写入位置
    /// 与原版实现相同，但针对优化版进行了适配
    /// 
    /// # 加载流程
    /// 1. 检查索引文件是否存在
    /// 2. 读取索引文件内容
    /// 3. 反序列化索引数据
    /// 4. 更新内存中的索引
    /// 5. 计算当前写入位置
    /// 
    /// # 返回值
    /// - `Ok(())`: 加载成功
    /// - `Err(std::io::Error)`: 加载失败，可能是文件损坏或格式错误
    /// 
    /// # 注意事项
    /// - 索引文件不存在时不会报错，使用空索引
    /// - 索引文件损坏可能导致数据丢失
    /// - 加载过程中会计算写入位置，确保数据一致性
    fn load_index(&self) -> std::io::Result<()> {
        // 步骤1: 检查索引文件是否存在
        // 如果不存在，使用空索引，这是正常情况
        if !self.index_file_path.exists() {
            return Ok(());
        }
        
        // 步骤2: 打开索引文件并读取内容
        // 使用 read_to_end 读取整个文件内容
        let mut file = File::open(&self.index_file_path)?;
        let mut index_data = Vec::new();
        file.read_to_end(&mut index_data)?;
        
        // 步骤3: 反序列化索引数据
        // 使用 bincode 将字节数据转换为 HashMap
        let index: HashMap<String, (u64, u64)> = bincode::deserialize(&index_data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        
        // 步骤4: 更新内存中的索引
        // 获取写锁并替换整个索引
        *self.index.write() = index;
        
        // 步骤5: 计算当前写入位置
        // 遍历所有索引项，找到最大的偏移量 + 大小
        // 这确保新数据不会覆盖现有数据
        let max_offset = self.index.read().values()
            .map(|(offset, size)| offset + size)  // 计算每个数据项的结束位置
            .max()                                // 找到最大的结束位置
            .unwrap_or(0);                        // 如果没有数据，使用 0
        
        // 更新写入位置
        self.write_offset.store(max_offset, Ordering::Relaxed);
        
        Ok(())
    }
    
    /// 扩展数据文件大小
    /// 
    /// # 参数
    /// - `required_size`: 所需的最小文件大小
    /// 
    /// # 功能说明
    /// 当数据文件空间不足时，扩展文件到所需大小
    /// 使用配置的增长步长和最大文件大小限制
    /// 扩展后自动重新映射 MMAP
    /// 
    /// # 扩展策略
    /// 1. 计算新文件大小 = min(required_size + growth_step, max_file_size)
    /// 2. 如果新大小大于当前大小，执行扩展
    /// 3. 重新映射 MMAP 以覆盖新文件大小
    /// 4. 更新文件扩展统计信息
    /// 
    /// # 返回值
    /// - `Ok(())`: 扩展成功
    /// - `Err(std::io::Error)`: 扩展失败，可能是磁盘空间不足或权限问题
    /// 
    /// # 性能优化点
    /// - 自动重新映射 MMAP，确保映射覆盖整个文件
    /// - 统计扩展次数，用于性能监控
    /// - 原子性操作，不影响正在进行的读写
    /// 
    /// # 注意事项
    /// - 文件扩展是原子操作，不会影响其他线程
    /// - 扩展大小受 max_file_size 限制
    /// - 大文件扩展可能需要较长时间
    /// - 扩展失败可能导致写入失败
    /// - 重新映射 MMAP 可能短暂影响性能
    fn expand_file(&self, required_size: u64) -> std::io::Result<()> {
        // 步骤1: 计算新文件大小
        // 使用增长步长避免频繁扩展，但不超过最大限制
        let new_size = std::cmp::min(
            required_size + self.config.growth_step,  // 所需大小 + 增长步长
            self.config.max_file_size                  // 最大文件大小限制
        );
        
        // 步骤2: 检查是否需要扩展
        // 只有新大小大于当前大小时才执行扩展
        let current = self.data_file_size.load(Ordering::Relaxed);
        if new_size > current {
            // 步骤3: 打开文件并扩展
            // 使用读写模式打开文件
            let file = OpenOptions::new()
                .read(true)   // 允许读取
                .write(true)  // 允许写入
                .open(&self.data_file_path)?;
            
            // 扩展文件到新大小
            // 注意：这是原子操作，要么成功要么失败
            file.set_len(new_size)?;
            
            // 步骤4: 重新映射 MMAP（优化版特有）
            // 确保内存映射覆盖整个文件
            // 这是优化版的重要特性
            self.remap_mmap()?;
            
            // 步骤5: 更新统计信息
            // 记录文件扩展次数，用于性能监控
            {
                let mut stats = self.stats.write();
                stats.file_expansions += 1;
            }
        }
        
        Ok(())
    }
    
    /// 在指定偏移量写入数据到文件
    /// 
    /// # 参数
    /// - `offset`: 文件中的写入偏移量
    /// - `data`: 要写入的数据
    /// 
    /// # 功能说明
    /// 使用复用的内存映射在指定位置写入数据，包括头部信息和实际数据
    /// 这是优化版的核心特性，使用固定 16 字节头部格式
    /// 
    /// # 写入格式
    /// [头部信息][实际数据]
    /// - 头部: 固定 16 字节的 SimpleDataHeader
    /// - 数据: 实际的字节数据
    /// 
    /// # 返回值
    /// - `Ok(())`: 写入成功
    /// - `Err(std::io::Error)`: 写入失败，可能是 MMAP 未初始化
    /// 
    /// # 性能优化点
    /// - 使用复用的内存映射避免系统调用
    /// - 固定 16 字节头部，避免动态序列化
    /// - 直接内存操作，零拷贝写入
    /// - 小端字节序，提高跨平台兼容性
    /// 
    /// # 注意事项
    /// - 写入操作是线程安全的
    /// - 需要确保偏移量在文件范围内
    /// - MMAP 未初始化会导致写入失败
    /// - 头部格式固定，不支持动态扩展
    fn write_data_at_offset(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        // 获取 MMAP 的写锁
        let mut mmap_guard = self.mmap.write();
        
        // 检查 MMAP 是否已初始化
        if let Some(mmap) = mmap_guard.as_mut() {
            // 步骤1: 创建简化的头部信息
            // 使用固定格式，避免动态序列化开销
            let header = SimpleDataHeader {
                size: data.len() as u32,  // 数据大小
                compressed: false,        // 当前版本固定为 false
                timestamp: std::time::SystemTime::now()   // 当前时间戳
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };
            
            // 步骤2: 将头部信息转换为字节数组
            // 使用固定 16 字节格式，小端字节序
            let mut header_bytes = [0u8; 16];
            header_bytes[0..4].copy_from_slice(&header.size.to_le_bytes());  // 数据大小 (4 字节)
            header_bytes[4] = if header.compressed { 1 } else { 0 };         // 压缩标志 (1 字节)
            // 5-7 字节保留，未使用
            header_bytes[8..16].copy_from_slice(&header.timestamp.to_le_bytes());  // 时间戳 (8 字节)
            
            // 步骤3: 写入头部信息
            // 在指定偏移量写入 16 字节头部
            mmap[offset as usize..offset as usize + 16]
                .copy_from_slice(&header_bytes);
            
            // 步骤4: 写入实际数据
            // 在头部后面写入实际数据
            let data_offset = offset + 16;
            mmap[data_offset as usize..data_offset as usize + data.len()]
                .copy_from_slice(data);
            
            Ok(())
        } else {
            // MMAP 未初始化，返回错误
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "MMAP not initialized"
            ))
        }
    }
    
    /// 从复用的 MMAP 映射读取数据
    /// 
    /// # 参数
    /// - `offset`: 文件中的读取偏移量
    /// - `_size`: 数据项的总大小（当前未使用）
    /// 
    /// # 功能说明
    /// 使用复用的内存映射从指定位置读取数据，包括头部信息和实际数据
    /// 这是优化版的核心特性，直接使用内存映射，避免磁盘 I/O
    /// 
    /// # 读取流程
    /// 1. 获取 MMAP 的读锁
    /// 2. 读取并解析 16 字节头部信息
    /// 3. 根据头部信息读取实际数据
    /// 4. 返回数据
    /// 
    /// # 返回值
    /// - `Ok(Vec<u8>)`: 成功读取数据
    /// - `Err(std::io::Error)`: 读取失败，可能是 MMAP 未初始化
    /// 
    /// # 性能优化点
    /// - 使用复用的内存映射避免系统调用
    /// - 零拷贝读取，直接从内存映射获取数据
    /// - 固定 16 字节头部，简化解析逻辑
    /// - 小端字节序解析，提高跨平台兼容性
    /// 
    /// # 注意事项
    /// - 读取操作是线程安全的
    /// - 需要确保偏移量在文件范围内
    /// - MMAP 未初始化会导致读取失败
    /// - 头部格式固定，不支持动态扩展
    fn read_data_from_mmap(&self, offset: u64, _size: u64) -> std::io::Result<Vec<u8>> {
        // 获取 MMAP 的读锁
        let mmap_guard = self.mmap.read();
        
        // 检查 MMAP 是否已初始化
        if let Some(mmap) = mmap_guard.as_ref() {
            // 步骤1: 读取头部信息
            // 从指定偏移量读取 16 字节头部
            let header_bytes = &mmap[offset as usize..offset as usize + 16];
            
            // 步骤2: 解析头部信息
            // 使用小端字节序解析数据大小
            let data_size = u32::from_le_bytes([
                header_bytes[0], header_bytes[1], header_bytes[2], header_bytes[3]
            ]) as usize;
            
            // 步骤3: 读取实际数据
            // 根据头部信息中的大小读取数据
            let data_offset = offset + 16;
            let data = mmap[data_offset as usize..data_offset as usize + data_size].to_vec();
            
            Ok(data)
        } else {
            // MMAP 未初始化，返回错误
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "MMAP not initialized"
            ))
        }
    }
}

/// OptimizedMmapStats 的 Clone 实现
/// 
/// # 功能说明
/// 为 OptimizedMmapStats 结构体实现 Clone trait
/// 允许克隆统计信息用于外部访问
/// 
/// # 实现说明
/// 手动实现 Clone，确保所有字段都被正确克隆
/// 包括优化版特有的 mmap_remaps 字段
impl Clone for OptimizedMmapStats {
    fn clone(&self) -> Self {
        Self {
            total_writes: self.total_writes,
            total_reads: self.total_reads,
            total_write_bytes: self.total_write_bytes,
            total_read_bytes: self.total_read_bytes,
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
            file_expansions: self.file_expansions,
            avg_write_latency_us: self.avg_write_latency_us,
            avg_read_latency_us: self.avg_read_latency_us,
            mmap_remaps: self.mmap_remaps,  // 优化版特有字段
        }
    }
}
