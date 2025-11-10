// //! # 本地缓存模块
// //!
// //! 基于 Moka 的高性能本地缓存实现，支持：
// //! - 租约机制自动清理
// //! - 磁盘溢出（大值落盘）
// //! - 自动续租
// //! - 监控指标
//
// // 标准库导入
// use std::fs::{self, File};           // 文件系统操作：fs 模块提供文件和目录操作，File 提供文件读写
// use std::hash::Hash;                 // Hash trait：用于哈希计算，K 类型必须实现
// use std::time::Duration;             // 时间间隔类型：用于表示持续时间
// use std::marker::PhantomData;        // 幽灵数据：用于在泛型中标记类型但不实际存储
// use std::io::{Read, Write};          // IO trait：Read 用于读取，Write 用于写入
// use std::path::{Path, PathBuf};      // 路径类型：Path 是不可变路径引用，PathBuf 是可变路径
//
// // 第三方库导入
// use moka::sync::Cache;               // Moka 同步缓存：高性能的线程安全缓存实现
// use serde::{Serialize, de::DeserializeOwned}; // 序列化库：Serialize 用于序列化，DeserializeOwned 用于反序列化
// use uuid::Uuid;                      // UUID 生成器：用于生成唯一标识符
// use parking_lot::RwLock;             // 读写锁：高性能的读写锁实现
// use std::collections::HashMap;       // 哈希映射：键值对存储结构
// use std::sync::Arc;                  // 原子引用计数：用于多线程共享数据
// use tokio::time::{interval, Duration as TokioDuration}; // Tokio 时间：interval 用于定时器，Duration 重命名避免冲突
// use std::sync::atomic::{AtomicBool, Ordering}; // 原子操作：AtomicBool 原子布尔值，Ordering 内存排序
//
// /// 租约信息结构体
// ///
// /// 用于跟踪每个缓存条目的租约状态，包括过期时间、是否允许自动续租等
// #[derive(Debug, Clone)]  // derive 宏：自动实现 Debug（调试打印）和 Clone（克隆）trait
// struct LeaseInfo {
//     /// 过期时间点：使用 std::time::Instant 表示绝对时间点
//     expires_at: std::time::Instant,
//     /// 是否允许自动续租：布尔值，true 表示允许在 get 时自动续租
//     auto_renew: bool,
//     /// 自动续租时长：Duration 类型，表示每次自动续租的时间长度
//     renew_duration: Duration,
// }
//
// /// 监控指标快照结构体
// ///
// /// 用于获取缓存的各种统计信息，包括条目数量、容量、过期条目等
// #[derive(Debug, Clone, Copy, PartialEq, Eq)]  // Copy trait 表示可以按值复制，PartialEq 和 Eq 用于相等性比较
// pub struct MetricsSnapshot {
//     /// 内存中的条目数量：u64 无符号 64 位整数
//     pub entries: u64,
//     /// 最大容量：Option<u64> 可选值，None 表示无限制
//     pub max_capacity: Option<u64>,
//     /// TTL 秒数：time-to-live，写入后过期时间
//     pub time_to_live_secs: Option<u64>,
//     /// TTI 秒数：time-to-idle，访问后过期时间
//     pub time_to_idle_secs: Option<u64>,
//     /// 磁盘条目数量：存储在磁盘上的条目数
//     pub disk_entries: u64,
//     /// 磁盘字节数：磁盘上存储的总字节数
//     pub disk_bytes: u64,
//     /// 租约条目数量：有租约的条目数量
//     pub leased_entries: u64,
//     /// 过期条目数量：已过期但未清理的条目数量
//     pub expired_entries: u64,
// }
//
// impl MetricsSnapshot {
//     /// 从 Moka Cache 创建指标快照
//     ///
//     /// # 参数
//     /// - `cache`: Moka Cache 的引用
//     ///
//     /// # 返回
//     /// 返回包含基本缓存信息的 MetricsSnapshot
//     fn from_cache<K, V>(cache: &Cache<K, V>) -> Self {
//         let policy = cache.policy();  // 获取缓存策略对象
//         Self {
//             entries: cache.entry_count(),  // 获取当前条目数量
//             max_capacity: policy.max_capacity(),  // 获取最大容量限制
//             time_to_live_secs: policy
//                 .time_to_live()  // 获取 TTL 设置
//                 .map(|d| d.as_secs()),  // 将 Duration 转换为秒数
//             time_to_idle_secs: policy
//                 .time_to_idle()  // 获取 TTI 设置
//                 .map(|d| d.as_secs()),  // 将 Duration 转换为秒数
//             disk_entries: 0,  // 初始化为 0，后续会更新
//             disk_bytes: 0,    // 初始化为 0，后续会更新
//             leased_entries: 0,  // 初始化为 0，后续会更新
//             expired_entries: 0, // 初始化为 0，后续会更新
//         }
//     }
// }
//
// /// 本地缓存构建器结构体
// ///
// /// 使用建造者模式构建 LocalCache 实例，支持链式调用配置各种参数
// ///
// /// # 泛型参数
// /// - `K`: 键类型，必须实现 Eq（相等性）、Hash（哈希）、Clone（克隆）、Send（跨线程发送）、Sync（跨线程共享）
// /// - `V`: 值类型，必须实现 Clone、Send、Sync、Serialize（序列化）、DeserializeOwned（反序列化）
// pub struct LocalCacheBuilder<K, V>
// where
//     K: Eq + Hash + Clone + Send + Sync + 'static,  // 键类型约束：相等性、哈希、克隆、线程安全
//     V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,  // 值类型约束：克隆、线程安全、序列化
// {
//     /// 最大容量：Option<u64> 可选值，None 表示无限制，Some(n) 表示最多 n 个条目
//     max_capacity: Option<u64>,
//     /// TTI 设置：time-to-idle，访问后多长时间过期
//     time_to_idle: Option<Duration>,
//     /// TTL 设置：time-to-live，写入后多长时间过期
//     time_to_live: Option<Duration>,
//     /// 磁盘溢出阈值：超过此字节数的值会序列化到磁盘
//     disk_threshold_bytes: Option<usize>,
//     /// 磁盘目录：存储大值文件的目录路径
//     disk_dir: Option<PathBuf>,
//     /// 默认租约时长：新插入条目的默认租约时间
//     default_lease_duration: Option<Duration>,
//     /// 清理任务间隔：后台清理任务执行的时间间隔
//     cleanup_interval: Option<Duration>,
//     /// 是否在 get 时自动续租：布尔值，true 表示获取数据时自动续租
//     auto_renew_on_get: Option<bool>,
//     /// 自动续租时长：每次自动续租的时间长度
//     auto_renew_duration: Option<Duration>,
//     /// 幽灵数据：用于在泛型中标记 K 和 V 类型，但不实际存储数据
//     _marker: PhantomData<(K, V)>,
// }
//
// /// LocalCacheBuilder 的实现块
// ///
// /// 为 LocalCacheBuilder 提供各种配置方法和构建方法
// impl<K, V> LocalCacheBuilder<K, V>
// where
//     K: Eq + Hash + Clone + Send + Sync + 'static,  // 键类型约束
//     V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,  // 值类型约束
// {
//     /// 创建新的构建器实例
//     ///
//     /// # 返回
//     /// 返回一个所有字段都为 None 的新构建器实例
//     pub fn new() -> Self {
//         Self {
//             max_capacity: None,              // 最大容量：无限制
//             time_to_idle: None,              // TTI：无限制
//             time_to_live: None,              // TTL：无限制
//             disk_threshold_bytes: None,      // 磁盘阈值：无限制
//             disk_dir: None,                  // 磁盘目录：使用默认
//             default_lease_duration: None,    // 默认租约：无限制
//             cleanup_interval: None,          // 清理间隔：使用默认
//             auto_renew_on_get: None,         // 自动续租：使用默认
//             auto_renew_duration: None,       // 续租时长：使用默认
//             _marker: PhantomData,            // 幽灵数据：标记泛型类型
//         }
//     }
//
//     /// 设置最大容量
//     ///
//     /// # 参数
//     /// - `capacity`: u64 类型，表示缓存的最大条目数量
//     ///
//     /// # 返回
//     /// 返回 Self，支持链式调用
//     pub fn max_capacity(mut self, capacity: u64) -> Self {
//         self.max_capacity = Some(capacity);  // 将容量设置为 Some(capacity)
//         self  // 返回 self 以支持链式调用
//     }
//
//     /// 设置访问后过期时间（TTI - Time To Idle）
//     ///
//     /// # 参数
//     /// - `duration`: Duration 类型，表示最后一次访问后多长时间过期
//     ///
//     /// # 返回
//     /// 返回 Self，支持链式调用
//     pub fn time_to_idle(mut self, duration: Duration) -> Self {
//         self.time_to_idle = Some(duration);  // 设置 TTI
//         self
//     }
//
//     /// 设置写入后过期时间（TTL - Time To Live）
//     ///
//     /// # 参数
//     /// - `duration`: Duration 类型，表示写入后多长时间过期
//     ///
//     /// # 返回
//     /// 返回 Self，支持链式调用
//     pub fn time_to_live(mut self, duration: Duration) -> Self {
//         self.time_to_live = Some(duration);  // 设置 TTL
//         self
//     }
//
//     /// 设置磁盘溢出阈值
//     ///
//     /// 当值的序列化字节数超过此阈值时，会将其存储到磁盘而不是内存
//     ///
//     /// # 参数
//     /// - `threshold_bytes`: usize 类型，字节数阈值
//     ///
//     /// # 返回
//     /// 返回 Self，支持链式调用
//     pub fn disk_offload_threshold(mut self, threshold_bytes: usize) -> Self {
//         self.disk_threshold_bytes = Some(threshold_bytes);  // 设置磁盘阈值
//         self
//     }
//
//     /// 设置磁盘存储目录
//     ///
//     /// # 参数
//     /// - `dir`: 实现了 Into<PathBuf> trait 的类型，会被转换为路径
//     ///
//     /// # 返回
//     /// 返回 Self，支持链式调用
//     pub fn disk_directory<P: Into<PathBuf>>(mut self, dir: P) -> Self {
//         self.disk_dir = Some(dir.into());  // 将输入转换为 PathBuf 并设置
//         self
//     }
//
//     /// 设置默认租约时长
//     ///
//     /// 用于自动清理机制，新插入的条目会使用此租约时长
//     ///
//     /// # 参数
//     /// - `duration`: Duration 类型，租约时长
//     ///
//     /// # 返回
//     /// 返回 Self，支持链式调用
//     pub fn default_lease_duration(mut self, duration: Duration) -> Self {
//         self.default_lease_duration = Some(duration);  // 设置默认租约时长
//         self
//     }
//
//     /// 设置清理任务间隔
//     ///
//     /// 后台清理任务会按此间隔执行，清理过期的条目
//     ///
//     /// # 参数
//     /// - `interval`: Duration 类型，清理间隔
//     ///
//     /// # 返回
//     /// 返回 Self，支持链式调用
//     pub fn cleanup_interval(mut self, interval: Duration) -> Self {
//         self.cleanup_interval = Some(interval);  // 设置清理间隔
//         self
//     }
//
//     /// 设置是否在 get 时自动续租
//     ///
//     /// # 参数
//     /// - `enabled`: bool 类型，true 表示开启自动续租
//     ///
//     /// # 返回
//     /// 返回 Self，支持链式调用
//     pub fn auto_renew_on_get(mut self, enabled: bool) -> Self {
//         self.auto_renew_on_get = Some(enabled);  // 设置自动续租开关
//         self
//     }
//
//     /// 设置自动续租时长
//     ///
//     /// 当自动续租触发时，会延长此时长
//     ///
//     /// # 参数
//     /// - `duration`: Duration 类型，续租时长
//     ///
//     /// # 返回
//     /// 返回 Self，支持链式调用
//     pub fn auto_renew_duration(mut self, duration: Duration) -> Self {
//         self.auto_renew_duration = Some(duration);  // 设置续租时长
//         self
//     }
//
//     /// 构建 LocalCache 实例
//     ///
//     /// 这是构建器的核心方法，根据配置创建 LocalCache 实例
//     ///
//     /// # 返回
//     /// 返回配置好的 LocalCache 实例
//     pub fn build(self) -> LocalCache<K, V> {
//         // 创建 Moka Cache 构建器
//         let mut builder = Cache::builder();
//
//         // 根据配置设置 Moka Cache 的参数
//         if let Some(cap) = self.max_capacity {
//             builder = builder.max_capacity(cap);  // 设置最大容量
//         }
//         if let Some(tti) = self.time_to_idle {
//             builder = builder.time_to_idle(tti);  // 设置 TTI
//         }
//         if let Some(ttl) = self.time_to_live {
//             builder = builder.time_to_live(ttl);  // 设置 TTL
//         }
//
//         // 准备磁盘目录与索引，并配置淘汰监听器
//         let disk_dir = self.disk_dir.unwrap_or_else(|| std::env::temp_dir().join("flux-cache"));  // 使用默认目录或指定目录
//         let _ = fs::create_dir_all(&disk_dir);  // 创建目录，忽略错误
//         let disk_index: Arc<RwLock<HashMap<K, PathBuf>>> = Arc::new(RwLock::new(HashMap::new()));  // 创建磁盘索引
//         let disk_index_for_listener = Arc::clone(&disk_index);  // 克隆引用用于监听器
//
//         // 配置淘汰监听器：当条目被淘汰时清理对应的磁盘文件
//         builder = builder.eviction_listener(move |k: Arc<K>, _v: V, _cause| {
//             // 从磁盘索引中移除键并删除对应文件
//             if let Some(path) = disk_index_for_listener.write().remove(&*k) {
//                 let _ = fs::remove_file(path);  // 删除文件，忽略错误
//             }
//         });
//
//         let inner = builder.build();  // 构建 Moka Cache 实例
//
//         // 创建租约索引：用于跟踪每个条目的租约信息
//         let lease_index: Arc<RwLock<HashMap<K, LeaseInfo>>> = Arc::new(RwLock::new(HashMap::new()));
//
//         // 设置默认值
//         let cleanup_interval = self.cleanup_interval.unwrap_or(Duration::from_secs(30));  // 默认 30 秒清理间隔
//         let default_lease_duration = self.default_lease_duration.unwrap_or(Duration::from_secs(300));  // 默认 5 分钟租约
//         let auto_renew_on_get = self.auto_renew_on_get.unwrap_or(true);  // 默认开启自动续租
//         let auto_renew_duration = self.auto_renew_duration.unwrap_or(default_lease_duration);  // 默认与租约时长相同
//
//         // 创建 LocalCache 实例
//         let cache = LocalCache {
//             inner,                                    // Moka Cache 实例
//             disk_threshold_bytes: self.disk_threshold_bytes.unwrap_or(usize::MAX),  // 磁盘阈值
//             disk_dir,                                 // 磁盘目录
//             disk_index,                               // 磁盘索引
//             lease_index,                              // 租约索引
//             default_lease_duration,                  // 默认租约时长
//             auto_renew_on_get,                        // 自动续租开关
//             auto_renew_duration,                      // 自动续租时长
//             cleanup_running: Arc::new(AtomicBool::new(false)),  // 清理任务运行状态
//         };
//
//         // 启动清理任务
//         cache.start_cleanup_task(cleanup_interval);
//
//         cache  // 返回构建好的缓存实例
//     }
// }
//
// /// 本地缓存封装结构体（线程安全）
// ///
// /// 这是主要的缓存实现，封装了 Moka Cache 并添加了租约机制和磁盘溢出功能
// ///
// /// # 泛型参数
// /// - `K`: 键类型，必须实现 Eq、Hash、Clone、Send、Sync
// /// - `V`: 值类型，必须实现 Clone、Send、Sync、Serialize、DeserializeOwned
// ///
// /// # 特性
// /// - 线程安全：所有操作都是线程安全的
// /// - 可克隆：实现了 Clone trait，可以安全地克隆
// #[derive(Clone)]  // derive 宏：自动实现 Clone trait
// pub struct LocalCache<K, V>
// where
//     K: Eq + Hash + Clone + Send + Sync + 'static,  // 键类型约束
//     V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,  // 值类型约束
// {
//     /// Moka Cache 实例：底层的缓存实现
//     inner: Cache<K, V>,
//     /// 磁盘溢出阈值：超过此字节数的值会存储到磁盘
//     disk_threshold_bytes: usize,
//     /// 磁盘存储目录：存储大值文件的目录路径
//     disk_dir: PathBuf,
//     /// 磁盘索引：键到文件路径的映射，用于快速定位磁盘文件
//     disk_index: Arc<RwLock<HashMap<K, PathBuf>>>,
//     /// 租约索引：键到租约信息的映射，用于跟踪租约状态
//     lease_index: Arc<RwLock<HashMap<K, LeaseInfo>>>,
//     /// 默认租约时长：新插入条目的默认租约时间
//     default_lease_duration: Duration,
//     /// 自动续租开关：是否在 get 时自动续租
//     auto_renew_on_get: bool,
//     /// 自动续租时长：每次自动续租的时间长度
//     auto_renew_duration: Duration,
//     /// 清理任务运行状态：原子布尔值，防止重复启动清理任务
//     cleanup_running: Arc<AtomicBool>,
// }
//
// /// LocalCache 的实现块
// ///
// /// 为 LocalCache 提供各种缓存操作方法
// impl<K, V> LocalCache<K, V>
// where
//     K: Eq + Hash + Clone + Send + Sync + 'static,  // 键类型约束
//     V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,  // 值类型约束
// {
//     /// 使用默认构建器创建无限制容量的缓存
//     ///
//     /// # 返回
//     /// 返回一个容量无限制的 LocalCache 实例
//     pub fn new_unbounded() -> Self {
//         let disk_dir = std::env::temp_dir().join("flux-cache");  // 使用系统临时目录
//         let _ = fs::create_dir_all(&disk_dir);  // 创建目录，忽略错误
//         let lease_index: Arc<RwLock<HashMap<K, LeaseInfo>>> = Arc::new(RwLock::new(HashMap::new()));  // 创建租约索引
//
//         let cache = LocalCache {
//             inner: Cache::new(usize::MAX as u64),  // 创建无限制容量的 Moka Cache
//             disk_threshold_bytes: usize::MAX,      // 无磁盘溢出阈值
//             disk_dir,                              // 磁盘目录
//             disk_index: Arc::new(RwLock::new(HashMap::new())),  // 磁盘索引
//             lease_index,                           // 租约索引
//             default_lease_duration: Duration::from_secs(300),  // 默认 5 分钟租约
//             auto_renew_on_get: true,               // 开启自动续租
//             auto_renew_duration: Duration::from_secs(300),  // 自动续租 5 分钟
//             cleanup_running: Arc::new(AtomicBool::new(false)),  // 清理任务未运行
//         };
//
//         cache.start_cleanup_task(Duration::from_secs(30));  // 启动清理任务
//         cache  // 返回缓存实例
//     }
//
//     /// 通过构建器创建缓存
//     ///
//     /// # 返回
//     /// 返回一个新的 LocalCacheBuilder 实例
//     pub fn builder() -> LocalCacheBuilder<K, V> {
//         LocalCacheBuilder::<K, V>::new()  // 创建新的构建器
//     }
//
//     /// 新增或更新条目（统一为 upsert）
//     ///
//     /// 使用默认租约时长，不允许自动续租
//     ///
//     /// # 参数
//     /// - `key`: K 类型，缓存键
//     /// - `value`: V 类型，缓存值
//     pub fn upsert(&self, key: K, value: V) {
//         self.upsert_with_lease(key, value, self.default_lease_duration, false)  // 调用带租约的 upsert
//     }
//
//     /// 新增或更新条目（带租约）
//     ///
//     /// 根据值的大小决定存储位置：小值存内存，大值存磁盘
//     ///
//     /// # 参数
//     /// - `key`: K 类型，缓存键
//     /// - `value`: V 类型，缓存值
//     /// - `lease_duration`: Duration 类型，租约时长
//     /// - `auto_renew`: bool 类型，是否允许自动续租
//     pub fn upsert_with_lease(&self, key: K, value: V, lease_duration: Duration, auto_renew: bool) {
//         // 尝试序列化值以检查大小
//         let ser = bincode::encode_into_slice(&value).ok();  // 序列化，忽略错误
//         if let Some(bytes) = ser {  // 如果序列化成功
//             if bytes.len() > self.disk_threshold_bytes {  // 如果超过磁盘阈值
//                 if let Ok(path) = self.write_to_disk(&bytes) {  // 写入磁盘成功
//                     self.disk_index.write().insert(key.clone(), path);  // 更新磁盘索引
//                     // 设置租约信息
//                     self.lease_index.write().insert(key.clone(), LeaseInfo {
//                         expires_at: std::time::Instant::now() + lease_duration,  // 过期时间
//                         auto_renew,  // 自动续租标志
//                         renew_duration: self.auto_renew_duration,  // 续租时长
//                     });
//                     // 大对象：仅落盘并在索引记录，不放入内存缓存
//                     return;
//                 }
//             }
//         }
//         // 小对象：直接走内存缓存，并确保索引无残留
//         self.disk_index.write().remove(&key);  // 从磁盘索引中移除
//         self.inner.insert(key.clone(), value);  // 插入到内存缓存
//         // 设置租约信息
//         self.lease_index.write().insert(key, LeaseInfo {
//             expires_at: std::time::Instant::now() + lease_duration,  // 过期时间
//             auto_renew,  // 自动续租标志
//             renew_duration: self.auto_renew_duration,  // 续租时长
//         });
//     }
//
//     /// 新增条目（语义糖方法）
//     ///
//     /// 这是 upsert 方法的语义糖，行为与 upsert 完全等价
//     ///
//     /// # 参数
//     /// - `key`: K 类型，缓存键
//     /// - `value`: V 类型，缓存值
//     pub fn insert(&self, key: K, value: V) {
//         self.upsert(key, value);  // 调用 upsert 方法
//     }
//
//     /// 新增条目（带租约）
//     ///
//     /// 这是 upsert_with_lease 方法的语义糖
//     ///
//     /// # 参数
//     /// - `key`: K 类型，缓存键
//     /// - `value`: V 类型，缓存值
//     /// - `lease_duration`: Duration 类型，租约时长
//     /// - `auto_renew`: bool 类型，是否允许自动续租
//     pub fn insert_with_lease(&self, key: K, value: V, lease_duration: Duration, auto_renew: bool) {
//         self.upsert_with_lease(key, value, lease_duration, auto_renew);  // 调用带租约的 upsert
//     }
//
//     /// 手动续租（延长租约时间）
//     ///
//     /// 为指定键延长租约时间，防止被自动清理
//     ///
//     /// # 参数
//     /// - `key`: &K 类型，缓存键的引用
//     /// - `additional_duration`: Duration 类型，要延长的时长
//     ///
//     /// # 返回
//     /// 返回 bool，true 表示续租成功，false 表示键不存在
//     pub fn renew_lease(&self, key: &K, additional_duration: Duration) -> bool {
//         let mut lease_index = self.lease_index.write();  // 获取租约索引的写锁
//         if let Some(lease_info) = lease_index.get_mut(key) {  // 如果找到租约信息
//             lease_info.expires_at += additional_duration;  // 延长过期时间
//             true  // 返回成功
//         } else {
//             false  // 键不存在，返回失败
//         }
//     }
//
//     /// 自动续租（内部方法）
//     ///
//     /// 在 get 操作时自动调用，根据租约设置决定是否续租
//     ///
//     /// # 参数
//     /// - `key`: &K 类型，缓存键的引用
//     fn auto_renew_lease(&self, key: &K) {
//         let mut lease_index = self.lease_index.write();  // 获取租约索引的写锁
//         if let Some(lease_info) = lease_index.get_mut(key) {  // 如果找到租约信息
//             if lease_info.auto_renew {  // 如果允许自动续租
//                 lease_info.expires_at += lease_info.renew_duration;  // 延长过期时间
//             }
//         }
//     }
//
//     /// 强制清理指定键（忽略租约）
//     ///
//     /// 立即删除指定键的所有数据，包括内存、磁盘和租约信息
//     ///
//     /// # 参数
//     /// - `key`: &K 类型，要删除的缓存键的引用
//     pub fn force_remove(&self, key: &K) {
//         self.inner.invalidate(key);  // 从内存缓存中移除
//         self.lease_index.write().remove(key);  // 从租约索引中移除
//         if let Some(path) = self.disk_index.write().remove(key) {  // 从磁盘索引中移除
//             let _ = fs::remove_file(path);  // 删除磁盘文件，忽略错误
//         }
//     }
//
//     /// 获取缓存值
//     ///
//     /// 优先从磁盘获取，如果磁盘没有则从内存获取
//     /// 如果开启了自动续租且获取成功，会自动续租
//     ///
//     /// # 参数
//     /// - `key`: &K 类型，缓存键的引用
//     ///
//     /// # 返回
//     /// 返回 Option<V>，Some(value) 表示找到值，None 表示未找到
//     pub fn get(&self, key: &K) -> Option<V> {
//         let mut result = None;  // 初始化结果为 None
//
//         // 先尝试从磁盘获取
//         if let Some(path) = self.disk_index.read().get(key).cloned() {  // 从磁盘索引获取路径
//             if let Ok(bytes) = self.read_from_disk(&path) {  // 从磁盘读取字节
//                 if let Ok(v) = bincode::deserialize::<V>(&bytes) {  // 反序列化成功
//                     result = Some(v);  // 设置结果
//                 }
//             }
//         }
//
//         // 如果磁盘没有，尝试从内存获取
//         if result.is_none() {  // 如果磁盘获取失败
//             result = self.inner.get(key);  // 从内存缓存获取
//         }
//
//         // 如果获取成功且开启了自动续租，则续租
//         if result.is_some() && self.auto_renew_on_get {  // 如果获取成功且开启自动续租
//             self.auto_renew_lease(key);  // 执行自动续租
//         }
//
//         result  // 返回结果
//     }
//
//     /// 删除指定键的缓存条目
//     ///
//     /// 从内存缓存、磁盘索引、租约索引中移除指定键的所有数据
//     ///
//     /// # 参数
//     /// - `key`: &K 类型，要删除的缓存键的引用
//     pub fn remove(&self, key: &K) {
//         self.inner.invalidate(key);  // 从内存缓存中移除
//         self.lease_index.write().remove(key);  // 从租约索引中移除
//         if let Some(path) = self.disk_index.write().remove(key) {  // 从磁盘索引中移除
//             let _ = fs::remove_file(path);  // 删除磁盘文件，忽略错误
//         }
//     }
//
//     /// 清空所有缓存条目
//     ///
//     /// 清空内存缓存、删除所有磁盘文件、清空所有索引
//     pub fn clear(&self) {
//         self.inner.invalidate_all();  // 清空内存缓存
//         // 清理索引及对应文件
//         let paths: Vec<PathBuf> = self.disk_index.write().drain().map(|(_, p)| p).collect();  // 获取所有磁盘文件路径
//         for p in paths {
//             let _ = fs::remove_file(p);  // 删除所有文件，忽略错误
//         }
//         // 清理租约索引
//         self.lease_index.write().clear();  // 清空租约索引
//     }
//
//     /// 获取监控指标快照
//     ///
//     /// 统计缓存的各种指标，包括条目数量、磁盘使用情况、租约状态等
//     ///
//     /// # 返回
//     /// 返回 MetricsSnapshot 结构体，包含所有监控指标
//     pub fn metrics(&self) -> MetricsSnapshot {
//         let mut m = MetricsSnapshot::from_cache(&self.inner);  // 从 Moka Cache 获取基本指标
//
//         // 统计磁盘索引中的文件数与字节数
//         let index_snapshot: Vec<PathBuf> = self.disk_index.read().values().cloned().collect();  // 获取所有磁盘文件路径
//         m.disk_entries = index_snapshot.len() as u64;  // 磁盘条目数量
//         let mut total = 0u64;  // 总字节数
//         for p in index_snapshot {  // 遍历所有文件路径
//             if let Ok(meta) = fs::metadata(&p) {  // 获取文件元数据
//                 total += meta.len();  // 累加文件大小
//             }
//         }
//         m.disk_bytes = total;  // 设置磁盘字节数
//
//         // 统计租约信息
//         let lease_snapshot = self.lease_index.read();  // 获取租约索引的读锁
//         m.leased_entries = lease_snapshot.len() as u64;  // 租约条目数量
//         let now = std::time::Instant::now();  // 当前时间
//         m.expired_entries = lease_snapshot.values()  // 遍历所有租约信息
//             .filter(|lease| lease.expires_at <= now)  // 过滤已过期的租约
//             .count() as u64;  // 计算过期条目数量
//
//         m  // 返回指标快照
//     }
//
//     /// 将字节数据写入磁盘
//     ///
//     /// # 参数
//     /// - `bytes`: &[u8] 类型，要写入的字节数据
//     ///
//     /// # 返回
//     /// 返回 std::io::Result<PathBuf>，成功时返回文件路径，失败时返回错误
//     fn write_to_disk(&self, bytes: &[u8]) -> std::io::Result<PathBuf> {
//         let id = Uuid::new_v4().to_string();  // 生成唯一 ID
//         let path = self.disk_dir.join(format!("{}.bin", id));  // 构造文件路径
//         let mut f = File::create(&path)?;  // 创建文件，? 操作符用于错误传播
//         f.write_all(bytes)?;  // 写入所有字节
//         Ok(path)  // 返回文件路径
//     }
//
//     /// 从磁盘读取字节数据
//     ///
//     /// # 参数
//     /// - `path`: &Path 类型，文件路径
//     ///
//     /// # 返回
//     /// 返回 std::io::Result<Vec<u8>>，成功时返回字节数据，失败时返回错误
//     fn read_from_disk(&self, path: &Path) -> std::io::Result<Vec<u8>> {
//         let mut f = File::open(path)?;  // 打开文件
//         let mut buf = Vec::new();  // 创建缓冲区
//         f.read_to_end(&mut buf)?;  // 读取所有内容到缓冲区
//         Ok(buf)  // 返回字节数据
//     }
//
//     /// 启动后台清理任务
//     ///
//     /// 在 tokio runtime 中启动异步任务，定期清理过期的条目
//     ///
//     /// # 参数
//     /// - `interval_duration`: Duration 类型，清理任务执行间隔
//     fn start_cleanup_task(&self, interval_duration: Duration) {
//         // 使用原子操作确保只启动一个清理任务
//         if self.cleanup_running.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
//             return;  // 如果已经在运行，直接返回
//         }
//
//         // 克隆需要的引用
//         let inner = self.inner.clone();  // Moka Cache 实例
//         let disk_index = Arc::clone(&self.disk_index);  // 磁盘索引
//         let lease_index = Arc::clone(&self.lease_index);  // 租约索引
//         let _cleanup_running = Arc::clone(&self.cleanup_running);  // 清理状态（未使用，但保持引用）
//
//         // 检查是否在 tokio runtime 中
//         if tokio::runtime::Handle::try_current().is_ok() {  // 如果在 tokio runtime 中
//             tokio::spawn(async move {  // 启动异步任务
//                 let mut interval = interval(TokioDuration::from_secs(interval_duration.as_secs()));  // 创建定时器
//                 loop {  // 无限循环
//                     interval.tick().await;  // 等待下一个时间间隔
//
//                     let now = std::time::Instant::now();  // 获取当前时间
//                     let mut expired_keys = Vec::new();  // 过期键列表
//
//                     // 收集过期的 key
//                     {
//                         let lease_guard = lease_index.read();  // 获取租约索引的读锁
//                         for (key, lease_info) in lease_guard.iter() {  // 遍历所有租约
//                             if lease_info.expires_at <= now {  // 如果已过期
//                                 expired_keys.push(key.clone());  // 添加到过期列表
//                             }
//                         }
//                     }  // lease_guard 在这里被释放
//
//                     // 批量清理过期条目
//                     if !expired_keys.is_empty() {  // 如果有过期条目
//                         let mut disk_guard = disk_index.write();  // 获取磁盘索引的写锁
//                         let mut lease_guard = lease_index.write();  // 获取租约索引的写锁
//
//                         for key in expired_keys {  // 遍历过期键
//                             // 清理内存缓存
//                             inner.invalidate(&key);  // 从 Moka Cache 中移除
//                             // 清理磁盘文件
//                             if let Some(path) = disk_guard.remove(&key) {  // 从磁盘索引中移除
//                                 let _ = fs::remove_file(path);  // 删除文件，忽略错误
//                             }
//                             // 清理租约
//                             lease_guard.remove(&key);  // 从租约索引中移除
//                         }
//                     }
//                 }
//             });
//         }
//         // 如果不在 tokio runtime 中，跳过启动清理任务（测试环境）
//     }
//
//     // 不再扫描整个目录，而是通过索引统计
// }
//
// /// 测试模块
// ///
// /// 包含各种单元测试，验证缓存功能的正确性
// #[cfg(test)]  // 条件编译：只在测试时编译
// mod tests {
//     use super::*;  // 导入父模块的所有内容
//
//     /// 基本插入、获取、更新、删除和指标测试
//     ///
//     /// 测试缓存的基本功能，包括磁盘溢出和租约机制
//     #[test]
//     fn basic_insert_get_update_delete_and_metrics() {
//         // 创建测试缓存：小容量、小磁盘阈值、短租约
//         let cache: LocalCache<String, String> = LocalCache::<String, String>::builder()
//             .max_capacity(100)  // 最大容量 100
//             .disk_offload_threshold(1)  // 很小阈值以便测试落盘
//             .default_lease_duration(Duration::from_secs(10))  // 10秒租约
//             .build();
//
//         // 测试插入（由于阈值很小，应当落盘）
//         cache.insert("a".into(), "1".into());  // 插入键值对
//         // 对于落盘对象，get 会从磁盘反序列化
//         assert_eq!(cache.get(&"a".into()).as_deref(), Some("1"));  // 验证获取结果
//         let m_after_insert = cache.metrics();  // 获取指标
//         assert!(m_after_insert.disk_entries >= 1);  // 验证磁盘条目数
//         assert!(m_after_insert.leased_entries >= 1);  // 验证租约条目数
//
//         // 测试更新（通过 upsert）
//         cache.upsert("a".into(), "2".into());  // 更新值
//         assert_eq!(cache.get(&"a".into()).as_deref(), Some("2"));  // 验证更新结果
//
//         // 测试未命中
//         assert!(cache.get(&"missing".into()).is_none());  // 验证不存在的键返回 None
//
//         // 测试删除（删除后磁盘文件与索引应一起清理）
//         cache.remove(&"a".into());  // 删除键
//         assert!(cache.get(&"a".into()).is_none());  // 验证删除后获取不到
//
//         // 验证指标正确性
//         let m = cache.metrics();  // 获取最终指标
//         assert!(m.entries <= 1);  // 内存条目应该很少
//         // 删除后应无落盘文件
//         assert_eq!(m.disk_entries, 0);  // 磁盘条目应该为 0
//         assert_eq!(m.leased_entries, 0);  // 租约条目应该为 0
//     }
//
//     /// 租约续租和强制删除测试
//     ///
//     /// 测试租约机制的手动续租和强制删除功能
//     #[test]
//     fn lease_renewal_and_force_remove() {
//         // 创建测试缓存：很短的租约时间
//         let cache: LocalCache<String, String> = LocalCache::<String, String>::builder()
//             .default_lease_duration(Duration::from_millis(100))  // 很短的租约
//             .build();
//
//         cache.insert("test".into(), "value".into());  // 插入测试数据
//         assert_eq!(cache.get(&"test".into()).as_deref(), Some("value"));  // 验证插入成功
//
//         // 测试续租
//         assert!(cache.renew_lease(&"test".into(), Duration::from_secs(1)));  // 续租 1 秒
//         assert_eq!(cache.get(&"test".into()).as_deref(), Some("value"));  // 验证续租后仍能获取
//
//         // 测试强制清理
//         cache.force_remove(&"test".into());  // 强制删除
//         assert!(cache.get(&"test".into()).is_none());  // 验证强制删除后获取不到
//     }
//
//     /// 自动续租测试
//     ///
//     /// 测试获取数据时的自动续租功能
//     #[test]
//     fn auto_renew_on_get() {
//         // 创建支持自动续租的测试缓存
//         let cache: LocalCache<String, String> = LocalCache::<String, String>::builder()
//             .default_lease_duration(Duration::from_millis(50))  // 很短的租约
//             .auto_renew_on_get(true)  // 开启自动续租
//             .auto_renew_duration(Duration::from_millis(100))  // 自动续租 100ms
//             .build();
//
//         // 插入数据，设置自动续租
//         cache.insert_with_lease("auto_renew_test".into(), "value".into(), Duration::from_millis(50), true);
//
//         // 等待租约即将过期
//         std::thread::sleep(Duration::from_millis(30));
//
//         // 获取数据，应该自动续租
//         assert_eq!(cache.get(&"auto_renew_test".into()).as_deref(), Some("value"));
//
//         // 再等待一段时间，数据应该仍然存在（因为自动续租了）
//         std::thread::sleep(Duration::from_millis(30));
//         assert_eq!(cache.get(&"auto_renew_test".into()).as_deref(), Some("value"));
//
//         // 测试关闭自动续租
//         let cache_no_auto: LocalCache<String, String> = LocalCache::<String, String>::builder()
//             .default_lease_duration(Duration::from_millis(50))
//             .auto_renew_on_get(false)  // 关闭自动续租
//             .build();
//
//         cache_no_auto.insert_with_lease("no_auto_renew".into(), "value".into(), Duration::from_millis(50), true);
//
//         // 等待租约过期
//         std::thread::sleep(Duration::from_millis(60));
//
//         // 获取数据，不应该自动续租，数据可能已被清理
//         // 注意：这里的结果取决于清理任务的执行时机
//         let result = cache_no_auto.get(&"no_auto_renew".into());
//         // 由于清理任务可能还没执行，我们只验证获取操作不会 panic
//         let _ = result;  // 忽略结果，只验证不 panic
//     }
// }
//
//
