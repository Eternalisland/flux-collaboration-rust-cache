// //! # JNI 桥接模块
// //!
// //! 提供 Java Native Interface (JNI) 支持，允许 Java 代码调用 Rust 缓存功能
// //!
// //! ## 功能特性
// //! - 创建和管理缓存实例
// //! - 设置缓存参数（容量、磁盘阈值等）
// //! - 添加、删除、获取缓存数据
// //! - 查看缓存状态信息
// //! - 支持 JSON 序列化进行数据交换
//
// use std::collections::HashMap;
// use std::sync::{Arc, Mutex};
// use std::time::Duration;
//
// use jni::objects::{JClass, JString};
// use jni::sys::{jboolean, jlong, jstring};
// use jni::JNIEnv;
// use serde_json::json;
//
// // use crate::cache::LocalCache;
//
// /// 全局缓存实例存储
// ///
// /// 使用 Arc<Mutex<HashMap>> 存储多个缓存实例，支持 Java 创建多个独立的缓存
// type CacheStorage = Arc<Mutex<HashMap<u64, LocalCache<String, String>>>>;
//
// /// 全局缓存存储实例
// ///
// /// 线程安全的缓存实例存储，键为缓存 ID，值为缓存实例
// static CACHE_STORAGE: std::sync::OnceLock<CacheStorage> = std::sync::OnceLock::new();
//
// /// 获取全局缓存存储实例
// ///
// /// # 返回
// /// 返回 CacheStorage 的引用
// fn get_cache_storage() -> &'static CacheStorage {
//     CACHE_STORAGE.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
// }
//
// /// 生成新的缓存 ID
// ///
// /// # 返回
// /// 返回一个唯一的缓存 ID
// fn generate_cache_id() -> u64 {
//     use std::sync::atomic::{AtomicU64, Ordering};
//     static COUNTER: AtomicU64 = AtomicU64::new(1);
//     COUNTER.fetch_add(1, Ordering::SeqCst)
// }
//
// /// 创建新的缓存实例
// ///
// /// # 参数
// /// - `env`: JNI 环境
// /// - `class`: Java 类对象
// /// - `max_capacity`: 最大容量
// /// - `disk_threshold_bytes`: 磁盘溢出阈值（字节）
// ///
// /// # 返回
// /// 返回缓存 ID，失败时返回 0
// #[unsafe(no_mangle)]
// pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCache_createCache(
//     env: JNIEnv,
//     _class: JClass,
//     max_capacity: jlong,
//     disk_threshold_bytes: jlong,
// ) -> jlong {
//     // 处理 JNI 异常
//     if let Err(e) = env.exception_check() {
//         eprintln!("JNI exception in createCache: {:?}", e);
//         return 0;
//     }
//
//     // 创建缓存实例
//     let cache = LocalCache::<String, String>::builder()
//         .max_capacity(max_capacity as u64)
//         .disk_offload_threshold(disk_threshold_bytes as usize)
//         .default_lease_duration(Duration::from_secs(300))  // 默认 5 分钟租约
//         .cleanup_interval(Duration::from_secs(30))         // 30 秒清理间隔
//         .auto_renew_on_get(true)                           // 开启自动续租
//         .build();
//
//     // 生成缓存 ID 并存储
//     let cache_id = generate_cache_id();
//     let storage = get_cache_storage();
//
//     match storage.lock() {
//         Ok(mut storage) => {
//             storage.insert(cache_id, cache);
//             cache_id as jlong
//         }
//         Err(e) => {
//             eprintln!("Failed to acquire cache storage lock: {:?}", e);
//             0
//         }
//     }
// }
//
// /// 向缓存添加数据
// ///
// /// # 参数
// /// - `env`: JNI 环境
// /// - `class`: Java 类对象
// /// - `cache_id`: 缓存 ID
// /// - `key`: 键（JSON 字符串）
// /// - `value`: 值（JSON 字符串）
// ///
// /// # 返回
// /// 返回是否成功添加
// #[unsafe(no_mangle)]
// pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCache_put(
//     mut env: JNIEnv,
//     _class: JClass,
//     cache_id: jlong,
//     key: JString,
//     value: JString,
// ) -> jboolean {
//     // 处理 JNI 异常
//     if let Err(e) = env.exception_check() {
//         eprintln!("JNI exception in put: {:?}", e);
//         return 0;
//     }
//
//     // 获取字符串内容
//     let key_str = match env.get_string(&key) {
//         Ok(s) => s.to_string_lossy().to_string(),
//         Err(e) => {
//             eprintln!("Failed to get key string: {:?}", e);
//             return 0;
//         }
//     };
//
//     let value_str = match env.get_string(&value) {
//         Ok(s) => s.to_string_lossy().to_string(),
//         Err(e) => {
//             eprintln!("Failed to get value string: {:?}", e);
//             return 0;
//         }
//     };
//
//     // 获取缓存实例
//     let storage = get_cache_storage();
//     match storage.lock() {
//         Ok(storage) => {
//             if let Some(cache) = storage.get(&(cache_id as u64)) {
//                 cache.insert(key_str, value_str);
//                 1  // 成功
//             } else {
//                 eprintln!("Cache with ID {} not found", cache_id);
//                 0  // 失败
//             }
//         }
//         Err(e) => {
//             eprintln!("Failed to acquire cache storage lock: {:?}", e);
//             0
//         }
//     }
// }
//
// /// 从缓存获取数据
// ///
// /// # 参数
// /// - `env`: JNI 环境
// /// - `class`: Java 类对象
// /// - `cache_id`: 缓存 ID
// /// - `key`: 键（JSON 字符串）
// ///
// /// # 返回
// /// 返回值（JSON 字符串），未找到时返回 null
// #[unsafe(no_mangle)]
// pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCache_get(
//     mut env: JNIEnv,
//     _class: JClass,
//     cache_id: jlong,
//     key: JString,
// ) -> jstring {
//     // 处理 JNI 异常
//     if let Err(e) = env.exception_check() {
//         eprintln!("JNI exception in get: {:?}", e);
//         return std::ptr::null_mut();
//     }
//
//     // 获取键字符串
//     let key_str = match env.get_string(&key) {
//         Ok(s) => s.to_string_lossy().to_string(),
//         Err(e) => {
//             eprintln!("Failed to get key string: {:?}", e);
//             return std::ptr::null_mut();
//         }
//     };
//
//     // 获取缓存实例
//     let storage = get_cache_storage();
//     match storage.lock() {
//         Ok(storage) => {
//             if let Some(cache) = storage.get(&(cache_id as u64)) {
//                 if let Some(value) = cache.get(&key_str) {
//                     // 返回值字符串
//                     match env.new_string(value) {
//                         Ok(jstring) => jstring.into_raw(),
//                         Err(e) => {
//                             eprintln!("Failed to create return string: {:?}", e);
//                             std::ptr::null_mut()
//                         }
//                     }
//                 } else {
//                     std::ptr::null_mut()  // 未找到
//                 }
//             } else {
//                 eprintln!("Cache with ID {} not found", cache_id);
//                 std::ptr::null_mut()
//             }
//         }
//         Err(e) => {
//             eprintln!("Failed to acquire cache storage lock: {:?}", e);
//             std::ptr::null_mut()
//         }
//     }
// }
//
// /// 从缓存删除数据
// ///
// /// # 参数
// /// - `env`: JNI 环境
// /// - `class`: Java 类对象
// /// - `cache_id`: 缓存 ID
// /// - `key`: 键（JSON 字符串）
// ///
// /// # 返回
// /// 返回是否成功删除
// #[unsafe(no_mangle)]
// pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCache_remove(
//     mut env: JNIEnv,
//     _class: JClass,
//     cache_id: jlong,
//     key: JString,
// ) -> jboolean {
//     // 处理 JNI 异常
//     if let Err(e) = env.exception_check() {
//         eprintln!("JNI exception in remove: {:?}", e);
//         return 0;
//     }
//
//     // 获取键字符串
//     let key_str = match env.get_string(&key) {
//         Ok(s) => s.to_string_lossy().to_string(),
//         Err(e) => {
//             eprintln!("Failed to get key string: {:?}", e);
//             return 0;
//         }
//     };
//
//     // 获取缓存实例
//     let storage = get_cache_storage();
//     match storage.lock() {
//         Ok(storage) => {
//             if let Some(cache) = storage.get(&(cache_id as u64)) {
//                 cache.remove(&key_str);
//                 1  // 成功
//             } else {
//                 eprintln!("Cache with ID {} not found", cache_id);
//                 0  // 失败
//             }
//         }
//         Err(e) => {
//             eprintln!("Failed to acquire cache storage lock: {:?}", e);
//             0
//         }
//     }
// }
//
// /// 清空缓存
// ///
// /// # 参数
// /// - `env`: JNI 环境
// /// - `class`: Java 类对象
// /// - `cache_id`: 缓存 ID
// ///
// /// # 返回
// /// 返回是否成功清空
// #[unsafe(no_mangle)]
// pub extern "system" fn Java_FluxCache_clear(
//     env: JNIEnv,
//     _class: JClass,
//     cache_id: jlong,
// ) -> jboolean {
//     // 处理 JNI 异常
//     if let Err(e) = env.exception_check() {
//         eprintln!("JNI exception in clear: {:?}", e);
//         return 0;
//     }
//
//     // 获取缓存实例
//     let storage = get_cache_storage();
//     match storage.lock() {
//         Ok(storage) => {
//             if let Some(cache) = storage.get(&(cache_id as u64)) {
//                 cache.clear();
//                 1  // 成功
//             } else {
//                 eprintln!("Cache with ID {} not found", cache_id);
//                 0  // 失败
//             }
//         }
//         Err(e) => {
//             eprintln!("Failed to acquire cache storage lock: {:?}", e);
//             0
//         }
//     }
// }
//
// /// 获取缓存状态信息
// ///
// /// # 参数
// /// - `env`: JNI 环境
// /// - `class`: Java 类对象
// /// - `cache_id`: 缓存 ID
// ///
// /// # 返回
// /// 返回 JSON 格式的状态信息
// #[unsafe(no_mangle)]
// pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCache_getMetrics(
//     env: JNIEnv,
//     _class: JClass,
//     cache_id: jlong,
// ) -> jstring {
//     // 处理 JNI 异常
//     if let Err(e) = env.exception_check() {
//         eprintln!("JNI exception in getMetrics: {:?}", e);
//         return std::ptr::null_mut();
//     }
//
//     // 获取缓存实例
//     let storage = get_cache_storage();
//     match storage.lock() {
//         Ok(storage) => {
//             if let Some(cache) = storage.get(&(cache_id as u64)) {
//                 let metrics = cache.metrics();
//
//                 // 创建 JSON 状态信息
//                 let status = json!({
//                     "cache_id": cache_id,
//                     "entries": metrics.entries,
//                     "max_capacity": metrics.max_capacity,
//                     "time_to_live_secs": metrics.time_to_live_secs,
//                     "time_to_idle_secs": metrics.time_to_idle_secs,
//                     "disk_entries": metrics.disk_entries,
//                     "disk_bytes": metrics.disk_bytes,
//                     "leased_entries": metrics.leased_entries,
//                     "expired_entries": metrics.expired_entries
//                 });
//
//                 // 返回 JSON 字符串
//                 match env.new_string(status.to_string()) {
//                     Ok(jstring) => jstring.into_raw(),
//                     Err(e) => {
//                         eprintln!("Failed to create metrics string: {:?}", e);
//                         std::ptr::null_mut()
//                     }
//                 }
//             } else {
//                 eprintln!("Cache with ID {} not found", cache_id);
//                 std::ptr::null_mut()
//             }
//         }
//         Err(e) => {
//             eprintln!("Failed to acquire cache storage lock: {:?}", e);
//             std::ptr::null_mut()
//         }
//     }
// }
//
// /// 销毁缓存实例
// ///
// /// # 参数
// /// - `env`: JNI 环境
// /// - `class`: Java 类对象
// /// - `cache_id`: 缓存 ID
// ///
// /// # 返回
// /// 返回是否成功销毁
// #[unsafe(no_mangle)]
// pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCache_destroyCache(
//     env: JNIEnv,
//     _class: JClass,
//     cache_id: jlong,
// ) -> jboolean {
//     // 处理 JNI 异常
//     if let Err(e) = env.exception_check() {
//         eprintln!("JNI exception in destroyCache: {:?}", e);
//         return 0;
//     }
//
//     // 获取缓存存储并移除实例
//     let storage = get_cache_storage();
//     match storage.lock() {
//         Ok(mut storage) => {
//             if storage.remove(&(cache_id as u64)).is_some() {
//                 1  // 成功
//             } else {
//                 eprintln!("Cache with ID {} not found", cache_id);
//                 0  // 失败
//             }
//         }
//         Err(e) => {
//             eprintln!("Failed to acquire cache storage lock: {:?}", e);
//             0
//         }
//     }
// }
//
// /// 续租指定键
// ///
// /// # 参数
// /// - `env`: JNI 环境
// /// - `class`: Java 类对象
// /// - `cache_id`: 缓存 ID
// /// - `key`: 键（JSON 字符串）
// /// - `additional_duration_secs`: 续租时长（秒）
// ///
// /// # 返回
// /// 返回是否成功续租
// #[unsafe(no_mangle)]
// pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCache_renewLease(
//     mut env: JNIEnv,
//     _class: JClass,
//     cache_id: jlong,
//     key: JString,
//     additional_duration_secs: jlong,
// ) -> jboolean {
//     // 处理 JNI 异常
//     if let Err(e) = env.exception_check() {
//         eprintln!("JNI exception in renewLease: {:?}", e);
//         return 0;
//     }
//
//     // 获取键字符串
//     let key_str = match env.get_string(&key) {
//         Ok(s) => s.to_string_lossy().to_string(),
//         Err(e) => {
//             eprintln!("Failed to get key string: {:?}", e);
//             return 0;
//         }
//     };
//
//     // 获取缓存实例
//     let storage = get_cache_storage();
//     match storage.lock() {
//         Ok(storage) => {
//             if let Some(cache) = storage.get(&(cache_id as u64)) {
//                 let duration = Duration::from_secs(additional_duration_secs as u64);
//                 if cache.renew_lease(&key_str, duration) {
//                     1  // 成功
//                 } else {
//                     0  // 失败
//                 }
//             } else {
//                 eprintln!("Cache with ID {} not found", cache_id);
//                 0
//             }
//         }
//         Err(e) => {
//             eprintln!("Failed to acquire cache storage lock: {:?}", e);
//             0
//         }
//     }
// }
//
// /// 强制删除指定键（忽略租约）
// ///
// /// # 参数
// /// - `env`: JNI 环境
// /// - `class`: Java 类对象
// /// - `cache_id`: 缓存 ID
// /// - `key`: 键（JSON 字符串）
// ///
// /// # 返回
// /// 返回是否成功强制删除
// #[unsafe(no_mangle)]
// pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCache_forceRemove(
//     mut env: JNIEnv,
//     _class: JClass,
//     cache_id: jlong,
//     key: JString,
// ) -> jboolean {
//     // 处理 JNI 异常
//     if let Err(e) = env.exception_check() {
//         eprintln!("JNI exception in forceRemove: {:?}", e);
//         return 0;
//     }
//
//     // 获取键字符串
//     let key_str = match env.get_string(&key) {
//         Ok(s) => s.to_string_lossy().to_string(),
//         Err(e) => {
//             eprintln!("Failed to get key string: {:?}", e);
//             return 0;
//         }
//     };
//
//     // 获取缓存实例
//     let storage = get_cache_storage();
//     match storage.lock() {
//         Ok(storage) => {
//             if let Some(cache) = storage.get(&(cache_id as u64)) {
//                 cache.force_remove(&key_str);
//                 1  // 成功
//             } else {
//                 eprintln!("Cache with ID {} not found", cache_id);
//                 0
//             }
//         }
//         Err(e) => {
//             eprintln!("Failed to acquire cache storage lock: {:?}", e);
//             0
//         }
//     }
// }
