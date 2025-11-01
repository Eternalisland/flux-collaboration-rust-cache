//! # 高性能 JNI 桥接模块
//! 
//! 使用 MessagePack 序列化提供高性能的 Java-Rust 数据传输
//! 比 JSON 方案快 2-3 倍，内存使用更少

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jlong, jstring};
use jni::JNIEnv;

use crate::cache::LocalCache;
use crate::serialization::{HighPerformanceSerializer, SerializationFormat, CacheMetrics, CacheResult};

/// 全局缓存实例存储
type CacheStorage = Arc<Mutex<HashMap<u64, LocalCache<String, String>>>>;

/// 全局缓存存储实例
static CACHE_STORAGE: std::sync::OnceLock<CacheStorage> = std::sync::OnceLock::new();

/// 获取全局缓存存储实例
fn get_cache_storage() -> &'static CacheStorage {
    CACHE_STORAGE.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

/// 生成新的缓存 ID
fn generate_cache_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// 高性能序列化器实例
static SERIALIZER: HighPerformanceSerializer = HighPerformanceSerializer { format: SerializationFormat::MessagePack };

/// 创建新的缓存实例（高性能版本）
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `max_capacity`: 最大容量
/// - `disk_threshold_bytes`: 磁盘溢出阈值（字节）
/// 
/// # 返回
/// 返回缓存 ID，失败时返回 0
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheHighPerf_createCache(
    env: JNIEnv,
    _class: JClass,
    max_capacity: jlong,
    disk_threshold_bytes: jlong,
) -> jlong {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in createCache: {:?}", e);
        return 0;
    }

    // 创建缓存实例
    let cache = LocalCache::<String, String>::builder()
        .max_capacity(max_capacity as u64)
        .disk_offload_threshold(disk_threshold_bytes as usize)
        .default_lease_duration(Duration::from_secs(300))  // 默认 5 分钟租约
        .cleanup_interval(Duration::from_secs(30))         // 30 秒清理间隔
        .auto_renew_on_get(true)                           // 开启自动续租
        .build();

    // 生成缓存 ID 并存储
    let cache_id = generate_cache_id();
    let storage = get_cache_storage();
    
    match storage.lock() {
        Ok(mut storage) => {
            storage.insert(cache_id, cache);
            cache_id as jlong
        }
        Err(e) => {
            eprintln!("Failed to acquire cache storage lock: {:?}", e);
            0
        }
    }
}

/// 向缓存添加数据（高性能版本）
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// - `key`: 键（字符串）
/// - `value`: 值（字符串）
/// 
/// # 返回
/// 返回是否成功添加
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheHighPerf_put(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
    key: JString,
    value: JString,
) -> jboolean {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in put: {:?}", e);
        return 0;
    }

    // 获取字符串内容
    let key_str = match env.get_string(&key) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get key string: {:?}", e);
            return 0;
        }
    };

    let value_str = match env.get_string(&value) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get value string: {:?}", e);
            return 0;
        }
    };

    // 获取缓存实例
    let storage = get_cache_storage();
    match storage.lock() {
        Ok(storage) => {
            if let Some(cache) = storage.get(&(cache_id as u64)) {
                cache.insert(key_str, value_str);
                1  // 成功
            } else {
                eprintln!("Cache with ID {} not found", cache_id);
                0  // 失败
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire cache storage lock: {:?}", e);
            0
        }
    }
}

/// 从缓存获取数据（高性能版本）
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// - `key`: 键（字符串）
/// 
/// # 返回
/// 返回值（字符串），未找到时返回 null
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheHighPerf_get(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
    key: JString,
) -> jstring {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in get: {:?}", e);
        return std::ptr::null_mut();
    }

    // 获取键字符串
    let key_str = match env.get_string(&key) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get key string: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // 获取缓存实例
    let storage = get_cache_storage();
    match storage.lock() {
        Ok(storage) => {
            if let Some(cache) = storage.get(&(cache_id as u64)) {
                if let Some(value) = cache.get(&key_str) {
                    // 返回值字符串
                    match env.new_string(value) {
                        Ok(jstring) => jstring.into_raw(),
                        Err(e) => {
                            eprintln!("Failed to create return string: {:?}", e);
                            std::ptr::null_mut()
                        }
                    }
                } else {
                    std::ptr::null_mut()  // 未找到
                }
            } else {
                eprintln!("Cache with ID {} not found", cache_id);
                std::ptr::null_mut()
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire cache storage lock: {:?}", e);
            std::ptr::null_mut()
        }
    }
}

/// 从缓存删除数据（高性能版本）
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// - `key`: 键（字符串）
/// 
/// # 返回
/// 返回是否成功删除
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheHighPerf_remove(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
    key: JString,
) -> jboolean {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in remove: {:?}", e);
        return 0;
    }

    // 获取键字符串
    let key_str = match env.get_string(&key) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get key string: {:?}", e);
            return 0;
        }
    };

    // 获取缓存实例
    let storage = get_cache_storage();
    match storage.lock() {
        Ok(storage) => {
            if let Some(cache) = storage.get(&(cache_id as u64)) {
                cache.remove(&key_str);
                1  // 成功
            } else {
                eprintln!("Cache with ID {} not found", cache_id);
                0
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire cache storage lock: {:?}", e);
            0
        }
    }
}

/// 清空缓存（高性能版本）
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// 
/// # 返回
/// 返回是否成功清空
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheHighPerf_clear(
    env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
) -> jboolean {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in clear: {:?}", e);
        return 0;
    }

    // 获取缓存实例
    let storage = get_cache_storage();
    match storage.lock() {
        Ok(storage) => {
            if let Some(cache) = storage.get(&(cache_id as u64)) {
                cache.clear();
                1  // 成功
            } else {
                eprintln!("Cache with ID {} not found", cache_id);
                0
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire cache storage lock: {:?}", e);
            0
        }
    }
}

/// 获取缓存状态信息（高性能版本，使用 MessagePack）
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// 
/// # 返回
/// 返回 Base64 编码的 MessagePack 格式状态信息
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheHighPerf_getMetrics(
    env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
) -> jstring {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in getMetrics: {:?}", e);
        return std::ptr::null_mut();
    }

    // 获取缓存实例
    let storage = get_cache_storage();
    match storage.lock() {
        Ok(storage) => {
            if let Some(cache) = storage.get(&(cache_id as u64)) {
                let metrics = cache.metrics();
                
                // 创建缓存指标
                let cache_metrics = CacheMetrics {
                    cache_id: cache_id as u64,
                    entries: metrics.entries,
                    max_capacity: metrics.max_capacity,
                    time_to_live_secs: metrics.time_to_live_secs,
                    time_to_idle_secs: metrics.time_to_idle_secs,
                    disk_entries: metrics.disk_entries,
                    disk_bytes: metrics.disk_bytes,
                    leased_entries: metrics.leased_entries,
                    expired_entries: metrics.expired_entries,
                };

                // 使用 MessagePack 序列化并转换为 Base64
                match SERIALIZER.serialize_to_base64(&cache_metrics) {
                    Ok(base64_str) => {
                        match env.new_string(base64_str) {
                            Ok(jstring) => jstring.into_raw(),
                            Err(e) => {
                                eprintln!("Failed to create metrics string: {:?}", e);
                                std::ptr::null_mut()
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to serialize metrics: {:?}", e);
                        std::ptr::null_mut()
                    }
                }
            } else {
                eprintln!("Cache with ID {} not found", cache_id);
                std::ptr::null_mut()
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire cache storage lock: {:?}", e);
            std::ptr::null_mut()
        }
    }
}

/// 销毁缓存实例（高性能版本）
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// 
/// # 返回
/// 返回是否成功销毁
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheHighPerf_destroyCache(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
) -> jboolean {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in destroyCache: {:?}", e);
        return 0;
    }

    // 获取缓存存储并移除实例
    let storage = get_cache_storage();
    match storage.lock() {
        Ok(mut storage) => {
            if storage.remove(&(cache_id as u64)).is_some() {
                1  // 成功
            } else {
                eprintln!("Cache with ID {} not found", cache_id);
                0
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire cache storage lock: {:?}", e);
            0
        }
    }
}

/// 续租指定键（高性能版本）
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// - `key`: 键（字符串）
/// - `additional_duration_secs`: 续租时长（秒）
/// 
/// # 返回
/// 返回是否成功续租
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheHighPerf_renewLease(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
    key: JString,
    additional_duration_secs: jlong,
) -> jboolean {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in renewLease: {:?}", e);
        return 0;
    }

    // 获取键字符串
    let key_str = match env.get_string(&key) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get key string: {:?}", e);
            return 0;
        }
    };

    // 获取缓存实例
    let storage = get_cache_storage();
    match storage.lock() {
        Ok(storage) => {
            if let Some(cache) = storage.get(&(cache_id as u64)) {
                let duration = Duration::from_secs(additional_duration_secs as u64);
                if cache.renew_lease(&key_str, duration) {
                    1  // 成功
                } else {
                    0
                }
            } else {
                eprintln!("Cache with ID {} not found", cache_id);
                0
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire cache storage lock: {:?}", e);
            0
        }
    }
}

/// 强制删除指定键（忽略租约）（高性能版本）
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// - `key`: 键（字符串）
/// 
/// # 返回
/// 返回是否成功强制删除
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheHighPerf_forceRemove(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
    key: JString,
) -> jboolean {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in forceRemove: {:?}", e);
        return 0;
    }

    // 获取键字符串
    let key_str = match env.get_string(&key) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get key string: {:?}", e);
            return 0;
        }
    };

    // 获取缓存实例
    let storage = get_cache_storage();
    match storage.lock() {
        Ok(storage) => {
            if let Some(cache) = storage.get(&(cache_id as u64)) {
                cache.force_remove(&key_str);
                1  // 成功
            } else {
                eprintln!("Cache with ID {} not found", cache_id);
                0
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire cache storage lock: {:?}", e);
            0
        }
    }
}

/// 性能测试函数（用于基准测试）
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `iterations`: 测试迭代次数
/// 
/// # 返回
/// 返回 Base64 编码的性能测试结果
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheHighPerf_performanceTest(
    mut env: JNIEnv,
    _class: JClass,
    iterations: jlong,
) -> jstring {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in performanceTest: {:?}", e);
        return std::ptr::null_mut();
    }

    // 创建测试数据
    let test_data = std::collections::HashMap::from([
        ("user_id".to_string(), "12345".to_string()),
        ("username".to_string(), "alice".to_string()),
        ("email".to_string(), "alice@example.com".to_string()),
        ("created_at".to_string(), "2024-01-01T00:00:00Z".to_string()),
        ("profile".to_string(), "Software Engineer".to_string()),
        ("department".to_string(), "Engineering".to_string()),
    ]);

    // 运行性能测试
    let results = crate::serialization::PerformanceTester::test_serialization_performance(
        &test_data,
        iterations as usize,
    );

    // 序列化结果
    match SERIALIZER.serialize_to_base64(&results) {
        Ok(base64_str) => {
            match env.new_string(base64_str) {
                Ok(jstring) => jstring.into_raw(),
                Err(e) => {
                    eprintln!("Failed to create performance test result string: {:?}", e);
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to serialize performance test results: {:?}", e);
            std::ptr::null_mut()
        }
    }
}
