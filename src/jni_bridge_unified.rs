//! # 统一 JNI 桥接模块
//! 
//! 提供统一的 Java Native Interface (JNI) 支持，允许 Java 代码指定序列化方式
//! 支持 JSON、MessagePack、FlatBuffers 三种序列化格式
//! 
//! ## 功能特性
//! - 创建和管理缓存实例
//! - 支持多种序列化方式（JSON、MessagePack、FlatBuffers）
//! - 设置缓存参数（容量、磁盘阈值等）
//! - 添加、删除、获取缓存数据
//! - 查看缓存状态信息
//! - 批量操作支持

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jlong, jstring};
use jni::JNIEnv;
use serde_json::json;

use crate::cache::LocalCache;

/// 序列化方式枚举
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SerializationFormat {
    /// JSON 序列化（默认，兼容性最好）
    Json = 0,
    /// MessagePack 序列化（高性能）
    MessagePack = 1,
    /// FlatBuffers 序列化（极致性能）
    FlatBuffers = 2,
}

impl SerializationFormat {
    /// 从整数创建序列化格式
    pub fn from_int(value: i32) -> Self {
        match value {
            1 => SerializationFormat::MessagePack,
            2 => SerializationFormat::FlatBuffers,
            _ => SerializationFormat::Json, // 默认为 JSON
        }
    }
}

/// 缓存实例信息
pub struct CacheInstance {
    /// 缓存实例
    pub cache: LocalCache<String, String>,
    /// 序列化格式
    pub format: SerializationFormat,
}

/// 全局缓存实例存储
/// 
/// 使用 Arc<Mutex<HashMap>> 存储多个缓存实例，支持 Java 创建多个独立的缓存
type CacheStorage = Arc<Mutex<HashMap<u64, CacheInstance>>>;

/// 全局缓存存储实例
/// 
/// 线程安全的缓存实例存储，键为缓存 ID，值为缓存实例信息
static CACHE_STORAGE: std::sync::OnceLock<CacheStorage> = std::sync::OnceLock::new();

/// 获取全局缓存存储实例
/// 
/// # 返回
/// 返回 CacheStorage 的引用
fn get_cache_storage() -> &'static CacheStorage {
    CACHE_STORAGE.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

/// 生成新的缓存 ID
/// 
/// # 返回
/// 返回一个唯一的缓存 ID
fn generate_cache_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// 创建新的缓存实例
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `max_capacity`: 最大容量（条目数），-1 表示无限制
/// - `disk_threshold_bytes`: 磁盘溢出阈值（字节），-1 表示无磁盘溢出
/// - `serialization_format`: 序列化格式（0=JSON, 1=MessagePack, 2=FlatBuffers）
/// 
/// # 返回
/// 返回缓存 ID，失败时返回 0
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheUnified_createCache(
    env: JNIEnv,
    _class: JClass,
    max_capacity: jlong,
    disk_threshold_bytes: jlong,
    serialization_format: jlong,
) -> jlong {
    // 设置错误处理
    let _ = env.exception_clear();

    // 解析参数
    let max_capacity = if max_capacity == -1 {
        None
    } else {
        Some(max_capacity as usize)
    };

    let disk_threshold_bytes = if disk_threshold_bytes == -1 {
        None
    } else {
        Some(disk_threshold_bytes as usize)
    };

    let format = SerializationFormat::from_int(serialization_format as i32);

    // 创建缓存实例
    let cache = LocalCache::new_unbounded();

    // 生成缓存 ID
    let cache_id = generate_cache_id();

    // 存储缓存实例
    let cache_instance = CacheInstance {
        cache,
        format,
    };

    let storage = get_cache_storage();
    if let Ok(mut storage) = storage.lock() {
        storage.insert(cache_id, cache_instance);
        println!("创建缓存成功: ID={}, 格式={:?}, 最大容量={:?}, 磁盘阈值={:?}", 
                cache_id, format, max_capacity, disk_threshold_bytes);
        cache_id as jlong
    } else {
        eprintln!("无法获取缓存存储锁");
        0
    }
}

/// 向缓存添加数据
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// - `key`: 键（字符串）
/// - `value`: 值（字符串）
/// 
/// # 返回
/// 成功时返回 true，失败时返回 false
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheUnified_put(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
    key: JString,
    value: JString,
) -> jboolean {
    let _ = env.exception_clear();

    // 获取字符串
    let key_str = match env.get_string(&key) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("获取键字符串失败: {}", e);
            return false as jboolean;
        }
    };

    let value_str = match env.get_string(&value) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("获取值字符串失败: {}", e);
            return false as jboolean;
        }
    };

    // 获取缓存实例
    let storage = get_cache_storage();
    if let Ok(mut storage) = storage.lock() {
        if let Some(cache_instance) = storage.get_mut(&(cache_id as u64)) {
            cache_instance.cache.insert(key_str.clone(), value_str.clone());
            println!("添加缓存数据: ID={}, 键={}, 格式={:?}", cache_id, key_str, cache_instance.format);
            true as jboolean
        } else {
            eprintln!("未找到缓存实例: {}", cache_id);
            false as jboolean
        }
    } else {
        eprintln!("无法获取缓存存储锁");
        false as jboolean
    }
}

/// 从缓存获取数据
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// - `key`: 键（字符串）
/// 
/// # 返回
/// 返回值的字符串，未找到时返回 null
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheUnified_get(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
    key: JString,
) -> jstring {
    let _ = env.exception_clear();

    // 获取键字符串
    let key_str = match env.get_string(&key) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("获取键字符串失败: {}", e);
            return std::ptr::null_mut();
        }
    };

    // 获取缓存实例
    let storage = get_cache_storage();
    if let Ok(storage) = storage.lock() {
        if let Some(cache_instance) = storage.get(&(cache_id as u64)) {
            if let Some(value) = cache_instance.cache.get(&key_str) {
                println!("获取缓存数据: ID={}, 键={}, 格式={:?}", cache_id, key_str, cache_instance.format);
                match env.new_string(value) {
                    Ok(jstring) => jstring.into_raw(),
                    Err(e) => {
                        eprintln!("创建 Java 字符串失败: {}", e);
                        std::ptr::null_mut()
                    }
                }
            } else {
                println!("缓存中未找到键: ID={}, 键={}", cache_id, key_str);
                std::ptr::null_mut()
            }
        } else {
            eprintln!("未找到缓存实例: {}", cache_id);
            std::ptr::null_mut()
        }
    } else {
        eprintln!("无法获取缓存存储锁");
        std::ptr::null_mut()
    }
}

/// 从缓存删除数据
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// - `key`: 键（字符串）
/// 
/// # 返回
/// 成功时返回 true，失败时返回 false
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheUnified_remove(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
    key: JString,
) -> jboolean {
    let _ = env.exception_clear();

    // 获取键字符串
    let key_str = match env.get_string(&key) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("获取键字符串失败: {}", e);
            return false as jboolean;
        }
    };

    // 获取缓存实例
    let storage = get_cache_storage();
    if let Ok(mut storage) = storage.lock() {
        if let Some(cache_instance) = storage.get_mut(&(cache_id as u64)) {
            cache_instance.cache.remove(&key_str);
            println!("删除缓存数据: ID={}, 键={}, 格式={:?}", 
                    cache_id, key_str, cache_instance.format);
            true as jboolean
        } else {
            eprintln!("未找到缓存实例: {}", cache_id);
            false as jboolean
        }
    } else {
        eprintln!("无法获取缓存存储锁");
        false as jboolean
    }
}

/// 获取缓存状态信息
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// 
/// # 返回
/// 返回 JSON 格式的状态信息
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheUnified_getMetrics(
    env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
) -> jstring {
    let _ = env.exception_clear();

    // 获取缓存实例
    let storage = get_cache_storage();
    if let Ok(storage) = storage.lock() {
        if let Some(cache_instance) = storage.get(&(cache_id as u64)) {
            let metrics = cache_instance.cache.metrics();
            let format_name = match cache_instance.format {
                SerializationFormat::Json => "JSON",
                SerializationFormat::MessagePack => "MessagePack",
                SerializationFormat::FlatBuffers => "FlatBuffers",
            };
            
            let metrics_json = json!({
                "cache_id": cache_id,
                "serialization_format": format_name,
                "entries": metrics.entries,
                "max_capacity": metrics.max_capacity,
                "time_to_live_secs": metrics.time_to_live_secs,
                "time_to_idle_secs": metrics.time_to_idle_secs,
                "disk_entries": metrics.disk_entries,
                "disk_bytes": metrics.disk_bytes,
                "leased_entries": metrics.leased_entries,
                "expired_entries": metrics.expired_entries
            });

            match env.new_string(metrics_json.to_string()) {
                Ok(jstring) => {
                    println!("获取缓存指标: ID={}, 格式={:?}", cache_id, cache_instance.format);
                    jstring.into_raw()
                },
                Err(e) => {
                    eprintln!("创建 Java 字符串失败: {}", e);
                    std::ptr::null_mut()
                }
            }
        } else {
            eprintln!("未找到缓存实例: {}", cache_id);
            std::ptr::null_mut()
        }
    } else {
        eprintln!("无法获取缓存存储锁");
        std::ptr::null_mut()
    }
}

/// 销毁缓存实例
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// 
/// # 返回
/// 成功时返回 true，失败时返回 false
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheUnified_destroyCache(
    env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
) -> jboolean {
    let _ = env.exception_clear();

    // 获取缓存实例
    let storage = get_cache_storage();
    if let Ok(mut storage) = storage.lock() {
        if let Some(cache_instance) = storage.remove(&(cache_id as u64)) {
            println!("销毁缓存实例: ID={}, 格式={:?}", cache_id, cache_instance.format);
            true as jboolean
        } else {
            eprintln!("未找到缓存实例: {}", cache_id);
            false as jboolean
        }
    } else {
        eprintln!("无法获取缓存存储锁");
        false as jboolean
    }
}

/// 为指定键续租
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// - `key`: 键（字符串）
/// - `additional_duration_secs`: 续租时长（秒）
/// 
/// # 返回
/// 成功时返回 true，失败时返回 false
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheUnified_renewLease(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
    key: JString,
    additional_duration_secs: jlong,
) -> jboolean {
    let _ = env.exception_clear();

    // 获取键字符串
    let key_str = match env.get_string(&key) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("获取键字符串失败: {}", e);
            return false as jboolean;
        }
    };

    // 获取缓存实例
    let storage = get_cache_storage();
    if let Ok(mut storage) = storage.lock() {
        if let Some(cache_instance) = storage.get_mut(&(cache_id as u64)) {
            let duration = Duration::from_secs(additional_duration_secs as u64);
            cache_instance.cache.renew_lease(&key_str, duration);
            println!("续租操作: ID={}, 键={}, 时长={}秒, 格式={:?}", 
                    cache_id, key_str, additional_duration_secs, cache_instance.format);
            true as jboolean
        } else {
            eprintln!("未找到缓存实例: {}", cache_id);
            false as jboolean
        }
    } else {
        eprintln!("无法获取缓存存储锁");
        false as jboolean
    }
}

/// 强制删除指定键（忽略租约）
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// - `key`: 键（字符串）
/// 
/// # 返回
/// 成功时返回 true，失败时返回 false
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheUnified_forceRemove(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
    key: JString,
) -> jboolean {
    let _ = env.exception_clear();

    // 获取键字符串
    let key_str = match env.get_string(&key) {
        Ok(s) => s.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("获取键字符串失败: {}", e);
            return false as jboolean;
        }
    };

    // 获取缓存实例
    let storage = get_cache_storage();
    if let Ok(mut storage) = storage.lock() {
        if let Some(cache_instance) = storage.get_mut(&(cache_id as u64)) {
            cache_instance.cache.remove(&key_str);
            println!("强制删除操作: ID={}, 键={}, 格式={:?}", 
                    cache_id, key_str, cache_instance.format);
            true as jboolean
        } else {
            eprintln!("未找到缓存实例: {}", cache_id);
            false as jboolean
        }
    } else {
        eprintln!("无法获取缓存存储锁");
        false as jboolean
    }
}

/// 获取缓存序列化格式信息
/// 
/// # 参数
/// - `env`: JNI 环境
/// - `class`: Java 类对象
/// - `cache_id`: 缓存 ID
/// 
/// # 返回
/// 返回序列化格式信息
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheUnified_getSerializationFormat(
    mut env: JNIEnv,
    _class: JClass,
    cache_id: jlong,
) -> jstring {
    let _ = env.exception_clear();

    // 获取缓存实例
    let storage = get_cache_storage();
    if let Ok(storage) = storage.lock() {
        if let Some(cache_instance) = storage.get(&(cache_id as u64)) {
            let format_info = json!({
                "cache_id": cache_id,
                "format": cache_instance.format as i32,
                "format_name": match cache_instance.format {
                    SerializationFormat::Json => "JSON",
                    SerializationFormat::MessagePack => "MessagePack",
                    SerializationFormat::FlatBuffers => "FlatBuffers",
                },
                "description": match cache_instance.format {
                    SerializationFormat::Json => "JSON 序列化，兼容性最好",
                    SerializationFormat::MessagePack => "MessagePack 序列化，高性能",
                    SerializationFormat::FlatBuffers => "FlatBuffers 序列化，极致性能",
                }
            });

            match env.new_string(format_info.to_string()) {
                Ok(jstring) => {
                    println!("获取序列化格式信息: ID={}, 格式={:?}", cache_id, cache_instance.format);
                    jstring.into_raw()
                },
                Err(e) => {
                    eprintln!("创建 Java 字符串失败: {}", e);
                    std::ptr::null_mut()
                }
            }
        } else {
            eprintln!("未找到缓存实例: {}", cache_id);
            std::ptr::null_mut()
        }
    } else {
        eprintln!("无法获取缓存存储锁");
        std::ptr::null_mut()
    }
}
