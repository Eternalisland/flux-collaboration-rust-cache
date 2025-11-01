//! # 大数据场景 JNI 桥接模块
//! 
//! 专门为大文件（>1MB）缓存场景设计的高性能桥接
//! - 免序列化直接操作字节数组
//! - 内存映射优化大文件读写

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;

use jni::objects::{JClass, JString, JByteArray};
use jni::sys::{jlong, jbyteArray, jboolean, jstring, jdouble};
use jni::JNIEnv;

use crate::big_data_storage::{BigDataStorage, BigDataStorageConfig};
use crate::optimized_cache::{OptimizedCache, OptimizedCacheConfig};

/// 全局大数据存储实例
type BigDataStorageInstance = Arc<Mutex<BigDataStorage>>;

/// 全局优化缓存实例
type OptimizedCacheInstance = Arc<Mutex<OptimizedCache>>;

/// 全局存储实例存储
static STORAGE_INSTANCES: std::sync::OnceLock<Arc<Mutex<HashMap<u64, BigDataStorageInstance>>>> = std::sync::OnceLock::new();

/// 全局优化缓存实例存储
static CACHE_INSTANCES: std::sync::OnceLock<Arc<Mutex<HashMap<u64, OptimizedCacheInstance>>>> = std::sync::OnceLock::new();

/// 获取全局存储实例存储
fn get_storage_instances() -> &'static Arc<Mutex<HashMap<u64, BigDataStorageInstance>>> {
    STORAGE_INSTANCES.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

/// 获取全局优化缓存实例存储
fn get_cache_instances() -> &'static Arc<Mutex<HashMap<u64, OptimizedCacheInstance>>> {
    CACHE_INSTANCES.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

/// 生成新的存储实例 ID
fn generate_storage_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// 创建新的大数据存储实例（完整配置版本）
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheBigData_createStorageFull(
    mut env: JNIEnv,
    _class: JClass,
    disk_dir: JString,
    memory_threshold: jlong,
    prefetch_block_size: jlong,
    disk_cache_dir: JString,
    enable_compression: jboolean,
    cleanup_interval_secs: jlong,
) -> jlong {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in createStorage: {:?}", e);
        return 0;
    }

    // 获取磁盘目录字符串
    let disk_dir_str = match env.get_string(&disk_dir) {
        Ok(jstr) => jstr.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get disk_dir string: {:?}", e);
            return 0;
        }
    };

    // 获取磁盘缓存目录字符串
    let disk_cache_dir_str = match env.get_string(&disk_cache_dir) {
        Ok(jstr) => jstr.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get disk_cache_dir string: {:?}", e);
            return 0;
        }
    };

    // 创建大数据存储实例（使用所有传入的配置参数）
    let storage_config = BigDataStorageConfig {
        disk_dir: PathBuf::from(&disk_dir_str),
        memory_threshold: memory_threshold as usize,
        prefetch_block_size: prefetch_block_size as usize,
        disk_cache_dir: PathBuf::from(&disk_cache_dir_str),
        enable_compression: enable_compression != 0,
        cleanup_interval: std::time::Duration::from_secs(cleanup_interval_secs as u64),
    };

    let storage = match BigDataStorage::new(storage_config) {
        Ok(storage) => storage,
        Err(e) => {
            eprintln!("Failed to create big data storage: {:?}", e);
            return 0;
        }
    };

    // 生成存储实例 ID 并存储
    let storage_id = generate_storage_id();
    let storage_instance = Arc::new(Mutex::new(storage));
    let instances = get_storage_instances();
    
    match instances.lock() {
        Ok(mut instances) => {
            instances.insert(storage_id, storage_instance);
            storage_id as jlong
        }
        Err(e) => {
            eprintln!("Failed to acquire storage instances lock: {:?}", e);
            0
        }
    }
}

/// 存储大数据
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheBigData_store(
    mut env: JNIEnv,
    _class: JClass,
    storage_id: jlong,
    key: JString,
    data: JByteArray,
) -> jboolean {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in store: {:?}", e);
        return 0;
    }

    // 获取存储实例
    let instances = get_storage_instances();
    let storage_instance = match instances.lock() {
        Ok(instances) => {
            match instances.get(&(storage_id as u64)) {
                Some(instance) => instance.clone(),
                None => {
                    eprintln!("Storage instance {} not found", storage_id);
                    return 0;
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire storage instances lock: {:?}", e);
            return 0;
        }
    };

    // 获取键字符串
    let key_str = match env.get_string(&key) {
        Ok(jstr) => jstr.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get key string: {:?}", e);
            return 0;
        }
    };

    // 获取字节数组
    let byte_array = match env.convert_byte_array(&data) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Failed to convert byte array: {:?}", e);
            return 0;
        }
    };

    // 存储数据
    match storage_instance.lock() {
        Ok(storage) => {
            match storage.store(&key_str, &byte_array) {
                Ok(_) => 1, // 成功
                Err(e) => {
                    eprintln!("Failed to store data: {:?}", e);
                    0
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire storage lock: {:?}", e);
            0
        }
    }
}

/// 加载大数据
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheBigData_load(
    mut env: JNIEnv,
    _class: JClass,
    storage_id: jlong,
    key: JString,
) -> jbyteArray {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in load: {:?}", e);
        return std::ptr::null_mut();
    }

    // 获取存储实例
    let instances = get_storage_instances();
    let storage_instance = match instances.lock() {
        Ok(instances) => {
            match instances.get(&(storage_id as u64)) {
                Some(instance) => instance.clone(),
                None => {
                    eprintln!("Storage instance {} not found", storage_id);
                    return std::ptr::null_mut();
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire storage instances lock: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // 获取键字符串
    let key_str = match env.get_string(&key) {
        Ok(jstr) => jstr.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get key string: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // 加载数据
    let data = match storage_instance.lock() {
        Ok(storage) => {
            match storage.load(&key_str) {
                Ok(Some(data)) => data,
                Ok(None) => {
                    // 数据不存在
                    return std::ptr::null_mut();
                }
                Err(e) => {
                    eprintln!("Failed to load data: {:?}", e);
                    return std::ptr::null_mut();
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire storage lock: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // 创建并返回字节数组
    match env.byte_array_from_slice(&data) {
        Ok(arr) => arr.into_raw(),
        Err(e) => {
            eprintln!("Failed to create byte array: {:?}", e);
            std::ptr::null_mut()
        }
    }
}

/// 删除大数据
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheBigData_remove(
    mut env: JNIEnv,
    _class: JClass,
    storage_id: jlong,
    key: JString,
) -> jboolean {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in remove: {:?}", e);
        return 0;
    }

    // 获取存储实例
    let instances = get_storage_instances();
    let storage_instance = match instances.lock() {
        Ok(instances) => {
            match instances.get(&(storage_id as u64)) {
                Some(instance) => instance.clone(),
                None => {
                    eprintln!("Storage instance {} not found", storage_id);
                    return 0;
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire storage instances lock: {:?}", e);
            return 0;
        }
    };

    // 获取键字符串
    let key_str = match env.get_string(&key) {
        Ok(jstr) => jstr.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get key string: {:?}", e);
            return 0;
        }
    };

    // 删除数据
    match storage_instance.lock() {
        Ok(storage) => {
            match storage.remove(&key_str) {
                Ok(true) => 1, // 成功删除
                Ok(false) => 0, // 数据不存在
                Err(e) => {
                    eprintln!("Failed to remove data: {:?}", e);
                    0
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire storage lock: {:?}", e);
            0
        }
    }
}

/// 获取存储统计信息
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheBigData_getStats(
    env: JNIEnv,
    _class: JClass,
    storage_id: jlong,
) -> jstring {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in getStats: {:?}", e);
        return std::ptr::null_mut();
    }

    // 获取存储实例
    let instances = get_storage_instances();
    let storage_instance = match instances.lock() {
        Ok(instances) => {
            match instances.get(&(storage_id as u64)) {
                Some(instance) => instance.clone(),
                None => {
                    eprintln!("Storage instance {} not found", storage_id);
                    return std::ptr::null_mut();
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to acquire storage instances lock: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // 获取统计信息
    let stats = match storage_instance.lock() {
        Ok(storage) => storage.stats(),
        Err(e) => {
            eprintln!("Failed to acquire storage lock: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // 转换为 JSON 字符串
    let stats_json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
    
    // 创建 Java 字符串
    match env.new_string(&stats_json) {
        Ok(jstring) => jstring.into_raw(),
        Err(e) => {
            eprintln!("Failed to create Java string: {:?}", e);
            std::ptr::null_mut()
        }
    }
}

/// 销毁存储实例
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheBigData_destroyStorage(
    env: JNIEnv,
    _class: JClass,
    storage_id: jlong,
) {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in destroyStorage: {:?}", e);
        return;
    }

    // 获取存储实例
    let instances = get_storage_instances();
    match instances.lock() {
        Ok(mut instances) => {
            instances.remove(&(storage_id as u64));
        }
        Err(e) => {
            eprintln!("Failed to acquire storage instances lock: {:?}", e);
        }
    }
}

/// 创建新的大数据存储实例（简化版本，使用默认配置）
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheBigData_createStorage(
    mut env: JNIEnv,
    _class: JClass,
    disk_dir: JString,
    memory_threshold: jlong,
) -> jlong {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in createStorage: {:?}", e);
        return 0;
    }

    // 获取磁盘目录字符串
    let disk_dir_str = match env.get_string(&disk_dir) {
        Ok(jstr) => jstr.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get disk_dir string: {:?}", e);
            return 0;
        }
    };

    // 创建大数据存储实例（使用默认配置，只设置关键参数）
    let storage_config = BigDataStorageConfig {
        disk_dir: PathBuf::from(&disk_dir_str),
        memory_threshold: memory_threshold as usize,
        // 其他参数使用默认值
        ..Default::default()
    };

    let storage = match BigDataStorage::new(storage_config) {
        Ok(storage) => storage,
        Err(e) => {
            eprintln!("Failed to create big data storage: {:?}", e);
            return 0;
        }
    };

    // 生成存储实例 ID 并存储
    let storage_id = generate_storage_id();
    let storage_instance = Arc::new(Mutex::new(storage));
    let instances = get_storage_instances();
    
    match instances.lock() {
        Ok(mut instances) => {
            instances.insert(storage_id, storage_instance);
            storage_id as jlong
        }
        Err(e) => {
            eprintln!("Failed to acquire storage instances lock: {:?}", e);
            0
        }
    }
}

// ============================================================================
// 优化缓存 JNI 接口
// ============================================================================

/// 创建新的优化缓存实例（完整配置版本）
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheOptimized_createCache(
    mut env: JNIEnv,
    _class: JClass,
    disk_dir: JString,
    memory_threshold: jlong,
    max_memory_maps: jlong,
    warmup_threshold: jlong,
    cleanup_interval_secs: jlong,
    memory_cleanup_threshold: jdouble,
) -> jlong {
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        eprintln!("JNI exception in createOptimizedCache: {:?}", e);
        return 0;
    }

    // 获取磁盘目录字符串
    let disk_dir_str = match env.get_string(&disk_dir) {
        Ok(jstr) => jstr.to_string_lossy().to_string(),
        Err(e) => {
            eprintln!("Failed to get disk_dir string: {:?}", e);
            return 0;
        }
    };

    // 创建优化缓存实例（使用所有传入的配置参数）
    let cache_config = OptimizedCacheConfig {
        disk_dir: PathBuf::from(&disk_dir_str),
        memory_threshold: memory_threshold as usize,
        max_memory_maps: max_memory_maps as usize,
        warmup_threshold: warmup_threshold as u64,
        cleanup_interval: std::time::Duration::from_secs(cleanup_interval_secs as u64),
        memory_cleanup_threshold: memory_cleanup_threshold,
    };

    let cache = match OptimizedCache::new(cache_config) {
        Ok(cache) => cache,
        Err(e) => {
            eprintln!("Failed to create optimized cache: {:?}", e);
            return 0;
        }
    };

    // 生成缓存实例 ID 并存储
    let cache_id = generate_storage_id();
    let cache_instance = Arc::new(Mutex::new(cache));
    let instances = get_cache_instances();
    
    match instances.lock() {
        Ok(mut instances) => {
            instances.insert(cache_id, cache_instance);
            cache_id as jlong
        }
        Err(e) => {
            eprintln!("Failed to acquire cache instances lock: {:?}", e);
            0
        }
    }
}