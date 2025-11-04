use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jboolean, jbyteArray, jdouble, jlong, jstring};
use std::path::PathBuf;
use std::sync::Arc;

use crate::high_perf_mmap_storage::{
    HighPerfMmapConfig,
    HighPerfMmapStatus,
    HighPerfMmapStorage,
    MemoryLimitConfig,
};
use serde::Deserialize;
use serde_json;

type ResultBox<T> = Result<T, Box<dyn std::error::Error>>;

fn create_storage_handle(
    disk_path: PathBuf,
    config: HighPerfMmapConfig,
    memory_limit: MemoryLimitConfig,
    clear_on_start: bool,
) -> ResultBox<jlong> {
    std::fs::create_dir_all(&disk_path)?;

    let storage = Arc::new(HighPerfMmapStorage::new_with_options(
        disk_path,
        config,
        memory_limit,
        clear_on_start,
    )?);
    storage.start_background_tasks();

    Ok(Box::into_raw(Box::new(storage)) as jlong)
}

fn storage_from_ptr<'a>(storage_ptr: jlong) -> Result<&'a Arc<HighPerfMmapStorage>, &'static str> {
    if storage_ptr == 0 {
        return Err("storage pointer is null");
    }
    unsafe {
        (storage_ptr as *const Arc<HighPerfMmapStorage>)
            .as_ref()
            .ok_or("storage pointer is invalid")
    }
}

/// DatahubRustJniCache 的 JNI 桥接实现
/// 对应 Java 类：com.flux.collaboration.utils.cache.rust.jni.DatahubRustJniCache

/// 创建高性能 MMAP 存储实例
/// 对应 Java 方法：public static native long createHighPerfMmapStorage(String diskDir, ...)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_createHighPerfMmapStorage(
    mut env: JNIEnv,
    _class: JClass,
    disk_dir: JString,
    initial_file_size: jlong,
    growth_step: jlong,
    max_file_size: jlong,
    enable_compression: jboolean,
    _reserved1: jlong,  // 保留参数
    _reserved2: jlong,  // 保留参数
    enable_prefetch: jboolean,
    prefetch_queue_size: jlong,
) -> jlong {
    let result = (|| -> ResultBox<jlong> {
        let disk_dir_str: String = env.get_string(&disk_dir)?.into();
        let disk_path = PathBuf::from(disk_dir_str);

        let mut config = HighPerfMmapConfig::default();
        config.initial_file_size = initial_file_size.max(0) as u64;
        config.growth_step = growth_step.max(0) as u64;
        config.max_file_size = max_file_size.max(0) as u64;
        config.enable_compression = enable_compression != 0;
        config.enable_prefetch = enable_prefetch != 0;
        if prefetch_queue_size > 0 {
            config.prefetch_queue_size = prefetch_queue_size as usize;
        }

        create_storage_handle(disk_path, config, MemoryLimitConfig::default(), false)
    })();
    
    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            let error_msg = format!("Failed to create HighPerfMmapStorage: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            0
        }
    }
}

/// 创建高性能 MMAP 存储实例（简化版本）
/// 对应 Java 方法：public static native long createHighPerfMmapStorageSimple(String diskDir)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_createHighPerfMmapStorageSimple(
    mut env: JNIEnv,
    _class: JClass,
    disk_dir: JString,
) -> jlong {
    let result = (|| -> ResultBox<jlong> {
        let disk_dir_str: String = env.get_string(&disk_dir)?.into();
        let disk_path = PathBuf::from(disk_dir_str);

        let config = HighPerfMmapConfig::default();
        let memory = MemoryLimitConfig::default();

        create_storage_handle(disk_path, config, memory, false)
    })();
    
    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            let error_msg = format!("Failed to create HighPerfMmapStorage: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            0
        }
    }
}

/// 通过 JSON 字符串创建高性能 MMAP 存储实例（推荐）
/// 对应 Java 方法：public static native long createHighPerfMmapStorageFromJson(String jsonConfig)
/// JSON 示例：
/// {
///   "disk_dir": "/tmp/cache",
///   "initial_file_size": 104857600,
///   "growth_step": 52428800,
///   "max_file_size": 10737418240,
///   "enable_compression": false,
///   "l1_cache_size_limit": 52428800,
///   "l1_cache_entry_limit": 500,
///   "l2_cache_size_limit": 209715200,
///   "l2_cache_entry_limit": 2000,
///   "enable_prefetch": true,
///   "prefetch_queue_size": 100,
///   "memory_pressure_threshold": 0.8,
///   "cache_degradation_threshold": 0.9
/// }
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_createHighPerfMmapStorageFromJson(
    mut env: JNIEnv,
    _class: JClass,
    json_config: JString,
) -> jlong {
    #[derive(Debug, Deserialize)]
    struct IncomingConfig {
        disk_dir: String,
        #[serde(default)] clear_on_start: bool,
        #[serde(default)] config: Option<HighPerfMmapConfig>,
        #[serde(default)] memory_limit: Option<MemoryLimitConfig>,
        #[serde(default)] compression: Option<crate::high_perf_mmap_storage::CompressionConfig>,

        #[serde(default)] initial_file_size: Option<u64>,
        #[serde(default)] growth_step: Option<u64>,
        #[serde(default)] growth_reserve_steps: Option<u32>,
        #[serde(default)] max_file_size: Option<u64>,
        #[serde(default)] enable_compression: Option<bool>,
        #[serde(default)] l1_cache_size_limit: Option<u64>,
        #[serde(default)] l1_cache_entry_limit: Option<usize>,
        #[serde(default)] l2_cache_size_limit: Option<u64>,
        #[serde(default)] l2_cache_entry_limit: Option<usize>,
        #[serde(default)] enable_prefetch: Option<bool>,
        #[serde(default)] prefetch_queue_size: Option<usize>,
        #[serde(default)] memory_pressure_threshold: Option<f64>,
        #[serde(default)] cache_degradation_threshold: Option<f64>,

        #[serde(default)] heap_soft_limit: Option<u64>,
        #[serde(default)] heap_hard_limit: Option<u64>,
        #[serde(default)] l1_cache_hard_limit: Option<u64>,
        #[serde(default)] l1_cache_entry_hard_limit: Option<usize>,
        #[serde(default)] max_eviction_percent: Option<f64>,
        #[serde(default)] reject_writes_under_pressure: Option<bool>,
        #[serde(default)] check_interval_ms: Option<u64>,
    }

    let result = (|| -> ResultBox<jlong> {
        // 解析 JSON
        let json_str: String = env.get_string(&json_config)?.into();
        let incoming: IncomingConfig = serde_json::from_str(&json_str)?;

        // 目录
        let disk_path = PathBuf::from(&incoming.disk_dir);

        let mut cfg = incoming.config.unwrap_or_default();
        if let Some(v) = incoming.initial_file_size { cfg.initial_file_size = v; }
        if let Some(v) = incoming.growth_step { cfg.growth_step = v; }
        if let Some(v) = incoming.growth_reserve_steps { cfg.growth_reserve_steps = v; }
        if let Some(v) = incoming.max_file_size { cfg.max_file_size = v; }
        if let Some(v) = incoming.enable_compression { cfg.enable_compression = v; }
        if let Some(v) = incoming.l1_cache_size_limit { cfg.l1_cache_size_limit = v; }
        if let Some(v) = incoming.l1_cache_entry_limit { cfg.l1_cache_entry_limit = v; }
        if let Some(v) = incoming.l2_cache_size_limit { cfg.l2_cache_size_limit = v; }
        if let Some(v) = incoming.l2_cache_entry_limit { cfg.l2_cache_entry_limit = v; }
        if let Some(v) = incoming.enable_prefetch { cfg.enable_prefetch = v; }
        if let Some(v) = incoming.prefetch_queue_size { cfg.prefetch_queue_size = v; }
        if let Some(v) = incoming.memory_pressure_threshold { cfg.memory_pressure_threshold = v; }
        if let Some(v) = incoming.cache_degradation_threshold { cfg.cache_degradation_threshold = v; }
        if let Some(v) = incoming.compression { cfg.compression = v; }

        let mut memory = incoming.memory_limit.unwrap_or_default();
        if let Some(v) = incoming.heap_soft_limit { memory.heap_soft_limit = v; }
        if let Some(v) = incoming.heap_hard_limit { memory.heap_hard_limit = v; }
        if let Some(v) = incoming.l1_cache_hard_limit { memory.l1_cache_hard_limit = v; }
        if let Some(v) = incoming.l1_cache_entry_hard_limit { memory.l1_cache_entry_hard_limit = v; }
        if let Some(v) = incoming.max_eviction_percent { memory.max_eviction_percent = v; }
        if let Some(v) = incoming.reject_writes_under_pressure { memory.reject_writes_under_pressure = v; }
        if let Some(v) = incoming.check_interval_ms { memory.check_interval_ms = v; }

        create_storage_handle(disk_path, cfg, memory, incoming.clear_on_start)
    })();

    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            let error_msg = format!("Failed to create HighPerfMmapStorage from JSON: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            0
        }
    }
}

/// 写入数据
/// 对应 Java 方法：public static native boolean write(long storagePtr, String key, byte[] data)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_write(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
    key: JString,
    data: JByteArray,
) -> jboolean {
    let result = (|| -> ResultBox<jboolean> {
        let storage = storage_from_ptr(storage_ptr)
            .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?;

        let key_str: String = env.get_string(&key)?.into();
        let data_bytes = env.convert_byte_array(&data)?;

        storage.write(&key_str, &data_bytes)?;
        Ok(1)
    })();
    
    match result {
        Ok(success) => success,
        Err(e) => {
            let error_msg = format!("Failed to write data: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            0
        }
    }
}

/// 读取数据
/// 对应 Java 方法：public static native byte[] read(long storagePtr, String key)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_read(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
    key: JString,
) -> jbyteArray {
    let result = (|| -> ResultBox<jbyteArray> {
        let storage = storage_from_ptr(storage_ptr)
            .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?;

        let key_str: String = env.get_string(&key)?.into();

        match storage.read(&key_str)? {
            Some(data) => {
                let java_array = env.byte_array_from_slice(&data)?;
                Ok(java_array.into_raw())
            }
            None => Ok(std::ptr::null_mut()),
        }
    })();
    
    match result {
        Ok(java_array) => java_array,
        Err(e) => {
            let error_msg = format!("Failed to read data: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            std::ptr::null_mut()
        }
    }
}

/// 删除指定 key
/// 对应 Java 方法：public static native boolean delete(long storagePtr, String key)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_delete(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
    key: JString,
) -> jboolean {
    let result = (|| -> ResultBox<jboolean> {
        let storage = storage_from_ptr(storage_ptr)
            .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?;

        let key_str: String = env.get_string(&key)?.into();

        let removed = storage.delete(&key_str)?;
        Ok(if removed { 1 } else { 0 })
    })();

    match result {
        Ok(success) => success,
        Err(e) => {
            let error_msg = format!("Failed to delete key: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            0
        }
    }
}

/// 获取状态信息（读写删除 + 内存）
/// 对应 Java 方法：public static native String getStatus(long storagePtr)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_getStatus(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
) -> jstring {
    let result = (|| -> ResultBox<jstring> {
        let storage = storage_from_ptr(storage_ptr)
            .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?;

        let status: HighPerfMmapStatus = storage.get_status();
        let status_json = serde_json::to_string(&status)?;
        let java_string = env.new_string(&status_json)?;
        Ok(java_string.into_raw())
    })();
    
    match result {
        Ok(java_string) => java_string,
        Err(e) => {
            let error_msg = format!("Failed to get stats: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            std::ptr::null_mut()
        }
    }
}

/// 兼容旧接口：getStats 调用 getStatus
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_getStats(
    env: JNIEnv,
    class: JClass,
    storage_ptr: jlong,
) -> jstring {
    let env = env;
    Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_getStatus(
        env,
        class,
        storage_ptr,
    )
}

/// 保存索引
/// 对应 Java 方法：public static native boolean saveIndex(long storagePtr)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_saveIndex(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
) -> jboolean {
    if storage_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Storage pointer is null")
            .unwrap_or_default();
        return 0;
    }

    let result = (|| -> ResultBox<jboolean> {
        // 获取存储实例
        let storage = storage_from_ptr(storage_ptr)
            .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?;

        storage.save_index()?;

        Ok(1)
    })();
    
    match result {
        Ok(success) => success,
        Err(e) => {
            let error_msg = format!("Failed to save index: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            0
        }
    }
}

/// 释放存储实例
/// 对应 Java 方法：public static native void release(long storagePtr)
#[unsafe(no_mangle)]
pub  extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_release(
    _env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
) {
    if storage_ptr != 0 {
        unsafe {
            let boxed: Box<Arc<HighPerfMmapStorage>> =
                Box::from_raw(storage_ptr as *mut Arc<HighPerfMmapStorage>);
            Arc::as_ref(&boxed).stop_background_tasks();
            // drop(boxed) at end of scope, decrementing Arc count
        }
    }
}

/// 检查存储实例是否有效
/// 对应 Java 方法：public static native boolean isValid(long storagePtr)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_isValid(
    _env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
) -> jboolean {
    match storage_from_ptr(storage_ptr) {
        Ok(_) => 1,
        Err(_) => 0,
    }
}

/// 获取缓存命中率
/// 对应 Java 方法：public static native double getCacheHitRate(long storagePtr)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_getCacheHitRate(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
) -> jdouble {
    let result = (|| -> ResultBox<jdouble> {
        let storage = storage_from_ptr(storage_ptr)
            .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?;

        let stats = storage.get_stats();

        let total = stats.l1_cache_hits + stats.l1_cache_misses;
        let hit_rate = if total > 0 {
            (stats.l1_cache_hits as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        Ok(hit_rate)
    })();
    
    match result {
        Ok(hit_rate) => hit_rate,
        Err(e) => {
            let error_msg = format!("Failed to get cache hit rate: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            0.0
        }
    }
}

/// 获取平均读取延迟
/// 对应 Java 方法：public static native long getAvgReadLatency(long storagePtr)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_getAvgReadLatency(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
) -> jlong {
    let result = (|| -> ResultBox<jlong> {
        let storage = storage_from_ptr(storage_ptr)
            .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?;

        let stats = storage.get_stats();

        Ok(stats.avg_read_latency_us as jlong)
    })();
    
    match result {
        Ok(latency) => latency,
        Err(e) => {
            let error_msg = format!("Failed to get average read latency: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            0
        }
    }
}

/// 获取平均写入延迟
/// 对应 Java 方法：public static native long getAvgWriteLatency(long storagePtr)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_getAvgWriteLatency(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
) -> jlong {
    let result = (|| -> ResultBox<jlong> {
        let storage = storage_from_ptr(storage_ptr)
            .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?;

        let stats = storage.get_stats();

        Ok(stats.avg_write_latency_us as jlong)
    })();
    
    match result {
        Ok(latency) => latency,
        Err(e) => {
            let error_msg = format!("Failed to get average write latency: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            0
        }
    }
}

/// 触发垃圾回收（磁盘压缩回收）
/// 对应 Java 方法：public static native long forceGarbageCollect(long storagePtr)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_forceGarbageCollect(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
) -> jlong {
    let result = (|| -> ResultBox<jlong> {
        let storage = storage_from_ptr(storage_ptr)
            .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?;

        let deleted_count = storage.garbage_collect()?;
        Ok(deleted_count as jlong)
    })();

    match result {
        Ok(count) => count,
        Err(e) => {
            let error_msg = format!("Failed to run garbage collect: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            0
        }
    }
}
