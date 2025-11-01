use jni::JNIEnv;
use jni::objects::{JClass, JString, JByteArray, JObject, JObjectArray};
use jni::sys::{jstring, jbyteArray, jlong, jboolean, jdouble, jobjectArray};
use std::path::PathBuf;
use crate::high_perf_mmap_storage::{HighPerfMmapStorage, HighPerfMmapConfig};
use serde::Deserialize;
use async_std::sync::Arc;

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
    let result = (|| -> Result<jlong, Box<dyn std::error::Error>> {
        // 转换 Java 字符串为 Rust 字符串
        let disk_dir_str: String = env.get_string(&disk_dir)?.into();
        let disk_path = PathBuf::from(disk_dir_str);
        
        // 创建目录
        std::fs::create_dir_all(&disk_path)?;
        
        // 创建配置
        let mut config = HighPerfMmapConfig::default();
        config.initial_file_size = initial_file_size as u64;
        config.growth_step = growth_step as u64;
        config.max_file_size = max_file_size as u64;
        config.enable_compression = enable_compression != 0;
        config.enable_prefetch = enable_prefetch != 0;
        config.prefetch_queue_size = prefetch_queue_size as usize;
        
        //  // 创建存储实例
        let storage = Arc::new(HighPerfMmapStorage::new(disk_path, config)?);
        // 2) 启动后台任务（方案 B：方法签名是 `self: &Arc<Self>`）
        storage.start_background_tasks();
        
        // 句柄类型是 *mut Arc<HighPerfMmapStorage>
        let storage_ptr = Box::into_raw(Box::new(storage));
        Ok(storage_ptr as jlong)
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
    let result = (|| -> Result<jlong, Box<dyn std::error::Error>> {
        // 转换 Java 字符串为 Rust 字符串
        let disk_dir_str: String = env.get_string(&disk_dir)?.into();
        let disk_path = PathBuf::from(disk_dir_str);
        
        // 创建目录
        std::fs::create_dir_all(&disk_path)?;
        
        // 使用默认配置
        let config = HighPerfMmapConfig::default();
        
        // 创建存储实例
        // let storage = HighPerfMmapStorage::new(disk_path, config)?;
        // let storage_ptr = Box::into_raw(Box::new(storage));
        
       //  // 创建存储实例
       let storage = Arc::new(HighPerfMmapStorage::new(disk_path, config)?);
       // 2) 启动后台任务（方案 B：方法签名是 `self: &Arc<Self>`）
       storage.start_background_tasks();
       
       // 句柄类型是 *mut Arc<HighPerfMmapStorage>
       let storage_ptr = Box::into_raw(Box::new(storage));
       Ok(storage_ptr as jlong)
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
    #[derive(Deserialize)]
    struct IncomingConfig {
        disk_dir: String,
        #[serde(default)] initial_file_size: Option<u64>,
        #[serde(default)] growth_step: Option<u64>,
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
    }

    let result = (|| -> Result<jlong, Box<dyn std::error::Error>> {
        // 解析 JSON
        let json_str: String = env.get_string(&json_config)?.into();
        let incoming: IncomingConfig = serde_json::from_str(&json_str)?;

        // 目录
        let disk_path = PathBuf::from(&incoming.disk_dir);
        std::fs::create_dir_all(&disk_path)?;

        // 合并默认配置与覆盖项
        let mut cfg = HighPerfMmapConfig::default();
        if let Some(v) = incoming.initial_file_size { cfg.initial_file_size = v; }
        if let Some(v) = incoming.growth_step { cfg.growth_step = v; }
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

        // 创建存储实例
        // let storage = HighPerfMmapStorage::new(disk_path, cfg)?;
        // let storage_ptr = Box::into_raw(Box::new(storage));
       //  // 创建存储实例
       let storage = Arc::new(HighPerfMmapStorage::new(disk_path, cfg)?);
       // 2) 启动后台任务（方案 B：方法签名是 `self: &Arc<Self>`）
       storage.start_background_tasks();
       
       // 句柄类型是 *mut Arc<HighPerfMmapStorage>
       let storage_ptr = Box::into_raw(Box::new(storage));
       Ok(storage_ptr as jlong)
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
    if storage_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Storage pointer is null")
            .unwrap_or_default();
        return 0;
    }
    
    let result = (|| -> Result<jboolean, Box<dyn std::error::Error>> {
        // 获取存储实例
        // 转换 Java 字符串为 Rust 字符串
        let key_str: String = env.get_string(&key)?.into();

        // 转换 Java 字节数组为 Rust 字节切片
        let data_bytes = env.convert_byte_array(&data)?;

        let write_result = unsafe {
            let storage =  &*(storage_ptr as *const Arc<HighPerfMmapStorage>);
            // 执行写入操作
            storage.write(&key_str, &data_bytes)
                .map(|_| true)  // 成功时返回 true
                .map_err(|e| e.into())  // 转换错误类型;
        };
        match write_result {
            Ok(success) => Ok(1),
            Err(e) =>  Err(e), // 处理错误
        } // 成功返回 true
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
    if storage_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Storage pointer is null")
            .unwrap_or_default();
        return std::ptr::null_mut();
    }
    
    let result = (|| -> Result<jbyteArray, Box<dyn std::error::Error>> {
        // 获取存储实例
        let storage: &Arc<HighPerfMmapStorage> = unsafe { &*(storage_ptr as *const Arc<HighPerfMmapStorage>) };
        
        // 转换 Java 字符串为 Rust 字符串
        let key_str: String = env.get_string(&key)?.into();
        
        // 执行读取操作
        match storage.read(&key_str)? {
            Some(data) => {
                // 将 Rust 字节向量转换为 Java 字节数组
                let java_array = env.byte_array_from_slice(&data)?;
                Ok(java_array.into_raw())
            }
            None => {
                // 数据不存在，返回 null
                Ok(std::ptr::null_mut())
            }
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
    if storage_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Storage pointer is null")
            .unwrap_or_default();
        return 0;
    }

    let result = (|| -> Result<jboolean, Box<dyn std::error::Error>> {
        // 获取存储实例
        let storage: &Arc<HighPerfMmapStorage> = unsafe { &*(storage_ptr as *const Arc<HighPerfMmapStorage>) };
        // 转换 Java 字符串为 Rust 字符串
        let key_str: String = env.get_string(&key)?.into();

        // 执行删除操作
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

/// 批量写入数据
/// 对应 Java 方法：public static native boolean writeBatch(long storagePtr, String[] keys, byte[][] dataArray)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_writeBatch(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
    keys: JObjectArray,
    data_array: JObjectArray,
) -> jboolean {
    if storage_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Storage pointer is null")
            .unwrap_or_default();
        return 0;
    }
    
    let result = (|| -> Result<jboolean, Box<dyn std::error::Error>> {
        // 获取存储实例
    
        let storage: &Arc<HighPerfMmapStorage> = unsafe { &*(storage_ptr as *const Arc<HighPerfMmapStorage>) };
        // 获取数组长度
        let keys_len = env.get_array_length(&keys)?;
        let data_len = env.get_array_length(&data_array)?;
        
        if keys_len != data_len {
            return Err("Keys and data arrays must have the same length".into());
        }
        
        // 批量写入数据
        for i in 0..keys_len {
            let key_obj = env.get_object_array_element(&keys, i)?;
            let key_str: String = env.get_string(&key_obj.into())?.into();
            
            let data_obj = env.get_object_array_element(&data_array, i)?;
            let data_bytes = env.convert_byte_array(&JByteArray::from(data_obj))?;
            
            storage.write(&key_str, &data_bytes)?;
        }
        
        Ok(1) // 成功返回 true
    })();
    
    match result {
        Ok(success) => success,
        Err(e) => {
            let error_msg = format!("Failed to write batch data: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            0
        }
    }
}

/// 批量读取数据
/// 对应 Java 方法：public static native byte[][] readBatch(long storagePtr, String[] keys)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_readBatch(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
    keys: JObjectArray,
) -> jobjectArray {
    if storage_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Storage pointer is null")
            .unwrap_or_default();
        return std::ptr::null_mut();
    }
    
    let result = (|| -> Result<jobjectArray, Box<dyn std::error::Error>> {
        // 获取存储实例
        let storage: &Arc<HighPerfMmapStorage> = unsafe { &*(storage_ptr as *const Arc<HighPerfMmapStorage>) };
        // 获取键数组长度
        let keys_len = env.get_array_length(&keys)?;
        
        // 创建结果数组
        let result_array = env.new_object_array(keys_len, "[B", JObject::default())?;
        
        // 批量读取数据
        for i in 0..keys_len {
            let key_obj = env.get_object_array_element(&keys, i)?;
            let key_str: String = env.get_string(&key_obj.into())?.into();
            
            match storage.read(&key_str)? {
                Some(data) => {
                    let java_array = env.byte_array_from_slice(&data)?;
                    env.set_object_array_element(&result_array, i, &JObject::from(java_array))?;
                }
                None => {
                    // 数据不存在，设置为 null
                    env.set_object_array_element(&result_array, i, &JObject::default())?;
                }
            }
        }
        
        Ok(result_array.into_raw())
    })();
    
    match result {
        Ok(result_array) => result_array,
        Err(e) => {
            let error_msg = format!("Failed to read batch data: {}", e);
            env.throw_new("java/lang/RuntimeException", &error_msg)
                .unwrap_or_default();
            std::ptr::null_mut()
        }
    }
}

/// 获取统计信息
/// 对应 Java 方法：public static native String getStats(long storagePtr)
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_com_flux_collaboration_utils_cache_rust_jni_DatahubRustJniCache_getStats(
    mut env: JNIEnv,
    _class: JClass,
    storage_ptr: jlong,
) -> jstring {
    if storage_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Storage pointer is null")
            .unwrap_or_default();
        return std::ptr::null_mut();
    }
    
    let result = (|| -> Result<jstring, Box<dyn std::error::Error>> {
        // 获取存储实例
        let storage: &Arc<HighPerfMmapStorage> = unsafe { &*(storage_ptr as *const Arc<HighPerfMmapStorage>) };
        
        // 获取统计信息
        let stats = storage.get_stats();
        
        // 格式化统计信息为 JSON 字符串
        let stats_json = serde_json::json!({
            "total_writes": stats.total_writes,
            "total_reads": stats.total_reads,
            "total_write_bytes": stats.total_write_bytes,
            "total_read_bytes": stats.total_read_bytes,
            "l1_cache_hits": stats.l1_cache_hits,
            "l1_cache_misses": stats.l1_cache_misses,
            "l2_cache_hits": stats.l2_cache_hits,
            "l2_cache_misses": stats.l2_cache_misses,
            "prefetch_hits": stats.prefetch_hits,
            "avg_write_latency_us": stats.avg_write_latency_us,
            "avg_read_latency_us": stats.avg_read_latency_us,
            "mmap_remaps": stats.mmap_remaps,
            "l1_cache_hit_rate": if stats.l1_cache_hits + stats.l1_cache_misses > 0 {
                stats.l1_cache_hits as f64 / (stats.l1_cache_hits + stats.l1_cache_misses) as f64 * 100.0
            } else {
                0.0
            },
            "l2_cache_hit_rate": if stats.l2_cache_hits + stats.l2_cache_misses > 0 {
                stats.l2_cache_hits as f64 / (stats.l2_cache_hits + stats.l2_cache_misses) as f64 * 100.0
            } else {
                0.0
            }
        });
        
        let stats_str = stats_json.to_string();
        let java_string = env.new_string(&stats_str)?;
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
    
    let result = (|| -> Result<jboolean, Box<dyn std::error::Error>> {
        // 获取存储实例
        let storage = unsafe { &*(storage_ptr as *const Arc<HighPerfMmapStorage>) };
        
        // 保存索引
        storage.save_index()?;
        
        Ok(1) // 成功返回 true
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
            let boxed: Box<Arc<HighPerfMmapStorage>> = Box::from_raw(storage_ptr as *mut Arc<HighPerfMmapStorage>);
            boxed.stop_background_tasks();   // 调用在 &Arc 上：(*boxed).stop_background_tasks() 也行
            // 离开作用域后 drop(boxed)：Arc 引用计数 -1，若为最后一个则释放底层对象
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
    if storage_ptr == 0 {
        return 0;
    }
    
    // 简单检查指针是否有效
    unsafe {
        let storage = &*(storage_ptr as *const Arc<HighPerfMmapStorage>);
        // 这里可以添加更多的有效性检查
        std::ptr::addr_of!(*storage).is_null() as jboolean
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
    if storage_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Storage pointer is null")
            .unwrap_or_default();
        return 0.0;
    }
    
    let result = (|| -> Result<jdouble, Box<dyn std::error::Error>> {
        // 获取存储实例
        let storage: &Arc<HighPerfMmapStorage> = unsafe { &*(storage_ptr as *const Arc<HighPerfMmapStorage>) };

        // 获取统计信息
        let stats = storage.get_stats();
        
        // 计算缓存命中率 (使用 L1 缓存命中率)
        let hit_rate = if stats.l1_cache_hits + stats.l1_cache_misses > 0 {
            stats.l1_cache_hits as f64 / (stats.l1_cache_hits + stats.l1_cache_misses) as f64 * 100.0
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
    if storage_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Storage pointer is null")
            .unwrap_or_default();
        return 0;
    }
    
    let result = (|| -> Result<jlong, Box<dyn std::error::Error>> {
        // 获取存储实例
        let storage: &Arc<HighPerfMmapStorage> = unsafe { &*(storage_ptr as *const Arc<HighPerfMmapStorage>) };
        // 获取统计信息
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
    if storage_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Storage pointer is null")
            .unwrap_or_default();
        return 0;
    }
    
    let result = (|| -> Result<jlong, Box<dyn std::error::Error>> {
        // 获取存储实例
     let storage: &Arc<HighPerfMmapStorage> = unsafe { &*(storage_ptr as *const Arc<HighPerfMmapStorage>) };
        // 获取统计信息
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
    if storage_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Storage pointer is null")
            .unwrap_or_default();
        return 0;
    }

    let result = (|| -> Result<jlong, Box<dyn std::error::Error>> {
        // 获取存储实例
        let storage: &Arc<HighPerfMmapStorage> = unsafe { &*(storage_ptr as *const Arc<HighPerfMmapStorage>) };
        // 执行垃圾回收，返回清理条目数
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