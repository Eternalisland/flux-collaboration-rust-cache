//! # 增强的JNI桥接模块
//! 
//! 提供完善的错误处理和诊断功能的JNI接口
//! - 详细的错误码和错误信息
//! - 错误上下文跟踪
//! - Java端友好的错误报告

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;

use jni::objects::{JClass, JString, JByteArray};
use jni::sys::{jlong, jboolean, jstring};
use jni::JNIEnv;

use crate::big_data_storage::{BigDataStorage, BigDataStorageConfig};
use crate::optimized_cache::{/* OptimizedCache, OptimizedCacheConfig */};
use crate::error_handling::{
    CacheError, ErrorCode, ErrorReport, /* ErrorContext, */ ConfigInfo,
    create_error_context, create_error_report, generate_operation_id,
    handle_jni_error, handle_file_error
};

/// 全局大数据存储实例
type BigDataStorageInstance = Arc<Mutex<BigDataStorage>>;

/// 全局存储实例存储
static STORAGE_INSTANCES: std::sync::OnceLock<Arc<Mutex<HashMap<u64, BigDataStorageInstance>>>> = std::sync::OnceLock::new();

/// 全局错误报告存储
static ERROR_REPORTS: std::sync::OnceLock<Arc<Mutex<HashMap<u64, ErrorReport>>>> = std::sync::OnceLock::new();

/// 获取全局存储实例存储
fn get_storage_instances() -> &'static Arc<Mutex<HashMap<u64, BigDataStorageInstance>>> {
    STORAGE_INSTANCES.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

/// 获取全局错误报告存储
fn get_error_reports() -> &'static Arc<Mutex<HashMap<u64, ErrorReport>>> {
    ERROR_REPORTS.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

/// 生成新的存储实例 ID
fn generate_storage_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// 将错误报告转换为JSON字符串
fn error_report_to_json(report: &ErrorReport) -> String {
    serde_json::to_string(report).unwrap_or_else(|_| "{}".to_string())
}

/// 创建新的大数据存储实例（增强错误处理版本）
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheEnhanced_createStorage(
    mut env: JNIEnv,
    _class: JClass,
    disk_dir: JString,
    memory_threshold: jlong,
) -> jlong {
    let operation_id = generate_operation_id();
    
    // 处理 JNI 异常
    if let Err(e) = env.exception_check() {
        let error_type = CacheError::JniError {
            operation: "createStorage".to_string(),
            details: format!("JNI exception check failed: {:?}", e),
        };
        let context = create_error_context("createStorage");
        let error_report = create_error_report(ErrorCode::JniError, error_type, context, None);
        
        // 存储错误报告
        let error_reports = get_error_reports();
        if let Ok(mut reports) = error_reports.lock() {
            reports.insert(operation_id, error_report);
        }
        
        eprintln!("JNI exception in createStorage: {:?}", e);
        return -(operation_id as jlong); // 返回负数表示错误，操作ID用于查询错误详情
    }

    // 验证参数
    if memory_threshold <= 0 {
        let error_type = CacheError::ConfigurationError {
            parameter: "memory_threshold".to_string(),
            value: memory_threshold.to_string(),
            details: "Memory threshold must be positive".to_string(),
        };
        let context = create_error_context("createStorage");
        let config_info = Some(ConfigInfo {
            memory_threshold: memory_threshold as u64,
            disk_directory: "unknown".to_string(),
            max_memory_maps: 0,
            cleanup_interval: 0,
        });
        let error_report = create_error_report(ErrorCode::InvalidMemoryThreshold, error_type, context, config_info);
        
        let error_reports = get_error_reports();
        if let Ok(mut reports) = error_reports.lock() {
            reports.insert(operation_id, error_report);
        }
        
        return -(operation_id as jlong);
    }

    // 获取磁盘目录字符串
    let disk_dir_str = match handle_jni_error("get_disk_dir_string", env.get_string(&disk_dir)) {
        Ok(jstr) => jstr.to_string_lossy().to_string(),
        Err(error_report) => {
            let error_reports = get_error_reports();
            if let Ok(mut reports) = error_reports.lock() {
                reports.insert(operation_id, error_report);
            }
            return -(operation_id as jlong);
        }
    };

    // 验证路径
    let disk_path = PathBuf::from(&disk_dir_str);
    if !disk_path.parent().map(|p| p.exists()).unwrap_or(false) {
        let error_type = CacheError::ConfigurationError {
            parameter: "disk_dir".to_string(),
            value: disk_dir_str.clone(),
            details: "Parent directory does not exist".to_string(),
        };
        let context = create_error_context("createStorage");
        let config_info = Some(ConfigInfo {
            memory_threshold: memory_threshold as u64,
            disk_directory: disk_dir_str.clone(),
            max_memory_maps: 0,
            cleanup_interval: 0,
        });
        let error_report = create_error_report(ErrorCode::InvalidPath, error_type, context, config_info);
        
        let error_reports = get_error_reports();
        if let Ok(mut reports) = error_reports.lock() {
            reports.insert(operation_id, error_report);
        }
        
        return -(operation_id as jlong);
    }

    // 创建大数据存储实例
    let storage_config = BigDataStorageConfig {
        disk_dir: disk_path,
        memory_threshold: memory_threshold as usize,
        ..Default::default()
    };

    let storage = match handle_file_error("create_storage", &disk_dir_str, BigDataStorage::new(storage_config)) {
        Ok(storage) => storage,
        Err(error_report) => {
            let error_reports = get_error_reports();
            if let Ok(mut reports) = error_reports.lock() {
                reports.insert(operation_id, error_report);
            }
            return -(operation_id as jlong);
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
            let error_type = CacheError::ConcurrencyError {
                operation: "insert_storage_instance".to_string(),
                details: format!("Failed to acquire storage instances lock: {:?}", e),
            };
            let context = create_error_context("createStorage");
            let error_report = create_error_report(ErrorCode::LockTimeout, error_type, context, None);
            
            let error_reports = get_error_reports();
            if let Ok(mut reports) = error_reports.lock() {
                reports.insert(operation_id, error_report);
            }
            
            -(operation_id as jlong)
        }
    }
}

/// 获取错误报告详情
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheEnhanced_getErrorReport(
    env: JNIEnv,
    _class: JClass,
    operation_id: jlong,
) -> jstring {
    let operation_id = operation_id.abs() as u64; // 取绝对值
    
    let error_reports = get_error_reports();
    match error_reports.lock() {
        Ok(reports) => {
            match reports.get(&operation_id) {
                Some(error_report) => {
                    let json_str = error_report_to_json(error_report);
                    match env.new_string(&json_str) {
                        Ok(jstring) => jstring.into_raw(),
                        Err(_) => std::ptr::null_mut(),
                    }
                }
                None => {
                    // 没有找到错误报告，返回默认错误信息
                    let default_error = r#"{
                        "error_code": 9999,
                        "error_type": "UnknownError",
                        "details": "Error report not found",
                        "operation_id": 0
                    }"#;
                    match env.new_string(default_error) {
                        Ok(jstring) => jstring.into_raw(),
                        Err(_) => std::ptr::null_mut(),
                    }
                }
            }
        }
        Err(_) => {
            // 锁获取失败
            let lock_error = r#"{
                "error_code": 6001,
                "error_type": "ConcurrencyError",
                "details": "Failed to acquire error reports lock",
                "operation_id": 0
            }"#;
            match env.new_string(lock_error) {
                Ok(jstring) => jstring.into_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
    }
}

/// 获取系统诊断信息
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheEnhanced_getSystemDiagnostics(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let diagnostics = crate::error_handling::get_system_info();
    let json_str = serde_json::to_string(&diagnostics).unwrap_or_else(|_| "{}".to_string());
    
    match env.new_string(&json_str) {
        Ok(jstring) => jstring.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// 获取存储实例统计信息（包含错误统计）
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheEnhanced_getStorageDiagnostics(
    env: JNIEnv,
    _class: JClass,
    storage_id: jlong,
) -> jstring {
    let instances = get_storage_instances();
    let error_reports = get_error_reports();
    
    let mut diagnostics = serde_json::Map::new();
    
    // 获取存储统计信息
    match instances.lock() {
        Ok(instances) => {
            match instances.get(&(storage_id as u64)) {
                Some(instance) => {
                    match instance.lock() {
                        Ok(storage) => {
                            let stats = storage.stats();
                            diagnostics.insert("storage_stats".to_string(), serde_json::to_value(stats).unwrap_or(serde_json::Value::Null));
                        }
                        Err(_) => {
                            diagnostics.insert("storage_stats".to_string(), serde_json::Value::String("Failed to acquire storage lock".to_string()));
                        }
                    }
                }
                None => {
                    diagnostics.insert("storage_stats".to_string(), serde_json::Value::String("Storage instance not found".to_string()));
                }
            }
        }
        Err(_) => {
            diagnostics.insert("storage_stats".to_string(), serde_json::Value::String("Failed to acquire instances lock".to_string()));
        }
    }
    
    // 获取错误统计信息
    match error_reports.lock() {
        Ok(reports) => {
            let error_count = reports.len();
            let recent_errors: Vec<&ErrorReport> = reports.values().take(10).collect();
            
            diagnostics.insert("error_count".to_string(), serde_json::Value::Number(serde_json::Number::from(error_count)));
            diagnostics.insert("recent_errors".to_string(), serde_json::to_value(recent_errors).unwrap_or(serde_json::Value::Null));
        }
        Err(_) => {
            diagnostics.insert("error_count".to_string(), serde_json::Value::Number(serde_json::Number::from(0)));
        }
    }
    
    let json_str = serde_json::to_string(&diagnostics).unwrap_or_else(|_| "{}".to_string());
    
    match env.new_string(&json_str) {
        Ok(jstring) => jstring.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// 清理错误报告（防止内存泄露）
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheEnhanced_clearErrorReports(
    _env: JNIEnv,
    _class: JClass,
    older_than_hours: jlong,
) -> jboolean {
    let cutoff_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() - (older_than_hours as u64 * 3600);
    
    let error_reports = get_error_reports();
    match error_reports.lock() {
        Ok(mut reports) => {
            let initial_count = reports.len();
            reports.retain(|_, report| report.context.timestamp > cutoff_time);
            let final_count = reports.len();
            
            println!("Cleared {} error reports older than {} hours", initial_count - final_count, older_than_hours);
            1 // 成功
        }
        Err(_) => 0 // 失败
    }
}

/// 存储数据（增强错误处理版本）
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_flux_collaboration_storage_jni_rust_cache_DatahubRusCacheEnhanced_store(
    mut env: JNIEnv,
    _class: JClass,
    storage_id: jlong,
    key: JString,
    data: JByteArray,
) -> jlong {
    let operation_id = generate_operation_id();
    
    // 获取存储实例
    let instances = get_storage_instances();
    let storage_instance = match instances.lock() {
        Ok(instances) => {
            match instances.get(&(storage_id as u64)) {
                Some(instance) => instance.clone(),
                None => {
                    let error_type = CacheError::StorageError {
                        operation: "store".to_string(),
                        key: None,
                        details: format!("Storage instance {} not found", storage_id),
                    };
                    let context = create_error_context("store");
                    let error_report = create_error_report(ErrorCode::KeyNotFound, error_type, context, None);
                    
                    let error_reports = get_error_reports();
                    if let Ok(mut reports) = error_reports.lock() {
                        reports.insert(operation_id, error_report);
                    }
                    
                    return -(operation_id as jlong);
                }
            }
        }
        Err(e) => {
            let error_type = CacheError::ConcurrencyError {
                operation: "get_storage_instance".to_string(),
                details: format!("Failed to acquire storage instances lock: {:?}", e),
            };
            let context = create_error_context("store");
            let error_report = create_error_report(ErrorCode::LockTimeout, error_type, context, None);
            
            let error_reports = get_error_reports();
            if let Ok(mut reports) = error_reports.lock() {
                reports.insert(operation_id, error_report);
            }
            
            return -(operation_id as jlong);
        }
    };
    let result1 = env.get_string(&key);
    // 获取键字符串
    let key_str = match handle_jni_error("get_key_string", result1) {
        Ok(jstr) => jstr.to_string_lossy().to_string(),
        Err(error_report) => {
            let error_reports = get_error_reports();
            if let Ok(mut reports) = error_reports.lock() {
                reports.insert(operation_id, error_report);
            }
            return -(operation_id as jlong);
        }
    };

    // 获取字节数组（安全转换）
    let byte_array = match env.convert_byte_array(&data) {
        Ok(v) => v,
        Err(e) => {
            let error_type = CacheError::JniError {
                operation: "get_byte_array".to_string(),
                details: format!("Failed to convert byte array: {:?}", e),
            };
            let context = create_error_context("store");
            let error_report = create_error_report(ErrorCode::JniByteArrayAccess, error_type, context, None);
            let error_reports = get_error_reports();
            if let Ok(mut reports) = error_reports.lock() {
                reports.insert(operation_id, error_report);
            }
            return -(operation_id as jlong);
        }
    };

    // 存储数据
    match storage_instance.lock() {
        Ok(storage) => {
            match storage.store(&key_str, &byte_array) {
                Ok(_) => operation_id as jlong, // 成功，返回操作ID
                Err(e) => {
                    let error_type = CacheError::StorageError {
                        operation: "store".to_string(),
                        key: Some(key_str),
                        details: format!("Failed to store data: {:?}", e),
                    };
                    let context = create_error_context("store");
                    let error_report = create_error_report(ErrorCode::StorageInitializationFailed, error_type, context, None);
                    
                    let error_reports = get_error_reports();
                    if let Ok(mut reports) = error_reports.lock() {
                        reports.insert(operation_id, error_report);
                    }
                    
                    -(operation_id as jlong)
                }
            }
        }
        Err(e) => {
            let error_type = CacheError::ConcurrencyError {
                operation: "store_data".to_string(),
                details: format!("Failed to acquire storage lock: {:?}", e),
            };
            let context = create_error_context("store");
            let error_report = create_error_report(ErrorCode::LockTimeout, error_type, context, None);
            
            let error_reports = get_error_reports();
            if let Ok(mut reports) = error_reports.lock() {
                reports.insert(operation_id, error_report);
            }
            
            -(operation_id as jlong)
        }
    }
}

