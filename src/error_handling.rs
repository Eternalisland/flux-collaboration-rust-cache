//! # 异常处理与错误诊断模块
//! 
//! 提供完善的错误处理机制，包括：
//! - 详细的错误码和错误信息
//! - 错误上下文跟踪
//! - Java端友好的错误报告
//! - 性能监控和诊断信息

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use serde::{Serialize, Deserialize};

/// 错误类型枚举
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheError {
    /// JNI相关错误
    JniError {
        operation: String,
        details: String,
    },
    /// 文件系统错误
    FileSystemError {
        path: String,
        operation: String,
        details: String,
    },
    /// 内存不足错误
    OutOfMemoryError {
        requested: u64,
        available: u64,
        context: String,
    },
    /// 配置错误
    ConfigurationError {
        parameter: String,
        value: String,
        details: String,
    },
    /// 存储操作错误
    StorageError {
        operation: String,
        key: Option<String>,
        details: String,
    },
    /// 并发访问错误
    ConcurrencyError {
        operation: String,
        details: String,
    },
    /// 数据损坏错误
    DataCorruptionError {
        key: String,
        details: String,
    },
    /// 未知错误
    UnknownError {
        details: String,
    },
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheError::JniError { operation, details } => {
                write!(f, "JNI错误 [操作: {}] {}", operation, details)
            }
            CacheError::FileSystemError { path, operation, details } => {
                write!(f, "文件系统错误 [路径: {}] [操作: {}] {}", path, operation, details)
            }
            CacheError::OutOfMemoryError { requested, available, context } => {
                write!(f, "内存不足 [请求: {}B] [可用: {}B] [上下文: {}]", requested, available, context)
            }
            CacheError::ConfigurationError { parameter, value, details } => {
                write!(f, "配置错误 [参数: {}] [值: {}] {}", parameter, value, details)
            }
            CacheError::StorageError { operation, key, details } => {
                let key_info = key.as_ref().map(|k| format!("[键: {}] ", k)).unwrap_or_default();
                write!(f, "存储错误 [操作: {}] {}{}", operation, key_info, details)
            }
            CacheError::ConcurrencyError { operation, details } => {
                write!(f, "并发访问错误 [操作: {}] {}", operation, details)
            }
            CacheError::DataCorruptionError { key, details } => {
                write!(f, "数据损坏 [键: {}] {}", key, details)
            }
            CacheError::UnknownError { details } => {
                write!(f, "未知错误: {}", details)
            }
        }
    }
}

impl std::error::Error for CacheError {}

/// 错误码枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    // JNI相关错误 (1000-1999)
    JniStringConversion = 1001,
    JniByteArrayAccess = 1002,
    JniMemoryAllocation = 1003,
    JniError = 1004,

    // 文件系统错误 (2000-2999)
    FileNotFound = 2001,
    FilePermissionDenied = 2002,
    FileSystemFull = 2003,
    DirectoryCreationFailed = 2004,
    FileReadError = 2005,
    FileWriteError = 2006,
    FileSystemError = 2007,

    // 内存相关错误 (3000-3999)
    OutOfMemory = 3001,
    MemoryMappingFailed = 3002,
    MemoryThresholdExceeded = 3003,
    
    // 配置错误 (4000-4999)
    InvalidConfiguration = 4001,
    InvalidMemoryThreshold = 4002,
    InvalidPath = 4003,
    InvalidParameter = 4004,
    
    // 存储操作错误 (5000-5999)
    StorageInitializationFailed = 5001,
    KeyNotFound = 5002,
    DataCorruption = 5003,
    StorageFull = 5004,
    
    // 并发访问错误 (6000-6999)
    LockTimeout = 6001,
    ConcurrentModification = 6002,
    
    // 系统错误 (9000-9999)
    SystemError = 9001,
    UnknownError = 9999,
}

impl ErrorCode {
    /// 获取错误码的数值
    pub fn code(&self) -> u32 {
        *self as u32
    }
    
    /// 获取错误码的描述
    pub fn description(&self) -> &'static str {
        match self {
            ErrorCode::JniStringConversion => "JNI字符串转换失败",
            ErrorCode::JniByteArrayAccess => "JNI字节数组访问失败",
            ErrorCode::JniMemoryAllocation => "JNI内存分配失败",
            ErrorCode::JniError => "JNI异常",

            ErrorCode::FileNotFound => "文件不存在",
            ErrorCode::FilePermissionDenied => "文件权限不足",
            ErrorCode::FileSystemFull => "文件系统空间不足",
            ErrorCode::DirectoryCreationFailed => "目录创建失败",
            ErrorCode::FileReadError => "文件读取失败",
            ErrorCode::FileWriteError => "文件写入失败",
            ErrorCode::FileSystemError => "文件系统异常",

            ErrorCode::OutOfMemory => "内存不足",
            ErrorCode::MemoryMappingFailed => "内存映射失败",
            ErrorCode::MemoryThresholdExceeded => "内存阈值超限",
            
            ErrorCode::InvalidConfiguration => "配置无效",
            ErrorCode::InvalidMemoryThreshold => "内存阈值无效",
            ErrorCode::InvalidPath => "路径无效",
            ErrorCode::InvalidParameter => "参数无效",
            
            ErrorCode::StorageInitializationFailed => "存储初始化失败",
            ErrorCode::KeyNotFound => "键不存在",
            ErrorCode::DataCorruption => "数据损坏",
            ErrorCode::StorageFull => "存储空间已满",
            
            ErrorCode::LockTimeout => "锁超时",
            ErrorCode::ConcurrentModification => "并发修改",
            
            ErrorCode::SystemError => "系统错误",
            ErrorCode::UnknownError => "未知错误",
        }
    }
}

/// 错误上下文信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorContext {
    /// 错误发生时间
    pub timestamp: u64,
    /// 线程ID
    pub thread_id: u64,
    /// 操作ID（用于跟踪）
    pub operation_id: u64,
    /// 调用栈信息（简化版）
    pub call_stack: Vec<String>,
    /// 系统信息
    pub system_info: SystemInfo,
}

/// 系统信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// 可用内存（字节）
    pub available_memory: u64,
    /// 磁盘使用率（百分比）
    pub disk_usage_percent: f64,
    /// 文件描述符使用数
    pub file_descriptors_used: u32,
    /// 系统负载
    pub system_load: f64,
}

/// 详细的错误报告
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorReport {
    /// 错误码
    pub error_code: ErrorCode,
    /// 错误类型
    pub error_type: CacheError,
    /// 错误上下文
    pub context: ErrorContext,
    /// 建议的解决方案
    pub suggestions: Vec<String>,
    /// 相关配置信息
    pub config_info: Option<ConfigInfo>,
}

/// 配置信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigInfo {
    /// 内存阈值
    pub memory_threshold: u64,
    /// 磁盘目录
    pub disk_directory: String,
    /// 最大映射数
    pub max_memory_maps: u32,
    /// 清理间隔
    pub cleanup_interval: u64,
}

/// 操作ID生成器
static OPERATION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// 生成新的操作ID
pub fn generate_operation_id() -> u64 {
    OPERATION_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// 获取当前系统信息
pub fn get_system_info() -> SystemInfo {
    SystemInfo {
        available_memory: get_available_memory(),
        disk_usage_percent: get_disk_usage(),
        file_descriptors_used: get_file_descriptor_count(),
        system_load: get_system_load(),
    }
}

/// 获取可用内存（简化实现）
fn get_available_memory() -> u64 {
    // 这里可以集成系统内存信息获取
    // 暂时返回一个估算值
    8 * 1024 * 1024 * 1024 // 8GB
}

/// 获取磁盘使用率（简化实现）
fn get_disk_usage() -> f64 {
    // 这里可以集成磁盘使用率获取
    // 暂时返回一个估算值
    45.0 // 45%
}

/// 获取文件描述符使用数（简化实现）
fn get_file_descriptor_count() -> u32 {
    // 这里可以集成文件描述符计数
    // 暂时返回一个估算值
    128
}

/// 获取系统负载（简化实现）
fn get_system_load() -> f64 {
    // 这里可以集成系统负载获取
    // 暂时返回一个估算值
    1.2
}

/// 创建错误上下文
pub fn create_error_context(operation: &str) -> ErrorContext {
    ErrorContext {
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        thread_id: get_thread_id(),
        operation_id: generate_operation_id(),
        call_stack: vec![operation.to_string()],
        system_info: get_system_info(),
    }
}

/// 获取线程ID（简化实现）
fn get_thread_id() -> u64 {
    // 这里可以获取真实的线程ID
    // 暂时返回一个随机值
    12345
}

/// 创建错误报告
pub fn create_error_report(
    error_code: ErrorCode,
    error_type: CacheError,
    context: ErrorContext,
    config_info: Option<ConfigInfo>,
) -> ErrorReport {
    let suggestions = generate_suggestions(&error_code, &error_type);
    ErrorReport {
        error_code,
        error_type,
        context,
        suggestions,
        config_info,
    }
}

/// 生成解决建议
fn generate_suggestions(error_code: &ErrorCode, error_type: &CacheError) -> Vec<String> {
    let mut suggestions = Vec::new();
    
    match error_code {
        ErrorCode::OutOfMemory | ErrorCode::MemoryThresholdExceeded => {
            suggestions.push("增加内存阈值配置".to_string());
            suggestions.push("减少并发操作数量".to_string());
            suggestions.push("启用更频繁的内存清理".to_string());
        }
        ErrorCode::FileSystemFull | ErrorCode::FilePermissionDenied => {
            suggestions.push("检查磁盘空间和权限".to_string());
            suggestions.push("更换存储目录".to_string());
            suggestions.push("清理旧的缓存文件".to_string());
        }
        ErrorCode::InvalidConfiguration | ErrorCode::InvalidParameter => {
            suggestions.push("检查配置参数的有效性".to_string());
            suggestions.push("参考配置文档重新设置参数".to_string());
        }
        ErrorCode::LockTimeout | ErrorCode::ConcurrentModification => {
            suggestions.push("减少并发访问频率".to_string());
            suggestions.push("增加锁超时时间".to_string());
        }
        _ => {
            suggestions.push("检查系统资源和配置".to_string());
            suggestions.push("查看详细错误日志".to_string());
        }
    }
    
    suggestions
}

/// JNI错误处理辅助函数
pub fn handle_jni_error<T>(
    operation: &str,
    result: Result<T, jni::errors::Error>,
) -> Result<T, ErrorReport> {
    match result {
        Ok(value) => Ok(value),
        Err(e) => {
            // 归一化错误码
            let error_code = map_jni_error_code(&e);

            // 详细错误信息，尽量包含变体与调试串
            let details = format!("variant={:?}, msg={}", e, e);
            let error_type = CacheError::JniError {
                operation: operation.to_string(),
                details,
            };

            // 构造上下文并返回统一的错误报告
            let context = create_error_context(operation);
            Err(create_error_report(error_code, error_type, context, None))
        }
    }
}

/// 可选：发生 JNI 错误时同时向 Java 抛出 RuntimeException，便于 Java 侧直观看到异常
/// 注意：此函数需要可变的 JNIEnv
pub fn handle_jni_error_throw<T>(
    env: &mut jni::JNIEnv,
    operation: &str,
    result: Result<T, jni::errors::Error>,
) -> Result<T, ErrorReport> {
    match result {
        Ok(v) => Ok(v),
        Err(e) => {
            let code = map_jni_error_code(&e);
            let msg = format!("JNI error in {} (code={}): {}", operation, code.code(), e);

            // 尝试抛出 Java 端异常（失败也不影响错误报告返回）
            let _ = env.throw_new("java/lang/RuntimeException", &msg);

            let ctx = create_error_context(operation);
            let ty = CacheError::JniError { operation: operation.to_string(), details: msg };
            Err(create_error_report(code, ty, ctx, None))
        }
    }
}

/// 将 jni::errors::Error 映射为统一的 ErrorCode，便于前后端和监控统一识别
fn map_jni_error_code(err: &jni::errors::Error) -> ErrorCode {
    let _ = err; // 目前统一归类为 JNI 错误，避免对特定版本变体的紧耦合
    ErrorCode::JniError
}

/// 文件系统错误处理辅助函数
pub fn handle_file_error<T>(
    operation: &str,
    path: &str,
    result: Result<T, std::io::Error>,
) -> Result<T, ErrorReport> {
    match result {
        Ok(value) => Ok(value),
        Err(e) => {
            let error_type = CacheError::FileSystemError {
                path: path.to_string(),
                operation: operation.to_string(),
                details: format!("{:?}", e),
            };
            let context = create_error_context(operation);
            let error_code = match e.kind() {
                std::io::ErrorKind::NotFound => ErrorCode::FileNotFound,
                std::io::ErrorKind::PermissionDenied => ErrorCode::FilePermissionDenied,
                std::io::ErrorKind::StorageFull => ErrorCode::FileSystemFull,
                _ => ErrorCode::FileSystemError,
            };
            
            Err(create_error_report(error_code, error_type, context, None))
        }
    }
}

impl From<std::io::Error> for CacheError {
    fn from(err: std::io::Error) -> Self {
        CacheError::FileSystemError {
            path: "unknown".to_string(),
            operation: "unknown".to_string(),
            details: err.to_string(),
        }
    }
}

impl From<jni::errors::Error> for CacheError {
    fn from(err: jni::errors::Error) -> Self {
        CacheError::JniError {
            operation: "unknown".to_string(),
            details: err.to_string(),
        }
    }
}
