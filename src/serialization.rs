//! # 高性能序列化模块
//! 
//! 提供多种序列化方案，支持 Java 和 Rust 之间的高效数据传输
//! 
//! ## 支持的序列化格式
//! - MessagePack: 二进制格式，比 JSON 快 2-3 倍
//! - JSON: 文本格式，兼容性好但性能较低
//! - Base64: 用于二进制数据的文本传输

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 序列化格式枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// MessagePack 二进制格式（推荐）
    MessagePack,
    /// JSON 文本格式（兼容性好）
    Json,
}

/// 缓存操作结果
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheResult<T> {
    /// 操作是否成功
    pub success: bool,
    /// 结果数据
    pub data: Option<T>,
    /// 错误信息
    pub error: Option<String>,
}

impl<T> CacheResult<T> {
    /// 创建成功结果
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    /// 创建失败结果
    pub fn error(error: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(error),
        }
    }
}

/// 缓存指标信息
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheMetrics {
    pub cache_id: u64,
    pub entries: u64,
    pub max_capacity: Option<u64>,
    pub time_to_live_secs: Option<u64>,
    pub time_to_idle_secs: Option<u64>,
    pub disk_entries: u64,
    pub disk_bytes: u64,
    pub leased_entries: u64,
    pub expired_entries: u64,
}

/// 高性能序列化器
pub struct HighPerformanceSerializer {
    pub format: SerializationFormat,
}

impl HighPerformanceSerializer {
    /// 创建新的序列化器
    pub fn new(format: SerializationFormat) -> Self {
        Self { format }
    }

    /// 创建 MessagePack 序列化器（推荐）
    pub fn messagepack() -> Self {
        Self::new(SerializationFormat::MessagePack)
    }

    /// 创建 JSON 序列化器
    pub fn json() -> Self {
        Self::new(SerializationFormat::Json)
    }

    /// 序列化数据
    pub fn serialize<T: Serialize>(&self, data: &T) -> Result<Vec<u8>, String> {
        match self.format {
            SerializationFormat::MessagePack => {
                rmp_serde::to_vec(data).map_err(|e| format!("MessagePack serialization failed: {}", e))
            }
            SerializationFormat::Json => {
                serde_json::to_vec(data).map_err(|e| format!("JSON serialization failed: {}", e))
            }
        }
    }

    /// 反序列化数据
    pub fn deserialize<T: for<'de> Deserialize<'de>>(&self, data: &[u8]) -> Result<T, String> {
        match self.format {
            SerializationFormat::MessagePack => {
                rmp_serde::from_slice(data).map_err(|e| format!("MessagePack deserialization failed: {}", e))
            }
            SerializationFormat::Json => {
                serde_json::from_slice(data).map_err(|e| format!("JSON deserialization failed: {}", e))
            }
        }
    }

    /// 序列化为 Base64 字符串（用于 JNI 传输）
    pub fn serialize_to_base64<T: Serialize>(&self, data: &T) -> Result<String, String> {
        let bytes = self.serialize(data)?;
        Ok(base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes))
    }

    /// 从 Base64 字符串反序列化
    pub fn deserialize_from_base64<T: for<'de> Deserialize<'de>>(&self, base64_str: &str) -> Result<T, String> {
        let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, base64_str).map_err(|e| format!("Base64 decode failed: {}", e))?;
        self.deserialize(&bytes)
    }
}

/// 缓存操作请求
#[derive(Debug, Serialize, Deserialize)]
pub enum CacheRequest {
    /// 创建缓存
    CreateCache {
        max_capacity: u64,
        disk_threshold_bytes: usize,
    },
    /// 添加数据
    Put {
        cache_id: u64,
        key: String,
        value: String,
    },
    /// 获取数据
    Get {
        cache_id: u64,
        key: String,
    },
    /// 删除数据
    Remove {
        cache_id: u64,
        key: String,
    },
    /// 清空缓存
    Clear {
        cache_id: u64,
    },
    /// 获取指标
    GetMetrics {
        cache_id: u64,
    },
    /// 销毁缓存
    DestroyCache {
        cache_id: u64,
    },
    /// 续租
    RenewLease {
        cache_id: u64,
        key: String,
        additional_duration_secs: u64,
    },
    /// 强制删除
    ForceRemove {
        cache_id: u64,
        key: String,
    },
}

/// 缓存操作响应
#[derive(Debug, Serialize, Deserialize)]
pub enum CacheResponse {
    /// 创建缓存响应
    CreateCache(CacheResult<u64>),
    /// 添加数据响应
    Put(CacheResult<bool>),
    /// 获取数据响应
    Get(CacheResult<String>),
    /// 删除数据响应
    Remove(CacheResult<bool>),
    /// 清空缓存响应
    Clear(CacheResult<bool>),
    /// 获取指标响应
    GetMetrics(CacheResult<CacheMetrics>),
    /// 销毁缓存响应
    DestroyCache(CacheResult<bool>),
    /// 续租响应
    RenewLease(CacheResult<bool>),
    /// 强制删除响应
    ForceRemove(CacheResult<bool>),
}

/// 批量操作请求
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchRequest {
    /// 操作列表
    pub operations: Vec<CacheRequest>,
}

/// 批量操作响应
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchResponse {
    /// 响应列表
    pub responses: Vec<CacheResponse>,
}

/// 性能测试结果
#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceTestResult {
    /// 序列化格式
    pub format: String,
    /// 数据大小（字节）
    pub data_size: usize,
    /// 序列化时间（微秒）
    pub serialize_time_us: u64,
    /// 反序列化时间（微秒）
    pub deserialize_time_us: u64,
    /// 序列化后大小（字节）
    pub serialized_size: usize,
    /// 压缩比
    pub compression_ratio: f64,
}

/// 性能测试工具
pub struct PerformanceTester;

impl PerformanceTester {
    /// 测试序列化性能
    pub fn test_serialization_performance<T: Serialize + for<'de> Deserialize<'de> + Clone>(
        data: &T,
        iterations: usize,
    ) -> Vec<PerformanceTestResult> {
        let mut results = Vec::new();

        // 测试 MessagePack
        let msgpack_serializer = HighPerformanceSerializer::messagepack();
        let msgpack_result = Self::test_format(&msgpack_serializer, data, iterations, "MessagePack");
        results.push(msgpack_result);

        // 测试 JSON
        let json_serializer = HighPerformanceSerializer::json();
        let json_result = Self::test_format(&json_serializer, data, iterations, "JSON");
        results.push(json_result);

        results
    }

    fn test_format<T: Serialize + for<'de> Deserialize<'de> + Clone>(
        serializer: &HighPerformanceSerializer,
        data: &T,
        iterations: usize,
        format_name: &str,
    ) -> PerformanceTestResult {
        use std::time::Instant;

        // 计算原始数据大小
        let data_size = serde_json::to_vec(data).unwrap_or_default().len();

        // 测试序列化性能
        let start = Instant::now();
        let mut serialized_data = Vec::new();
        for _ in 0..iterations {
            if let Ok(bytes) = serializer.serialize(data) {
                serialized_data = bytes;
            }
        }
        let serialize_time = start.elapsed();

        // 测试反序列化性能
        let start = Instant::now();
        for _ in 0..iterations {
            let _: T = serializer.deserialize(&serialized_data).unwrap_or_else(|_| data.clone());
        }
        let deserialize_time = start.elapsed();

        PerformanceTestResult {
            format: format_name.to_string(),
            data_size,
            serialize_time_us: serialize_time.as_micros() as u64 / iterations as u64,
            deserialize_time_us: deserialize_time.as_micros() as u64 / iterations as u64,
            serialized_size: serialized_data.len(),
            compression_ratio: serialized_data.len() as f64 / data_size as f64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_messagepack_performance() {
        let test_data = HashMap::from([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
            ("key3".to_string(), "value3".to_string()),
        ]);

        let serializer = HighPerformanceSerializer::messagepack();
        
        // 测试序列化
        let serialized = serializer.serialize(&test_data).unwrap();
        assert!(!serialized.is_empty());

        // 测试反序列化
        let deserialized: HashMap<String, String> = serializer.deserialize(&serialized).unwrap();
        assert_eq!(deserialized, test_data);

        // 测试 Base64 传输
        let base64_str = serializer.serialize_to_base64(&test_data).unwrap();
        let from_base64: HashMap<String, String> = serializer.deserialize_from_base64(&base64_str).unwrap();
        assert_eq!(from_base64, test_data);
    }

    #[test]
    fn test_performance_comparison() {
        let test_data = HashMap::from([
            ("user_id".to_string(), "12345".to_string()),
            ("username".to_string(), "alice".to_string()),
            ("email".to_string(), "alice@example.com".to_string()),
            ("created_at".to_string(), "2024-01-01T00:00:00Z".to_string()),
        ]);

        let results = PerformanceTester::test_serialization_performance(&test_data, 1000);
        
        for result in results {
            println!("Format: {}", result.format);
            println!("  Data size: {} bytes", result.data_size);
            println!("  Serialized size: {} bytes", result.serialized_size);
            println!("  Compression ratio: {:.2}", result.compression_ratio);
            println!("  Serialize time: {} μs", result.serialize_time_us);
            println!("  Deserialize time: {} μs", result.deserialize_time_us);
        }
    }
}
