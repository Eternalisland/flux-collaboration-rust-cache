//! Performance tests for the high performance MMAP storage system
//! This module contains benchmarks to verify the efficiency of read, write, and delete operations

use std::path::PathBuf;
use std::time::{Instant, Duration};
use std::collections::HashMap;

use crate::high_perf_mmap_storage::{HighPerfMmapStorage, HighPerfMmapConfig};

/// Test configuration for performance benchmarks
struct TestConfig {
    /// Number of operations to perform
    pub num_operations: usize,
    /// Size of data to write in each operation (bytes)
    pub data_size: usize,
    /// Directory for test storage
    pub test_dir: String,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            num_operations: 1000,
            data_size: 1024, // 1KB
            test_dir: "/tmp/cache_perf_test".to_string(),
        }
    }
}

/// Performance test results
#[derive(Debug, Clone)]
pub struct PerformanceResults {
    /// Write operations per second
    pub write_ops_per_sec: f64,
    /// Read operations per second
    pub read_ops_per_sec: f64,
    /// Delete operations per second
    pub delete_ops_per_sec: f64,
    /// Average write latency (microseconds)
    pub avg_write_latency_us: f64,
    /// Average read latency (microseconds)
    pub avg_read_latency_us: f64,
    /// Average delete latency (microseconds)
    pub avg_delete_latency_us: f64,
    /// Memory usage statistics
    pub memory_stats: Option<MemoryStats>,
    /// Cache hit rates
    pub cache_hit_rates: Option<CacheHitRates>,
}

/// Memory usage statistics
#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub total_memory_usage: u64,
    pub hot_cache_memory_usage: u64,
    pub memory_usage_ratio: f64,
}

/// Cache hit rates
#[derive(Debug, Clone)]
pub struct CacheHitRates {
    pub hot_cache_hit_rate: f64,
}

/// Performance test suite for HighPerfMmapStorage
pub struct PerformanceTestSuite {
    config: TestConfig,
}

impl PerformanceTestSuite {
    /// Create a new performance test suite
    pub fn new(config: TestConfig) -> Self {
        Self { config }
    }

    /// Create a test storage instance with default configuration
    fn create_test_storage(&self) -> HighPerfMmapStorage {
        let config = HighPerfMmapConfig::default();
        let disk_dir = PathBuf::from(&self.config.test_dir);
        
        // Clean up previous test data
        if disk_dir.exists() {
            std::fs::remove_dir_all(&disk_dir).unwrap_or(());
        }
        
        HighPerfMmapStorage::new(disk_dir, config).expect("Failed to create test storage")
    }

    /// Test write performance
    pub fn test_write_performance(&self) -> (f64, f64) {
        let storage = self.create_test_storage();
        let data = vec![0u8; self.config.data_size];
        
        let start_time = Instant::now();
        let mut total_latency = Duration::new(0, 0);
        
        for i in 0..self.config.num_operations {
            let key = format!("key_{}", i);
            let op_start = Instant::now();
            storage.write(&key, &data).expect("Write operation failed");
            total_latency += op_start.elapsed();
        }
        
        let elapsed = start_time.elapsed();
        let ops_per_sec = self.config.num_operations as f64 / elapsed.as_secs_f64();
        let avg_latency_us = total_latency.as_micros() as f64 / self.config.num_operations as f64;
        
        (ops_per_sec, avg_latency_us)
    }

    /// Test read performance
    pub fn test_read_performance(&self) -> (f64, f64) {
        let storage = self.create_test_storage();
        let data = vec![0u8; self.config.data_size];
        
        // Pre-populate data
        for i in 0..self.config.num_operations {
            let key = format!("read_key_{}", i);
            storage.write(&key, &data).expect("Pre-population write failed");
        }
        
        let start_time = Instant::now();
        let mut total_latency = Duration::new(0, 0);
        let mut successful_reads = 0;
        
        for i in 0..self.config.num_operations {
            let key = format!("read_key_{}", i);
            let op_start = Instant::now();
            match storage.read(&key) {
                Ok(Some(_)) => {
                    successful_reads += 1;
                    total_latency += op_start.elapsed();
                }
                Ok(None) => {
                    // Key not found, still count the operation
                    total_latency += op_start.elapsed();
                }
                Err(_) => {
                    // Error occurred, still count the operation
                    total_latency += op_start.elapsed();
                }
            }
        }
        
        let elapsed = start_time.elapsed();
        let ops_per_sec = self.config.num_operations as f64 / elapsed.as_secs_f64();
        let avg_latency_us = total_latency.as_micros() as f64 / self.config.num_operations as f64;
        
        println!("Successful reads: {}/{}", successful_reads, self.config.num_operations);
        (ops_per_sec, avg_latency_us)
    }

    /// Test delete performance
    pub fn test_delete_performance(&self) -> (f64, f64) {
        let storage = self.create_test_storage();
        let data = vec![0u8; self.config.data_size];
        
        // Pre-populate data
        for i in 0..self.config.num_operations {
            let key = format!("delete_key_{}", i);
            storage.write(&key, &data).expect("Pre-population write failed");
        }
        
        let start_time = Instant::now();
        let mut total_latency = Duration::new(0, 0);
        let mut successful_deletes = 0;
        
        for i in 0..self.config.num_operations {
            let key = format!("delete_key_{}", i);
            let op_start = Instant::now();
            match storage.delete(&key) {
                Ok(true) => {
                    successful_deletes += 1;
                    total_latency += op_start.elapsed();
                }
                Ok(false) => {
                    // Key not found, still count the operation
                    total_latency += op_start.elapsed();
                }
                Err(_) => {
                    // Error occurred, still count the operation
                    total_latency += op_start.elapsed();
                }
            }
        }
        
        let elapsed = start_time.elapsed();
        let ops_per_sec = self.config.num_operations as f64 / elapsed.as_secs_f64();
        let avg_latency_us = total_latency.as_micros() as f64 / self.config.num_operations as f64;
        
        println!("Successful deletes: {}/{}", successful_deletes, self.config.num_operations);
        (ops_per_sec, avg_latency_us)
    }

    /// Test lazy delete performance
    pub fn test_lazy_delete_performance(&self) -> (f64, f64) {
        let storage = self.create_test_storage();
        let data = vec![0u8; self.config.data_size];
        
        // Pre-populate data
        for i in 0..self.config.num_operations {
            let key = format!("lazy_delete_key_{}", i);
            storage.write(&key, &data).expect("Pre-population write failed");
        }
        
        let start_time = Instant::now();
        let mut total_latency = Duration::new(0, 0);
        let mut successful_deletes = 0;
        
        for i in 0..self.config.num_operations {
            let key = format!("lazy_delete_key_{}", i);
            let op_start = Instant::now();
            match storage.delete_lazy(&key) {
                Ok(true) => {
                    successful_deletes += 1;
                    total_latency += op_start.elapsed();
                }
                Ok(false) => {
                    // Key not found, still count the operation
                    total_latency += op_start.elapsed();
                }
                Err(_) => {
                    // Error occurred, still count the operation
                    total_latency += op_start.elapsed();
                }
            }
        }
        
        let elapsed = start_time.elapsed();
        let ops_per_sec = self.config.num_operations as f64 / elapsed.as_secs_f64();
        let avg_latency_us = total_latency.as_micros() as f64 / self.config.num_operations as f64;
        
        println!("Successful lazy deletes: {}/{}", successful_deletes, self.config.num_operations);
        (ops_per_sec, avg_latency_us)
    }

    /// Run all performance tests and return results
    pub fn run_all_tests(&self) -> PerformanceResults {
        println!("Running performance tests with {} operations of {} bytes each", 
                 self.config.num_operations, self.config.data_size);
        
        // Test write performance
        println!("Testing write performance...");
        let (write_ops_per_sec, avg_write_latency_us) = self.test_write_performance();
        println!("Write performance: {:.2} ops/sec, avg latency: {:.2} μs", 
                 write_ops_per_sec, avg_write_latency_us);
        
        // Test read performance
        println!("Testing read performance...");
        let (read_ops_per_sec, avg_read_latency_us) = self.test_read_performance();
        println!("Read performance: {:.2} ops/sec, avg latency: {:.2} μs", 
                 read_ops_per_sec, avg_read_latency_us);
        
        // Test delete performance
        println!("Testing delete performance...");
        let (delete_ops_per_sec, avg_delete_latency_us) = self.test_delete_performance();
        println!("Delete performance: {:.2} ops/sec, avg latency: {:.2} μs", 
                 delete_ops_per_sec, avg_delete_latency_us);
        
        // Test lazy delete performance
        println!("Testing lazy delete performance...");
        let (lazy_delete_ops_per_sec, avg_lazy_delete_latency_us) = self.test_lazy_delete_performance();
        println!("Lazy delete performance: {:.2} ops/sec, avg latency: {:.2} μs", 
                 lazy_delete_ops_per_sec, avg_lazy_delete_latency_us);
        
        // Get final storage statistics
        let storage = self.create_test_storage();
        let stats = storage.get_stats();
        PerformanceResults {
            write_ops_per_sec,
            read_ops_per_sec,
            delete_ops_per_sec,
            avg_write_latency_us,
            avg_read_latency_us,
            avg_delete_latency_us,
            memory_stats: None,
            cache_hit_rates: None,
        }
    }
}

/// Run a comprehensive performance benchmark
pub fn run_performance_benchmark() {
    let config = TestConfig {
        num_operations: 1000,
        data_size: 1024, // 1KB
        test_dir: "/tmp/cache_perf_test".to_string(),
    };
    
    let test_suite = PerformanceTestSuite::new(config);
    let results = test_suite.run_all_tests();
    
    println!("\n=== PERFORMANCE BENCHMARK RESULTS ===");
    println!("Write Operations:  {:.2} ops/sec (avg {:.2} μs)", 
             results.write_ops_per_sec, results.avg_write_latency_us);
    println!("Read Operations:   {:.2} ops/sec (avg {:.2} μs)", 
             results.read_ops_per_sec, results.avg_read_latency_us);
    println!("Delete Operations: {:.2} ops/sec (avg {:.2} μs)", 
             results.delete_ops_per_sec, results.avg_delete_latency_us);
    
    if let Some(cache_hit_rates) = &results.cache_hit_rates {
        println!("\n=== CACHE HIT RATES ===");
        println!("Hot Cache Hit Rate:  {:.2}%", cache_hit_rates.hot_cache_hit_rate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_performance() {
        let config = TestConfig {
            num_operations: 100,
            data_size: 256,
            test_dir: "/tmp/cache_test_basic".to_string(),
        };
        
        let test_suite = PerformanceTestSuite::new(config);
        let results = test_suite.run_all_tests();
        
        // Basic sanity checks
        assert!(results.write_ops_per_sec > 0.0);
        assert!(results.read_ops_per_sec > 0.0);
        assert!(results.delete_ops_per_sec > 0.0);
        assert!(results.avg_write_latency_us > 0.0);
        assert!(results.avg_read_latency_us > 0.0);
        assert!(results.avg_delete_latency_us > 0.0);
    }
}

/// Example usage of the performance test suite
#[cfg(not(test))]
fn main() {
    run_performance_benchmark();
}