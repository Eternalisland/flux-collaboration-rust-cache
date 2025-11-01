//! Performance tests for read, write, and delete operations in HighPerfMmapStorage
//! This test suite verifies the efficiency of the core operations with detailed metrics

use std::path::PathBuf;
use std::time::{Instant, Duration};
use flux_collaboration_rust_cache::high_perf_mmap_storage::{HighPerfMmapStorage, HighPerfMmapConfig};

/// Performance test configuration
struct PerformanceTestConfig {
    /// Number of operations to perform
    num_operations: usize,
    /// Size of data to write in each operation (bytes)
    data_size: usize,
    /// Test directory
    test_dir: PathBuf,
}

impl Default for PerformanceTestConfig {
    fn default() -> Self {
        Self {
            num_operations: 1000,
            data_size: 1024, // 1KB
            test_dir: PathBuf::from("/tmp/rw_delete_perf_test"),
        }
    }
}

/// Performance metrics for a single operation type
#[derive(Debug, Clone)]
struct OperationMetrics {
    /// Operations per second
    ops_per_sec: f64,
    /// Average latency in microseconds
    avg_latency_us: f64,
    /// Total time elapsed
    total_time: Duration,
    /// Successful operations count
    successful_ops: usize,
    /// Total operations count
    total_ops: usize,
}

/// Comprehensive performance results
#[derive(Debug, Clone)]
struct PerformanceResults {
    /// Write operation metrics
    write_metrics: OperationMetrics,
    /// Read operation metrics
    read_metrics: OperationMetrics,
    /// Delete operation metrics
    delete_metrics: OperationMetrics,
    /// Batch write operation metrics
    batch_write_metrics: OperationMetrics,
    /// Batch read operation metrics
    batch_read_metrics: OperationMetrics,
    /// Batch delete operation metrics
    batch_delete_metrics: OperationMetrics,
}

/// Performance test suite for read, write, and delete operations
struct RwDeletePerformanceTest {
    config: PerformanceTestConfig,
}

impl RwDeletePerformanceTest {
    /// Create a new performance test suite
    pub fn new(config: PerformanceTestConfig) -> Self {
        Self { config }
    }

    /// Create a test storage instance with default configuration
    fn create_test_storage(&self) -> HighPerfMmapStorage {
        let config = HighPerfMmapConfig::default();
        
        // Clean up previous test data
        if self.config.test_dir.exists() {
            std::fs::remove_dir_all(&self.config.test_dir).unwrap_or(());
        }
        
        HighPerfMmapStorage::new(self.config.test_dir.clone(), config)
            .expect("Failed to create test storage")
    }

    /// Test basic write performance
    pub fn test_write_performance(&self) -> OperationMetrics {
        let storage = self.create_test_storage();
        let data = vec![42u8; self.config.data_size];
        
        let start_time = Instant::now();
        let mut total_latency = Duration::new(0, 0);
        let mut successful_ops = 0;
        
        for i in 0..self.config.num_operations {
            let key = format!("write_key_{}", i);
            let op_start = Instant::now();
            
            match storage.write(&key, &data) {
                Ok(_) => {
                    successful_ops += 1;
                    total_latency += op_start.elapsed();
                }
                Err(_) => {
                    total_latency += op_start.elapsed();
                }
            }
        }
        
        let elapsed = start_time.elapsed();
        let ops_per_sec = self.config.num_operations as f64 / elapsed.as_secs_f64();
        let avg_latency_us = total_latency.as_micros() as f64 / self.config.num_operations as f64;
        
        OperationMetrics {
            ops_per_sec,
            avg_latency_us,
            total_time: elapsed,
            successful_ops,
            total_ops: self.config.num_operations,
        }
    }

    /// Test basic read performance
    pub fn test_read_performance(&self) -> OperationMetrics {
        let storage = self.create_test_storage();
        let data = vec![42u8; self.config.data_size];
        
        // Pre-populate data
        for i in 0..self.config.num_operations {
            let key = format!("read_key_{}", i);
            storage.write(&key, &data).expect("Pre-population write failed");
        }
        
        let start_time = Instant::now();
        let mut total_latency = Duration::new(0, 0);
        let mut successful_ops = 0;
        
        for i in 0..self.config.num_operations {
            let key = format!("read_key_{}", i);
            let op_start = Instant::now();
            
            match storage.read(&key) {
                Ok(Some(_)) => {
                    successful_ops += 1;
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
        
        OperationMetrics {
            ops_per_sec,
            avg_latency_us,
            total_time: elapsed,
            successful_ops,
            total_ops: self.config.num_operations,
        }
    }

    /// Test batch read performance (using read_batch)
    pub fn test_batch_read_performance(&self) -> OperationMetrics {
        let storage = self.create_test_storage();
        let data = vec![42u8; self.config.data_size];
        
        // Pre-populate data
        for i in 0..self.config.num_operations {
            let key = format!("batch_read_key_{}", i);
            storage.write(&key, &data).expect("Pre-population write failed");
        }
        
        // Prepare batch keys
        let batch_size = 100;
        let batches: Vec<Vec<String>> = (0..self.config.num_operations)
            .collect::<Vec<_>>()
            .chunks(batch_size)
            .map(|chunk| {
                chunk.iter().map(|&i| format!("batch_read_key_{}", i)).collect()
            })
            .collect();
        
        let start_time = Instant::now();
        let mut total_latency = Duration::new(0, 0);
        let mut successful_ops = 0;
        let total_batch_ops = batches.len() * batch_size;
        
        for batch in &batches {
            let op_start = Instant::now();
            match storage.read_batch(batch) {
                Ok(results) => {
                    successful_ops += results.len();
                    total_latency += op_start.elapsed();
                }
                Err(_) => {
                    // Error occurred, still count the operation
                    total_latency += op_start.elapsed();
                }
            }
        }
        
        let elapsed = start_time.elapsed();
        let ops_per_sec = total_batch_ops as f64 / elapsed.as_secs_f64();
        let avg_latency_us = total_latency.as_micros() as f64 / batches.len() as f64;
        
        OperationMetrics {
            ops_per_sec,
            avg_latency_us,
            total_time: elapsed,
            successful_ops,
            total_ops: total_batch_ops,
        }
    }

    /// Test basic delete performance
    pub fn test_delete_performance(&self) -> OperationMetrics {
        let storage = self.create_test_storage();
        let data = vec![42u8; self.config.data_size];
        
        // Pre-populate data
        for i in 0..self.config.num_operations {
            let key = format!("delete_key_{}", i);
            storage.write(&key, &data).expect("Pre-population write failed");
        }
        
        let start_time = Instant::now();
        let mut total_latency = Duration::new(0, 0);
        let mut successful_ops = 0;
        
        for i in 0..self.config.num_operations {
            let key = format!("delete_key_{}", i);
            let op_start = Instant::now();
            
            match storage.delete(&key) {
                Ok(true) => {
                    successful_ops += 1;
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
        
        OperationMetrics {
            ops_per_sec,
            avg_latency_us,
            total_time: elapsed,
            successful_ops,
            total_ops: self.config.num_operations,
        }
    }

    /// Test batch write performance
    pub fn test_batch_write_performance(&self) -> OperationMetrics {
        let storage = self.create_test_storage();
        let data = vec![42u8; self.config.data_size];

        // Prepare batch data
        let batch_size = 100;
        let batches: Vec<Vec<(String, Vec<u8>)>> = (0..self.config.num_operations)
            .collect::<Vec<_>>()
            .chunks(batch_size)
            .map(|chunk| {
                chunk.iter().map(|&i| {
                    let key = format!("batch_write_key_{}", i);
                    (key, data.clone())
                }).collect()
            })
            .collect();

        let start_time = Instant::now();
        let mut total_latency = Duration::new(0, 0);
        let mut successful_ops = 0;
        let total_batch_ops = batches.len() * batch_size;

        for batch in &batches {
            let op_start = Instant::now();
            let mut batch_success = 0;
            for (key, value) in batch {
                match storage.write(key, value) {
                    Ok(_) => {
                        batch_success += 1;
                    }
                    Err(_) => {}
                }
            }
            successful_ops += batch_success;
            total_latency += op_start.elapsed();
        }

        let elapsed = start_time.elapsed();
        let ops_per_sec = total_batch_ops as f64 / elapsed.as_secs_f64();
        let avg_latency_us = total_latency.as_micros() as f64 / batches.len() as f64;

        OperationMetrics {
            ops_per_sec,
            avg_latency_us,
            total_time: elapsed,
            successful_ops,
            total_ops: total_batch_ops,
        }
    }

    /// Test batch delete performance
    pub fn test_batch_delete_performance(&self) -> OperationMetrics {
        let storage = self.create_test_storage();
        let data = vec![42u8; self.config.data_size];
        
        // Pre-populate data
        for i in 0..self.config.num_operations {
            let key = format!("batch_delete_key_{}", i);
            storage.write(&key, &data).expect("Pre-population write failed");
        }
        
        // Prepare batch keys
        let batch_size = 100;
        let batches: Vec<Vec<String>> = (0..self.config.num_operations)
            .collect::<Vec<_>>()
            .chunks(batch_size)
            .map(|chunk| {
                chunk.iter().map(|&i| format!("batch_delete_key_{}", i)).collect()
            })
            .collect();
        
        let start_time = Instant::now();
        let mut total_latency = Duration::new(0, 0);
        let mut successful_ops = 0;
        let total_batch_ops = batches.len() * batch_size;
        
        for batch in &batches {
            let op_start = Instant::now();
            let mut deleted_count = 0;
            for key in batch {
                match storage.delete(key) {
                    Ok(true) => {
                        deleted_count += 1;
                    }
                    Ok(false) => {}
                    Err(_) => {}
                }
            }
            successful_ops += deleted_count;
            total_latency += op_start.elapsed();
        }
        
        let elapsed = start_time.elapsed();
        let ops_per_sec = total_batch_ops as f64 / elapsed.as_secs_f64();
        let avg_latency_us = total_latency.as_micros() as f64 / batches.len() as f64;
        
        OperationMetrics {
            ops_per_sec,
            avg_latency_us,
            total_time: elapsed,
            successful_ops,
            total_ops: total_batch_ops,
        }
    }

    /// Run all performance tests and return results
    pub fn run_all_tests(&self) -> PerformanceResults {
        println!("Running read/write/delete performance tests...");
        println!("Configuration: {} operations of {} bytes each", 
                 self.config.num_operations, self.config.data_size);
        
        // Test basic write performance
        println!("\nTesting basic write performance...");
        let write_metrics = self.test_write_performance();
        println!("  Write performance: {:.2} ops/sec, avg latency: {:.2} μs ({} successful)",
                 write_metrics.ops_per_sec, write_metrics.avg_latency_us, write_metrics.successful_ops);
        
        // Test batch write performance
        println!("\nTesting batch write performance...");
        let batch_write_metrics = self.test_batch_write_performance();
        println!("  Batch write performance: {:.2} ops/sec, avg latency: {:.2} μs ({} successful)",
                 batch_write_metrics.ops_per_sec, batch_write_metrics.avg_latency_us, batch_write_metrics.successful_ops);
        
        // Test basic read performance
        println!("\nTesting basic read performance...");
        let read_metrics = self.test_read_performance();
        println!("  Read performance: {:.2} ops/sec, avg latency: {:.2} μs ({} successful)",
                 read_metrics.ops_per_sec, read_metrics.avg_latency_us, read_metrics.successful_ops);
        
        // Test batch read performance
        println!("\nTesting batch read performance...");
        let batch_read_metrics = self.test_batch_read_performance();
        println!("  Batch read performance: {:.2} ops/sec, avg latency: {:.2} μs ({} successful)",
                 batch_read_metrics.ops_per_sec, batch_read_metrics.avg_latency_us, batch_read_metrics.successful_ops);
        
        // Test basic delete performance
        println!("\nTesting basic delete performance...");
        let delete_metrics = self.test_delete_performance();
        println!("  Delete performance: {:.2} ops/sec, avg latency: {:.2} μs ({} successful)",
                 delete_metrics.ops_per_sec, delete_metrics.avg_latency_us, delete_metrics.successful_ops);
        
        // Test batch delete performance
        println!("\nTesting batch delete performance...");
        let batch_delete_metrics = self.test_batch_delete_performance();
        println!("  Batch delete performance: {:.2} ops/sec, avg latency: {:.2} μs ({} successful)",
                 batch_delete_metrics.ops_per_sec, batch_delete_metrics.avg_latency_us, batch_delete_metrics.successful_ops);
        
        PerformanceResults {
            write_metrics,
            read_metrics,
            delete_metrics,
            batch_write_metrics,
            batch_read_metrics,
            batch_delete_metrics,
        }
    }
}

/// Print detailed performance report
fn print_performance_report(results: &PerformanceResults) {
    println!("\n{}", "=".repeat(80));
    println!("PERFORMANCE TEST RESULTS");
    println!("{}", "=".repeat(80));
    
    println!("\nBASIC OPERATIONS:");
    println!("  Write:  {:>8.2} ops/sec (avg {:>6.2} μs)", 
             results.write_metrics.ops_per_sec, results.write_metrics.avg_latency_us);
    println!("  Read:   {:>8.2} ops/sec (avg {:>6.2} μs)", 
             results.read_metrics.ops_per_sec, results.read_metrics.avg_latency_us);
    println!("  Delete: {:>8.2} ops/sec (avg {:>6.2} μs)", 
             results.delete_metrics.ops_per_sec, results.delete_metrics.avg_latency_us);
    
    println!("\nBATCH OPERATIONS:");
    println!("  Write:  {:>8.2} ops/sec (avg {:>6.2} μs)", 
             results.batch_write_metrics.ops_per_sec, results.batch_write_metrics.avg_latency_us);
    println!("  Read:   {:>8.2} ops/sec (avg {:>6.2} μs)", 
             results.batch_read_metrics.ops_per_sec, results.batch_read_metrics.avg_latency_us);
    println!("  Delete: {:>8.2} ops/sec (avg {:>6.2} μs)", 
             results.batch_delete_metrics.ops_per_sec, results.batch_delete_metrics.avg_latency_us);
    
    println!("\nSUCCESS RATES:");
    println!("  Write:  {:>3.1}% ({}/{})", 
             results.write_metrics.successful_ops as f64 / results.write_metrics.total_ops as f64 * 100.0,
             results.write_metrics.successful_ops, results.write_metrics.total_ops);
    println!("  Read:   {:>3.1}% ({}/{})", 
             results.read_metrics.successful_ops as f64 / results.read_metrics.total_ops as f64 * 100.0,
             results.read_metrics.successful_ops, results.read_metrics.total_ops);
    println!("  Delete: {:>3.1}% ({}/{})", 
             results.delete_metrics.successful_ops as f64 / results.delete_metrics.total_ops as f64 * 100.0,
             results.delete_metrics.successful_ops, results.delete_metrics.total_ops);
}

/// Run the performance benchmark
#[test]
fn test_read_write_delete_performance() {
    let config = PerformanceTestConfig {
        num_operations: 500, // Reduced for test execution time
        data_size: 512,      // 512 bytes
        test_dir: PathBuf::from("/tmp/rw_delete_perf_test_unit"),
    };
    
    let test_suite = RwDeletePerformanceTest::new(config);
    let results = test_suite.run_all_tests();
    
    print_performance_report(&results);
    
    // Basic assertions to ensure operations are working
    assert!(results.write_metrics.ops_per_sec > 0.0);
    assert!(results.read_metrics.ops_per_sec > 0.0);
    assert!(results.delete_metrics.ops_per_sec > 0.0);
    assert!(results.batch_write_metrics.ops_per_sec > 0.0);
    assert!(results.batch_read_metrics.ops_per_sec > 0.0);
    assert!(results.batch_delete_metrics.ops_per_sec > 0.0);
    
    // Success rate assertions (allowing for some failures in edge cases)
    assert!(results.write_metrics.successful_ops as f64 / results.write_metrics.total_ops as f64 > 0.9);
    assert!(results.read_metrics.successful_ops as f64 / results.read_metrics.total_ops as f64 > 0.9);
    assert!(results.delete_metrics.successful_ops as f64 / results.delete_metrics.total_ops as f64 > 0.9);
}

/// Main function for standalone execution
#[cfg(not(test))]
fn main() {
    let config = PerformanceTestConfig {
        num_operations: 1000,
        data_size: 1024, // 1KB
        test_dir: PathBuf::from("/tmp/rw_delete_perf_test_main"),
    };
    
    let test_suite = RwDeletePerformanceTest::new(config);
    let results = test_suite.run_all_tests();
    
    print_performance_report(&results);
    
    // Clean up
    if test_suite.config.test_dir.exists() {
        std::fs::remove_dir_all(&test_suite.config.test_dir).unwrap_or(());
    }
}