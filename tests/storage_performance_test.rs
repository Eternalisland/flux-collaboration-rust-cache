//! Integration tests for the HighPerfMmapStorage performance

use std::path::PathBuf;
use std::time::Instant;
use flux_collaboration_rust_cache::high_perf_mmap_storage::{HighPerfMmapStorage, HighPerfMmapConfig};

/// Test basic write performance
#[test]
fn test_basic_write_performance() {
    let config = HighPerfMmapConfig::default();
    let test_dir = PathBuf::from("/tmp/storage_write_test");
    
    // Clean up previous test data
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).unwrap_or(());
    }
    
    let storage = HighPerfMmapStorage::new(test_dir.clone(), config).expect("Failed to create storage");
    
    const NUM_OPERATIONS: usize = 100;
    const DATA_SIZE: usize = 256; // 256 bytes
    
    let data = vec![42u8; DATA_SIZE];
    let start_time = Instant::now();
    
    for i in 0..NUM_OPERATIONS {
        let key = format!("test_key_{}", i);
        storage.write(&key, &data).expect("Write operation failed");
    }
    
    let elapsed = start_time.elapsed();
    let ops_per_sec = NUM_OPERATIONS as f64 / elapsed.as_secs_f64();
    
    println!("Basic write performance: {:.2} ops/sec", ops_per_sec);
    
    // Clean up
    std::fs::remove_dir_all(&test_dir).unwrap_or(());
    
    assert!(ops_per_sec > 0.0);
}

/// Test basic read performance
#[test]
fn test_basic_read_performance() {
    let config = HighPerfMmapConfig::default();
    let test_dir = PathBuf::from("/tmp/storage_read_test");
    
    // Clean up previous test data
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).unwrap_or(());
    }
    
    let storage = HighPerfMmapStorage::new(test_dir.clone(), config).expect("Failed to create storage");
    
    const NUM_OPERATIONS: usize = 100;
    const DATA_SIZE: usize = 256; // 256 bytes
    
    let data = vec![42u8; DATA_SIZE];
    
    // Pre-populate data
    for i in 0..NUM_OPERATIONS {
        let key = format!("test_key_{}", i);
        storage.write(&key, &data).expect("Pre-population write failed");
    }
    
    let start_time = Instant::now();
    
    let mut successful_reads = 0;
    for i in 0..NUM_OPERATIONS {
        let key = format!("test_key_{}", i);
        match storage.read(&key) {
            Ok(Some(_)) => successful_reads += 1,
            Ok(None) => {}, // Key not found
            Err(_) => {}, // Error occurred
        }
    }
    
    let elapsed = start_time.elapsed();
    let ops_per_sec = NUM_OPERATIONS as f64 / elapsed.as_secs_f64();
    
    println!("Basic read performance: {:.2} ops/sec ({} successful reads)", 
             ops_per_sec, successful_reads);
    
    // Clean up
    std::fs::remove_dir_all(&test_dir).unwrap_or(());
    
    assert!(ops_per_sec > 0.0);
    assert_eq!(successful_reads, NUM_OPERATIONS);
}

/// Test basic delete performance
#[test]
fn test_basic_delete_performance() {
    let config = HighPerfMmapConfig::default();
    let test_dir = PathBuf::from("/tmp/storage_delete_test");
    
    // Clean up previous test data
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).unwrap_or(());
    }
    
    let storage = HighPerfMmapStorage::new(test_dir.clone(), config).expect("Failed to create storage");
    
    const NUM_OPERATIONS: usize = 100;
    const DATA_SIZE: usize = 256; // 256 bytes
    
    let data = vec![42u8; DATA_SIZE];
    
    // Pre-populate data
    for i in 0..NUM_OPERATIONS {
        let key = format!("test_key_{}", i);
        storage.write(&key, &data).expect("Pre-population write failed");
    }
    
    let start_time = Instant::now();
    
    let mut successful_deletes = 0;
    for i in 0..NUM_OPERATIONS {
        let key = format!("test_key_{}", i);
        match storage.delete(&key) {
            Ok(true) => successful_deletes += 1,
            Ok(false) => {}, // Key not found
            Err(_) => {}, // Error occurred
        }
    }
    
    let elapsed = start_time.elapsed();
    let ops_per_sec = NUM_OPERATIONS as f64 / elapsed.as_secs_f64();
    
    println!("Basic delete performance: {:.2} ops/sec ({} successful deletes)", 
             ops_per_sec, successful_deletes);
    
    // Clean up
    std::fs::remove_dir_all(&test_dir).unwrap_or(());
    
    assert!(ops_per_sec > 0.0);
    assert_eq!(successful_deletes, NUM_OPERATIONS);
}

/// Test batch operations performance
#[test]
fn test_batch_operations_performance() {
    let config = HighPerfMmapConfig::default();
    let test_dir = PathBuf::from("/tmp/storage_batch_test");
    
    // Clean up previous test data
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).unwrap_or(());
    }
    
    let storage = HighPerfMmapStorage::new(test_dir.clone(), config).expect("Failed to create storage");
    
    const BATCH_SIZE: usize = 50;
    const NUM_BATCHES: usize = 5;
    const DATA_SIZE: usize = 128; // 128 bytes
    
    let data = vec![42u8; DATA_SIZE];
    
    // Test batch write performance
    let start_time = Instant::now();
    
    // for batch in 0..NUM_BATCHES {
    //     let batch_data: Vec<(String, Vec<u8>, _)> = (0..BATCH_SIZE)
    //         .map(|i| {
    //             let key = format!("batch_{}_key_{}", batch, i);
    //             // let value_type = flux_collaboration_rust_cache::high_perf_mmap_storage::Binary;
    //             (key, data.clone(), value_type)
    //         })
    //         .collect();
    //     
    //     // storage.write(&batch_data).expect("Batch write failed");
    // }
    
    let write_elapsed = start_time.elapsed();
    let total_writes = NUM_BATCHES * BATCH_SIZE;
    let write_ops_per_sec = total_writes as f64 / write_elapsed.as_secs_f64();
    
    // Test batch read performance
    let start_time = Instant::now();
    
    for batch in 0..NUM_BATCHES {
        let keys: Vec<String> = (0..BATCH_SIZE)
            .map(|i| format!("batch_{}_key_{}", batch, i))
            .collect();
        
        let _results = storage.read_batch(&keys).expect("Batch read failed");
    }
    
    let read_elapsed = start_time.elapsed();
    let total_reads = NUM_BATCHES * BATCH_SIZE;
    let read_ops_per_sec = total_reads as f64 / read_elapsed.as_secs_f64();
    
    // Test batch delete performance
    let start_time = Instant::now();
    
    let mut total_deletes = 0;
    for batch in 0..NUM_BATCHES {
        let keys: Vec<String> = (0..BATCH_SIZE)
            .map(|i| format!("batch_{}_key_{}", batch, i))
            .collect();
        for key in keys {
            let deleted_keys = storage.delete(&key).expect("Batch delete failed");

        }

    }
    
    let delete_elapsed = start_time.elapsed();
    let delete_ops_per_sec = total_deletes as f64 / delete_elapsed.as_secs_f64();
    
    println!("Batch operations performance:");
    println!("  Write: {:.2} ops/sec", write_ops_per_sec);
    println!("  Read:  {:.2} ops/sec", read_ops_per_sec);
    println!("  Delete: {:.2} ops/sec", delete_ops_per_sec);
    
    // Clean up
    std::fs::remove_dir_all(&test_dir).unwrap_or(());
    
    assert!(write_ops_per_sec > 0.0);
    assert!(read_ops_per_sec > 0.0);
    assert!(delete_ops_per_sec > 0.0);
}

/// Test memory usage and cache statistics
#[test]
fn test_memory_and_cache_statistics() {
    let config = HighPerfMmapConfig::default();
    let test_dir = PathBuf::from("/tmp/storage_stats_test");
    
    // Clean up previous test data
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).unwrap_or(());
    }
    
    let storage = HighPerfMmapStorage::new(test_dir.clone(), config).expect("Failed to create storage");
    
    const NUM_OPERATIONS: usize = 50;
    const DATA_SIZE: usize = 512; // 512 bytes
    
    let data = vec![42u8; DATA_SIZE];
    
    // Perform some operations
    for i in 0..NUM_OPERATIONS {
        let key = format!("stats_key_{}", i);
        storage.write(&key, &data).expect("Write operation failed");
    }
    
    // Read some data to populate cache
    for i in 0..NUM_OPERATIONS / 2 {
        let key = format!("stats_key_{}", i);
        let _result = storage.read(&key).expect("Read operation failed");
    }
    
    // Get statistics
    let stats = storage.get_stats();
    // let cache_stats = storage.get_cache_stats();
    // let memory_stats = storage.get_memory_stats();
    
    println!("Performance Statistics:");
    println!("  Total writes: {}", stats.total_writes);
    println!("  Total reads: {}", stats.total_reads);
    println!("  L1 cache hits: {}", stats.l1_cache_hits);
    println!("  L1 cache misses: {}", stats.l1_cache_misses);
    println!("  L2 cache hits: {}", stats.l2_cache_hits);
    println!("  L2 cache misses: {}", stats.l2_cache_misses);
    println!("  Average write latency: {} μs", stats.avg_write_latency_us);
    println!("  Average read latency: {} μs", stats.avg_read_latency_us);
    
    // println!("Cache Statistics:");
    // println!("  L1 entry count: {}", cache_stats.l1_entry_count);
    // println!("  L2 entry count: {}", cache_stats.l2_entry_count);
    // println!("  L1 memory usage: {} bytes", cache_stats.l1_memory_usage);
    // println!("  L2 memory usage: {} bytes", cache_stats.l2_memory_usage);
    //
    // println!("Memory Statistics:");
    // println!("  Total memory usage: {} bytes", memory_stats.total_memory_usage);
    // println!("  Memory usage ratio: {:.2}%", memory_stats.memory_usage_ratio * 100.0);
    // println!("  Memory pressure level: {}", memory_stats.memory_pressure_level);
    
    // Clean up
    std::fs::remove_dir_all(&test_dir).unwrap_or(());
    
    // Basic assertions
    assert!(stats.total_writes >= NUM_OPERATIONS as u64);
    assert!(stats.total_reads >= (NUM_OPERATIONS / 2) as u64);
    // assert!(memory_stats.total_memory_usage > 0);
}