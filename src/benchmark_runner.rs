//! Benchmark runner for the high performance MMAP storage system
//! This module provides a simple interface to run performance benchmarks

use std::env;
use std::process;

use crate::performance_tests::{PerformanceTestSuite, TestConfig, run_performance_benchmark};

/// Run benchmarks with custom configuration
fn run_custom_benchmark() {
    let args: Vec<String> = env::args().collect();
    
    let num_operations = if args.len() > 1 {
        args[1].parse::<usize>().unwrap_or(1000)
    } else {
        1000
    };
    
    let data_size = if args.len() > 2 {
        args[2].parse::<usize>().unwrap_or(1024)
    } else {
        1024
    };
    
    let test_dir = if args.len() > 3 {
        args[3].clone()
    } else {
        "/tmp/cache_benchmark".to_string()
    };
    
    println!("Running benchmark with:");
    println!("  Operations: {}", num_operations);
    println!("  Data size: {} bytes", data_size);
    println!("  Test directory: {}", test_dir);
    
    let config = TestConfig {
        num_operations,
        data_size,
        test_dir,
    };
    
    let test_suite = PerformanceTestSuite::new(config);
    let results = test_suite.run_all_tests();
    
    println!("\n=== BENCHMARK RESULTS ===");
    println!("Write Operations:  {:.2} ops/sec (avg {:.2} μs)", 
             results.write_ops_per_sec, results.avg_write_latency_us);
    println!("Read Operations:   {:.2} ops/sec (avg {:.2} μs)", 
             results.read_ops_per_sec, results.avg_read_latency_us);
    println!("Delete Operations: {:.2} ops/sec (avg {:.2} μs)", 
             results.delete_ops_per_sec, results.avg_delete_latency_us);
    
    if let Some(memory_stats) = &results.memory_stats {
        println!("\n=== MEMORY STATISTICS ===");
        println!("Total Memory Usage: {} bytes", memory_stats.total_memory_usage);
        println!("L1 Memory Usage:    {} bytes", memory_stats.l1_memory_usage);
        println!("L2 Memory Usage:    {} bytes", memory_stats.l2_memory_usage);
        println!("Memory Usage Ratio: {:.2}%", memory_stats.memory_usage_ratio * 100.0);
    }
    
    if let Some(cache_hit_rates) = &results.cache_hit_rates {
        println!("\n=== CACHE HIT RATES ===");
        println!("L1 Cache Hit Rate:  {:.2}%", cache_hit_rates.l1_hit_rate);
        println!("L2 Cache Hit Rate:  {:.2}%", cache_hit_rates.l2_hit_rate);
    }
}

fn main() {
    // Check if we're running in a specific mode
    let args: Vec<String> = env::args().collect();
    
    if args.len() > 1 && args[1] == "custom" {
        run_custom_benchmark();
    } else {
        // Run the default benchmark
        run_performance_benchmark();
    }
}