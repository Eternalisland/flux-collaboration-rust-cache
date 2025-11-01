use flux_collaboration_rust_cache::mmap_storage::{MmapDiskStorage, MmapConfig};
use flux_collaboration_rust_cache::disk_storage::{OptimizedDiskStorage, StorageStrategy};
use std::time::Instant;

/// 磁盘存储优化示例
/// 
/// 演示各种磁盘存储优化技术：
/// 1. MMAP 内存映射
/// 2. 预分配文件池
/// 3. 批量写入
/// 4. 压缩存储
/// 5. 性能对比测试
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 磁盘存储优化示例 ===");
    
    // 创建临时目录
    let temp_dir = std::env::temp_dir().join("flux_cache_optimization");
    std::fs::create_dir_all(&temp_dir)?;
    
    println!("临时目录: {:?}", temp_dir);
    
    // 1. 测试传统存储 vs MMAP 存储
    test_traditional_vs_mmap(&temp_dir)?;
    
    // 2. 测试批量写入优化
    test_batch_write_optimization(&temp_dir)?;
    
    // 3. 测试压缩存储
    test_compression_optimization(&temp_dir)?;
    
    // 4. 测试文件池优化
    test_file_pool_optimization(&temp_dir)?;
    
    // 5. 综合性能测试
    test_comprehensive_performance(&temp_dir)?;
    
    // 清理临时文件
    std::fs::remove_dir_all(&temp_dir)?;
    
    println!("所有测试完成！");
    Ok(())
}

/// 测试传统存储 vs MMAP 存储
fn test_traditional_vs_mmap(temp_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 传统存储 vs MMAP 存储性能测试 ===");
    
    let test_data = generate_test_data(1024 * 1024); // 1MB 测试数据
    let num_operations = 1000;
    
    // 测试传统存储
    let traditional_dir = temp_dir.join("traditional");
    std::fs::create_dir_all(&traditional_dir)?;
    
    let traditional_storage = OptimizedDiskStorage::new(
        traditional_dir.clone(),
        StorageStrategy::Traditional,
    )?;
    
    let mut written_paths = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("traditional_{}", i);
        let path = traditional_storage.write(&key, &test_data)?;
        written_paths.push(path);
    }
    let traditional_write_time = start_time.elapsed();
    
    let start_time = Instant::now();
    for path in &written_paths {
        let _ = traditional_storage.read(path)?;
    }
    let traditional_read_time = start_time.elapsed();
    
    // 测试 MMAP 存储
    let mmap_dir = temp_dir.join("mmap");
    std::fs::create_dir_all(&mmap_dir)?;
    
    let mmap_config = MmapConfig {
        initial_file_size: 100 * 1024 * 1024, // 100MB
        growth_step: 50 * 1024 * 1024,        // 50MB
        max_file_size: 1024 * 1024 * 1024,    // 1GB
        enable_compression: false,
        cache_size_limit: 200 * 1024 * 1024,  // 200MB
        enable_async_io: true,
    };
    
    let mmap_storage = MmapDiskStorage::new(mmap_dir, mmap_config)?;
    
    let mut mmap_keys = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("mmap_{}", i);
        mmap_storage.write(&key, &test_data)?;
        mmap_keys.push(key);
    }
    let mmap_write_time = start_time.elapsed();
    
    let start_time = Instant::now();
    for key in &mmap_keys {
        let _ = mmap_storage.read(key)?;
    }
    let mmap_read_time = start_time.elapsed();
    
    // 输出结果
    println!("传统存储 - 写入时间: {:?}, 读取时间: {:?}", traditional_write_time, traditional_read_time);
    println!("MMAP 存储 - 写入时间: {:?}, 读取时间: {:?}", mmap_write_time, mmap_read_time);
    
    let write_speedup = traditional_write_time.as_micros() as f64 / mmap_write_time.as_micros() as f64;
    let read_speedup = traditional_read_time.as_micros() as f64 / mmap_read_time.as_micros() as f64;
    
    println!("MMAP 写入性能提升: {:.2}x", write_speedup);
    println!("MMAP 读取性能提升: {:.2}x", read_speedup);
    
    // 显示统计信息
    let mmap_stats = mmap_storage.get_stats();
    println!("MMAP 统计信息:");
    println!("  总写入次数: {}", mmap_stats.total_writes);
    println!("  总读取次数: {}", mmap_stats.total_reads);
    println!("  缓存命中率: {:.2}%", 
        mmap_stats.cache_hits as f64 / (mmap_stats.cache_hits + mmap_stats.cache_misses) as f64 * 100.0);
    println!("  平均写入延迟: {}μs", mmap_stats.avg_write_latency_us);
    println!("  平均读取延迟: {}μs", mmap_stats.avg_read_latency_us);
    
    Ok(())
}

/// 测试批量写入优化
fn test_batch_write_optimization(temp_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 批量写入优化测试 ===");
    
    let test_data = generate_test_data(1024); // 1KB 测试数据
    let num_operations = 10000;
    
    // 测试传统写入
    let traditional_dir = temp_dir.join("batch_traditional");
    std::fs::create_dir_all(&traditional_dir)?;
    
    let traditional_storage = OptimizedDiskStorage::new(
        traditional_dir.clone(),
        StorageStrategy::Traditional,
    )?;
    
    let mut traditional_paths = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("batch_traditional_{}", i);
        let path = traditional_storage.write(&key, &test_data)?;
        traditional_paths.push(path);
    }
    let traditional_time = start_time.elapsed();
    
    // 测试批量写入
    let batch_dir = temp_dir.join("batch_write");
    std::fs::create_dir_all(&batch_dir)?;
    
    let batch_storage = OptimizedDiskStorage::new(
        batch_dir.clone(),
        StorageStrategy::BatchWrite,
    )?;
    
    let mut batch_paths = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("batch_{}", i);
        let path = batch_storage.write(&key, &test_data)?;
        batch_paths.push(path);
    }
    // 刷新缓冲区
    batch_storage.flush()?;
    let batch_time = start_time.elapsed();
    
    println!("传统写入时间: {:?}", traditional_time);
    println!("批量写入时间: {:?}", batch_time);
    
    let speedup = traditional_time.as_micros() as f64 / batch_time.as_micros() as f64;
    println!("批量写入性能提升: {:.2}x", speedup);
    
    Ok(())
}

/// 测试压缩存储优化
fn test_compression_optimization(temp_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 压缩存储优化测试 ===");
    
    // 生成可压缩的测试数据（重复模式）
    let test_data = generate_compressible_data(1024 * 1024); // 1MB 可压缩数据
    let num_operations = 100;
    
    // 测试无压缩存储
    let uncompressed_dir = temp_dir.join("uncompressed");
    std::fs::create_dir_all(&uncompressed_dir)?;
    
    let uncompressed_storage = OptimizedDiskStorage::new(
        uncompressed_dir.clone(),
        StorageStrategy::Traditional,
    )?;
    
    let mut uncompressed_paths = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("uncompressed_{}", i);
        let path = uncompressed_storage.write(&key, &test_data)?;
        uncompressed_paths.push(path);
    }
    let uncompressed_time = start_time.elapsed();
    
    // 测试压缩存储
    let compressed_dir = temp_dir.join("compressed");
    std::fs::create_dir_all(&compressed_dir)?;
    
    let compressed_storage = OptimizedDiskStorage::new(
        compressed_dir.clone(),
        StorageStrategy::Compressed,
    )?;
    
    let mut compressed_paths = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("compressed_{}", i);
        let path = compressed_storage.write(&key, &test_data)?;
        compressed_paths.push(path);
    }
    let compressed_time = start_time.elapsed();
    
    // 计算磁盘使用量
    let uncompressed_size = calculate_directory_size(&uncompressed_dir)?;
    let compressed_size = calculate_directory_size(&compressed_dir)?;
    
    println!("无压缩存储时间: {:?}", uncompressed_time);
    println!("压缩存储时间: {:?}", compressed_time);
    println!("无压缩磁盘使用: {} bytes", uncompressed_size);
    println!("压缩磁盘使用: {} bytes", compressed_size);
    
    let compression_ratio = uncompressed_size as f64 / compressed_size as f64;
    let time_overhead = compressed_time.as_micros() as f64 / uncompressed_time.as_micros() as f64;
    
    println!("压缩比: {:.2}x", compression_ratio);
    println!("时间开销: {:.2}x", time_overhead);
    
    Ok(())
}

/// 测试文件池优化
fn test_file_pool_optimization(temp_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 文件池优化测试 ===");
    
    let test_data = generate_test_data(1024); // 1KB 测试数据
    let num_operations = 1000;
    
    // 测试传统文件创建
    let traditional_dir = temp_dir.join("pool_traditional");
    std::fs::create_dir_all(&traditional_dir)?;
    
    let traditional_storage = OptimizedDiskStorage::new(
        traditional_dir.clone(),
        StorageStrategy::Traditional,
    )?;
    
    let mut traditional_paths = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("pool_traditional_{}", i);
        let path = traditional_storage.write(&key, &test_data)?;
        traditional_paths.push(path);
    }
    let traditional_time = start_time.elapsed();
    
    // 测试文件池
    let pool_dir = temp_dir.join("file_pool");
    std::fs::create_dir_all(&pool_dir)?;
    
    let pool_storage = OptimizedDiskStorage::new(
        pool_dir.clone(),
        StorageStrategy::FilePool,
    )?;
    
    let mut pool_paths = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("pool_{}", i);
        let path = pool_storage.write(&key, &test_data)?;
        pool_paths.push(path);
    }
    let pool_time = start_time.elapsed();
    
    println!("传统文件创建时间: {:?}", traditional_time);
    println!("文件池时间: {:?}", pool_time);
    
    let speedup = traditional_time.as_micros() as f64 / pool_time.as_micros() as f64;
    println!("文件池性能提升: {:.2}x", speedup);
    
    Ok(())
}

/// 综合性能测试
fn test_comprehensive_performance(temp_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 综合性能测试 ===");
    
    let test_data = generate_test_data(1024 * 100); // 100KB 测试数据
    let num_operations = 500;
    
    // 测试各种策略
    let strategies = vec![
        ("传统存储", StorageStrategy::Traditional),
        ("MMAP 存储", StorageStrategy::MemoryMapped),
        ("批量写入", StorageStrategy::BatchWrite),
        ("文件池", StorageStrategy::FilePool),
        ("压缩存储", StorageStrategy::Compressed),
    ];
    
    let mut results = Vec::new();
    
    for (name, strategy) in strategies {
        let strategy_dir = temp_dir.join(name.replace(" ", "_"));
        std::fs::create_dir_all(&strategy_dir)?;
        
        let storage = OptimizedDiskStorage::new(strategy_dir.clone(), strategy)?;
        
        // 写入测试
        let mut written_paths = Vec::new();
        let start_time = Instant::now();
        for i in 0..num_operations {
            let key = format!("{}_{}", name, i);
            let path = storage.write(&key, &test_data)?;
            written_paths.push(path);
        }
        let write_time = start_time.elapsed();
        
        // 读取测试
        let start_time = Instant::now();
        for path in &written_paths {
            let _ = storage.read(path)?;
        }
        let read_time = start_time.elapsed();
        
        // 计算磁盘使用量
        let disk_usage = calculate_directory_size(&strategy_dir)?;
        
        results.push((name, write_time, read_time, disk_usage));
    }
    
    // 输出结果
    println!("策略名称\t\t写入时间\t\t读取时间\t\t磁盘使用");
    println!("{}", "-".repeat(80));
    
    for (name, write_time, read_time, disk_usage) in &results {
        println!("{}\t\t{:?}\t\t{:?}\t\t{} bytes", 
            name, write_time, read_time, disk_usage);
    }
    
    // 找出最佳策略
    let best_write = results.iter().min_by_key(|(_, write_time, _, _)| write_time).unwrap();
    let best_read = results.iter().min_by_key(|(_, _, read_time, _)| read_time).unwrap();
    let best_disk = results.iter().min_by_key(|(_, _, _, disk_usage)| disk_usage).unwrap();
    
    println!("\n最佳策略:");
    println!("  最快写入: {} ({:?})", best_write.0, best_write.1);
    println!("  最快读取: {} ({:?})", best_read.0, best_read.1);
    println!("  最少磁盘: {} ({} bytes)", best_disk.0, best_disk.3);
    
    Ok(())
}

/// 生成测试数据
fn generate_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// 生成可压缩的测试数据
fn generate_compressible_data(size: usize) -> Vec<u8> {
    let pattern = b"Hello, World! This is a compressible pattern. ".repeat(100);
    let mut data = Vec::with_capacity(size);
    
    while data.len() < size {
        let remaining = size - data.len();
        let to_add = std::cmp::min(pattern.len(), remaining);
        data.extend_from_slice(&pattern[..to_add]);
    }
    
    data
}

/// 计算目录大小
fn calculate_directory_size(dir: &std::path::Path) -> Result<u64, std::io::Error> {
    let mut total_size = 0;
    
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        
        if metadata.is_file() {
            total_size += metadata.len();
        } else if metadata.is_dir() {
            total_size += calculate_directory_size(&entry.path())?;
        }
    }
    
    Ok(total_size)
}
