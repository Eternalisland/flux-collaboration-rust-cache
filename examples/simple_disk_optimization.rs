
use flux_collaboration_rust_cache::disk_storage::{OptimizedDiskStorage, StorageStrategy};
use std::time::Instant;

/// 简化的磁盘存储优化示例
/// 
/// 演示核心的磁盘存储优化技术：
/// 1. 传统存储
/// 2. 批量写入
/// 3. 文件池
/// 4. 压缩存储
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 简化磁盘存储优化示例 ===");
    
    // 创建临时目录
    let temp_dir = std::env::temp_dir().join("flux_cache_simple");
    std::fs::create_dir_all(&temp_dir)?;
    
    println!("临时目录: {:?}", temp_dir);
    
    // 生成测试数据
    let test_data = generate_test_data(1024); // 1KB 测试数据
    let num_operations = 100; // 减少操作数量以加快测试
    
    // 1. 测试传统存储
    test_traditional_storage(&temp_dir, &test_data, num_operations)?;
    
    // 2. 测试批量写入
    test_batch_write(&temp_dir, &test_data, num_operations)?;
    
    // 3. 测试文件池
    test_file_pool(&temp_dir, &test_data, num_operations)?;
    
    // 4. 测试压缩存储
    test_compressed_storage(&temp_dir, &test_data, num_operations)?;
    
    // 清理临时文件
    std::fs::remove_dir_all(&temp_dir)?;
    
    println!("\n所有测试完成！");
    Ok(())
}

/// 测试传统存储
fn test_traditional_storage(
    temp_dir: &std::path::Path, 
    test_data: &[u8], 
    num_operations: usize
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 传统存储测试 ===");
    
    let traditional_dir = temp_dir.join("traditional");
    std::fs::create_dir_all(&traditional_dir)?;
    
    let storage = OptimizedDiskStorage::new(traditional_dir.clone(), StorageStrategy::Traditional)?;
    
    // 写入测试
    let mut written_paths = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("traditional_{}", i);
        let path = storage.write(&key, test_data)?;
        written_paths.push(path);
    }
    let write_time = start_time.elapsed();
    
    // 读取测试
    let start_time = Instant::now();
    for path in &written_paths {
        let _ = storage.read(path)?;
    }
    let read_time = start_time.elapsed();
    
    // 获取统计信息
    let stats = storage.get_stats();
    
    println!("传统存储结果:");
    println!("  写入时间: {:?}", write_time);
    println!("  读取时间: {:?}", read_time);
    println!("  总写入次数: {}", stats.total_writes);
    println!("  总读取次数: {}", stats.total_reads);
    println!("  平均写入延迟: {}μs", stats.avg_write_latency_us);
    println!("  平均读取延迟: {}μs", stats.avg_read_latency_us);
    
    Ok(())
}

/// 测试批量写入
fn test_batch_write(
    temp_dir: &std::path::Path, 
    test_data: &[u8], 
    num_operations: usize
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 批量写入测试 ===");
    
    let batch_dir = temp_dir.join("batch");
    std::fs::create_dir_all(&batch_dir)?;
    
    let storage = OptimizedDiskStorage::new(batch_dir.clone(), StorageStrategy::BatchWrite)?;
    
    // 写入测试
    let mut written_paths = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("batch_{}", i);
        let path = storage.write(&key, test_data)?;
        written_paths.push(path);
    }
    // 刷新缓冲区
    storage.flush()?;
    let write_time = start_time.elapsed();
    
    // 读取测试
    let start_time = Instant::now();
    for path in &written_paths {
        let _ = storage.read(path)?;
    }
    let read_time = start_time.elapsed();
    
    // 获取统计信息
    let stats = storage.get_stats();
    
    println!("批量写入结果:");
    println!("  写入时间: {:?}", write_time);
    println!("  读取时间: {:?}", read_time);
    println!("  总写入次数: {}", stats.total_writes);
    println!("  总读取次数: {}", stats.total_reads);
    println!("  平均写入延迟: {}μs", stats.avg_write_latency_us);
    println!("  平均读取延迟: {}μs", stats.avg_read_latency_us);
    
    Ok(())
}

/// 测试文件池
fn test_file_pool(
    temp_dir: &std::path::Path, 
    test_data: &[u8], 
    num_operations: usize
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 文件池测试 ===");
    
    let pool_dir = temp_dir.join("pool");
    std::fs::create_dir_all(&pool_dir)?;
    
    let storage = OptimizedDiskStorage::new(pool_dir.clone(), StorageStrategy::FilePool)?;
    
    // 写入测试
    let mut written_paths = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("pool_{}", i);
        let path = storage.write(&key, test_data)?;
        written_paths.push(path);
    }
    let write_time = start_time.elapsed();
    
    // 读取测试
    let start_time = Instant::now();
    for path in &written_paths {
        let _ = storage.read(path)?;
    }
    let read_time = start_time.elapsed();
    
    // 获取统计信息
    let stats = storage.get_stats();
    
    println!("文件池结果:");
    println!("  写入时间: {:?}", write_time);
    println!("  读取时间: {:?}", read_time);
    println!("  总写入次数: {}", stats.total_writes);
    println!("  总读取次数: {}", stats.total_reads);
    println!("  平均写入延迟: {}μs", stats.avg_write_latency_us);
    println!("  平均读取延迟: {}μs", stats.avg_read_latency_us);
    
    Ok(())
}

/// 测试压缩存储
fn test_compressed_storage(
    temp_dir: &std::path::Path, 
    test_data: &[u8], 
    num_operations: usize
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 压缩存储测试 ===");
    
    let compressed_dir = temp_dir.join("compressed");
    std::fs::create_dir_all(&compressed_dir)?;
    
    let storage = OptimizedDiskStorage::new(compressed_dir.clone(), StorageStrategy::Compressed)?;
    
    // 写入测试
    let mut written_paths = Vec::new();
    let start_time = Instant::now();
    for i in 0..num_operations {
        let key = format!("compressed_{}", i);
        let path = storage.write(&key, test_data)?;
        written_paths.push(path);
    }
    let write_time = start_time.elapsed();
    
    // 读取测试
    let start_time = Instant::now();
    for path in &written_paths {
        let _ = storage.read(path)?;
    }
    let read_time = start_time.elapsed();
    
    // 获取统计信息
    let stats = storage.get_stats();
    
    // 计算磁盘使用量
    let disk_usage = calculate_directory_size(&compressed_dir)?;
    
    println!("压缩存储结果:");
    println!("  写入时间: {:?}", write_time);
    println!("  读取时间: {:?}", read_time);
    println!("  磁盘使用: {} bytes", disk_usage);
    println!("  总写入次数: {}", stats.total_writes);
    println!("  总读取次数: {}", stats.total_reads);
    println!("  平均写入延迟: {}μs", stats.avg_write_latency_us);
    println!("  平均读取延迟: {}μs", stats.avg_read_latency_us);
    
    Ok(())
}

/// 生成测试数据
fn generate_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
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
