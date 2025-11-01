# 大数据缓存配置参数说明

## 🚀 配置参数总览

### BigDataStorage 配置参数

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `disk_dir` | String | `/tmp/flux-bigdata-cache` | 磁盘存储目录 |
| `memory_threshold` | long | 100MB | 内存热数据最大大小（字节） |
| `prefetch_block_size` | long | 1MB | 文件预读取块大小（字节） |
| `disk_cache_dir` | String | `/tmp/flux-bigdata-disk` | 磁盘缓存目录 |
| `enable_compression` | boolean | false | 是否启用压缩（大数据场景建议关闭） |
| `cleanup_interval_secs` | long | 300 | 清理间隔（秒） |

### OptimizedCache 配置参数

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `disk_dir` | String | `/tmp/flux-optimized-cache` | 磁盘存储目录 |
| `memory_threshold` | long | 500MB | 内存映射阈值（字节） |
| `max_memory_maps` | long | 100 | 最大内存映射数量 |
| `warmup_threshold` | long | 3 | 预热阈值（访问次数） |
| `cleanup_interval_secs` | long | 300 | 清理间隔（秒） |
| `memory_cleanup_threshold` | double | 0.8 | 内存清理触发阈值（80%） |

## 📋 JNI 接口说明

### 1. 大数据存储创建接口

#### 完整配置版本
```java
public native long createStorageFull(
    String diskDir,              // 磁盘目录
    long memoryThreshold,        // 内存阈值（字节）
    long prefetchBlockSize,      // 预读取块大小（字节）
    String diskCacheDir,         // 磁盘缓存目录
    boolean enableCompression,   // 是否启用压缩
    long cleanupIntervalSecs     // 清理间隔（秒）
);
```

#### 简化版本（推荐）
```java
public native long createStorage(
    String diskDir,              // 磁盘目录
    long memoryThreshold         // 内存阈值（字节）
);
```

### 2. 优化缓存创建接口

```java
public native long createCache(
    String diskDir,                    // 磁盘目录
    long memoryThreshold,              // 内存阈值（字节）
    long maxMemoryMaps,                // 最大内存映射数量
    long warmupThreshold,              // 预热阈值（访问次数）
    long cleanupIntervalSecs,          // 清理间隔（秒）
    double memoryCleanupThreshold      // 内存清理触发阈值
);
```

## 🎯 推荐配置策略

### 大数据场景（>1MB文件）

#### 高性能配置
```java
// 大数据存储
long storageId = createStorageFull(
    "/data/cache",           // 专用缓存目录
    500 * 1024 * 1024,      // 500MB内存阈值
    2 * 1024 * 1024,        // 2MB预读取块
    "/data/cache/disk",      // 磁盘缓存目录
    false,                   // 关闭压缩（提高速度）
    300                     // 5分钟清理间隔
);

// 优化缓存
long cacheId = createCache(
    "/data/optimized",       // 优化缓存目录
    1 * 1024 * 1024 * 1024, // 1GB内存阈值
    200,                     // 最多200个内存映射
    2,                       // 访问2次后预热
    180,                     // 3分钟清理间隔
    0.75                     // 75%内存使用率触发清理
);
```

#### 内存受限配置
```java
// 大数据存储
long storageId = createStorageFull(
    "/data/cache",
    100 * 1024 * 1024,      // 100MB内存阈值
    512 * 1024,             // 512KB预读取块
    "/data/cache/disk",
    false,
    600                     // 10分钟清理间隔
);

// 优化缓存
long cacheId = createCache(
    "/data/optimized",
    200 * 1024 * 1024,      // 200MB内存阈值
    50,                     // 最多50个内存映射
    5,                      // 访问5次后预热
    300,                    // 5分钟清理间隔
    0.8                     // 80%内存使用率触发清理
);
```

## ⚙️ 配置参数调优指南

### 内存阈值 (memory_threshold)
- **大数据场景**: 建议设置为系统内存的 10-20%
- **高并发场景**: 适当降低以支持更多并发连接
- **单文件大场景**: 可以适当提高

### 预读取块大小 (prefetch_block_size)
- **顺序读取**: 设置为 2-4MB
- **随机访问**: 设置为 512KB-1MB
- **网络存储**: 适当增大以减少网络往返

### 预热阈值 (warmup_threshold)
- **热点数据明显**: 设置为 2-3
- **访问模式分散**: 设置为 5-10
- **内存紧张**: 设置为 10+

### 清理间隔 (cleanup_interval_secs)
- **高频写入**: 设置为 60-180秒
- **低频写入**: 设置为 300-600秒
- **长时间运行**: 设置为 600秒以上

### 内存清理阈值 (memory_cleanup_threshold)
- **内存充足**: 设置为 0.8-0.9
- **内存紧张**: 设置为 0.6-0.7
- **极端情况**: 设置为 0.5

## 🔧 运行时配置调整

### 动态监控
```java
// 获取统计信息
String stats = getStats(storageId);
// 解析JSON获取内存使用情况
// 根据内存使用率动态调整策略
```

### 配置验证
```java
// 创建前验证配置合理性
if (memoryThreshold < 10 * 1024 * 1024) {
    // 内存阈值太小，可能影响性能
    throw new IllegalArgumentException("Memory threshold too small");
}

if (maxMemoryMaps > 1000) {
    // 映射数量太多，可能消耗过多文件描述符
    throw new IllegalArgumentException("Too many memory maps");
}
```

## 📊 性能监控指标

### 关键指标
- **内存命中率**: `memory_hits / (memory_hits + disk_hits)`
- **平均访问时间**: `avg_access_time_us`
- **内存使用率**: `memory_bytes / memory_threshold`
- **文件数量**: `file_count`

### 性能调优建议
1. **内存命中率 < 70%**: 考虑增加内存阈值
2. **平均访问时间 > 1000μs**: 检查磁盘I/O性能
3. **内存使用率 > 90%**: 降低内存阈值或增加清理频率
4. **文件数量过多**: 增加清理间隔或调整预热阈值

## 🚨 注意事项

1. **目录权限**: 确保缓存目录有读写权限
2. **磁盘空间**: 监控磁盘使用情况，避免空间不足
3. **文件描述符**: 大量内存映射可能消耗文件描述符
4. **系统内存**: 避免内存阈值设置过大导致系统内存不足
5. **清理策略**: 根据业务特点调整清理策略，避免误删重要数据
