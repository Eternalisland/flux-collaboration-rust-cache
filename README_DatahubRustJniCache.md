# DatahubRustJniCache - 高性能 MMAP 存储 JNI 桥接

## 概述

`DatahubRustJniCache` 是一个高性能的内存映射文件存储系统，通过 JNI 桥接为 Java 应用程序提供 Rust 实现的高性能存储能力。

## 特性

- 🚀 **高性能**：基于内存映射文件，比传统存储快数百倍
- 🔥 **热点缓存**：智能 LRU 缓存，命中率可达 80%+
- 📖 **智能预读**：自动预读相邻数据，提高缓存命中率
- 🔄 **批量操作**：支持批量读写，减少系统调用
- 📊 **详细统计**：提供完整的性能统计信息
- 🛡️ **线程安全**：无锁并发设计，支持高并发访问
- 💾 **持久化**：数据持久化到磁盘，支持索引保存

## 性能对比

| 存储方式 | 读取延迟 | 写入延迟 | 缓存命中率 |
|---------|---------|---------|-----------|
| 传统存储 | 4146μs | 9730μs | N/A |
| 高性能 MMAP | **9μs** | 16709μs | **80%+** |

**性能提升**：读取性能提升 **460倍**！

## 快速开始

### 1. 基本使用

```java
// 创建存储实例
long storagePtr = DatahubRustJniCache.createHighPerfMmapStorageSimple("/tmp/cache");

// 写入数据
byte[] data = "Hello World".getBytes();
boolean success = DatahubRustJniCache.write(storagePtr, "key1", data);

// 读取数据
byte[] result = DatahubRustJniCache.read(storagePtr, "key1");

// 释放资源
DatahubRustJniCache.release(storagePtr);
```

### 2. 使用包装类（推荐）

```java
try (DatahubRustJniCache.DatahubRustJniCacheWrapper storage = 
     DatahubRustJniCache.createWrapper("/tmp/cache")) {
    
    // 写入数据
    storage.write("user:123", "John Doe".getBytes());
    
    // 读取数据
    byte[] result = storage.read("user:123");
    
    // 获取统计信息
    String stats = storage.getStats();
    System.out.println("Stats: " + stats);
    
} // 自动释放资源
```

### 3. 自定义配置

```java
// 创建自定义配置
DatahubRustJniCache.StorageConfig config = new DatahubRustJniCache.StorageConfig()
    .setInitialFileSize(200 * 1024 * 1024)  // 200MB
    .setGrowthStep(100 * 1024 * 1024)       // 100MB
    .setMaxFileSize(5L * 1024 * 1024 * 1024) // 5GB
    .setEnableCompression(true)              // 启用压缩
    .setHotCacheSizeLimit(1L * 1024 * 1024 * 1024) // 1GB 热点缓存
    .setHotCacheEntryLimit(10000)            // 10000 个缓存条目
    .setEnablePrefetch(true)                 // 启用预读
    .setPrefetchQueueSize(200);              // 200 个预读任务

// 使用自定义配置创建存储
try (DatahubRustJniCache.DatahubRustJniCacheWrapper storage = 
     DatahubRustJniCache.createWrapper("/tmp/cache", config)) {
    
    // 使用存储...
}
```

## API 参考

### 创建存储实例

#### `createHighPerfMmapStorageSimple(String diskDir)`
使用默认配置创建存储实例。

**参数：**
- `diskDir`: 磁盘目录路径

**返回：** 存储实例指针

#### `createHighPerfMmapStorage(String diskDir, ...)`
使用自定义配置创建存储实例。

**参数：**
- `diskDir`: 磁盘目录路径
- `initialFileSize`: 初始文件大小（字节）
- `growthStep`: 文件增长步长（字节）
- `maxFileSize`: 最大文件大小（字节）
- `enableCompression`: 是否启用压缩
- `hotCacheSizeLimit`: 热点缓存大小限制（字节）
- `hotCacheEntryLimit`: 热点缓存条目数量限制
- `enablePrefetch`: 是否启用预读
- `prefetchQueueSize`: 预读队列大小

### 数据操作

#### `write(long storagePtr, String key, byte[] data)`
写入数据到存储。

#### `read(long storagePtr, String key)`
从存储读取数据。

#### `writeBatch(long storagePtr, String[] keys, byte[][] dataArray)`
批量写入数据。

#### `readBatch(long storagePtr, String[] keys)`
批量读取数据。

### 统计信息

#### `getStats(long storagePtr)`
获取详细统计信息（JSON 格式）。

**返回的 JSON 字段：**
```json
{
  "total_writes": 1000,
  "total_reads": 2000,
  "total_write_bytes": 1048576,
  "total_read_bytes": 2097152,
  "hot_cache_hits": 1600,
  "hot_cache_misses": 400,
  "prefetch_hits": 200,
  "avg_write_latency_us": 16709,
  "avg_read_latency_us": 9,
  "mmap_remaps": 5,
  "hot_cache_hit_rate": 80.0
}
```

#### `getCacheHitRate(long storagePtr)`
获取缓存命中率（百分比）。

#### `getAvgReadLatency(long storagePtr)`
获取平均读取延迟（微秒）。

#### `getAvgWriteLatency(long storagePtr)`
获取平均写入延迟（微秒）。

### 资源管理

#### `release(long storagePtr)`
释放存储实例。

#### `isValid(long storagePtr)`
检查存储实例是否有效。

#### `saveIndex(long storagePtr)`
保存索引到磁盘。

## 配置说明

### 默认配置

```java
public static class Config {
    /** 默认初始文件大小：100MB */
    public static final long DEFAULT_INITIAL_FILE_SIZE = 100 * 1024 * 1024;
    
    /** 默认文件增长步长：50MB */
    public static final long DEFAULT_GROWTH_STEP = 50 * 1024 * 1024;
    
    /** 默认最大文件大小：10GB */
    public static final long DEFAULT_MAX_FILE_SIZE = 10L * 1024 * 1024 * 1024;
    
    /** 默认热点缓存大小：500MB */
    public static final long DEFAULT_HOT_CACHE_SIZE = 500 * 1024 * 1024;
    
    /** 默认热点缓存条目数：5000 */
    public static final long DEFAULT_HOT_CACHE_ENTRIES = 5000;
    
    /** 默认预读队列大小：100 */
    public static final long DEFAULT_PREFETCH_QUEUE_SIZE = 100;
}
```

### 配置建议

**小数据量场景（< 1GB）：**
```java
.setInitialFileSize(50 * 1024 * 1024)      // 50MB
.setHotCacheSizeLimit(100 * 1024 * 1024)    // 100MB
.setHotCacheEntryLimit(1000)                // 1000 条目
```

**中等数据量场景（1-10GB）：**
```java
.setInitialFileSize(200 * 1024 * 1024)      // 200MB
.setHotCacheSizeLimit(500 * 1024 * 1024)    // 500MB
.setHotCacheEntryLimit(5000)                // 5000 条目
```

**大数据量场景（> 10GB）：**
```java
.setInitialFileSize(500 * 1024 * 1024)      // 500MB
.setHotCacheSizeLimit(1L * 1024 * 1024 * 1024) // 1GB
.setHotCacheEntryLimit(10000)               // 10000 条目
```

## 性能优化建议

### 1. 热点数据访问模式
- 将频繁访问的数据放在缓存中
- 使用批量操作减少系统调用
- 启用预读功能提高缓存命中率

### 2. 内存管理
- 根据数据量调整缓存大小
- 监控缓存命中率，调整缓存策略
- 定期保存索引避免数据丢失

### 3. 并发访问
- 存储实例是线程安全的
- 可以多线程并发读写
- 避免频繁创建和销毁存储实例

## 错误处理

### 常见错误

1. **存储指针为空**
   ```java
   if (storagePtr == 0) {
       throw new RuntimeException("Failed to create storage instance");
   }
   ```

2. **磁盘空间不足**
   ```java
   try {
       storage.write(key, data);
   } catch (RuntimeException e) {
       if (e.getMessage().contains("No space left")) {
           // 处理磁盘空间不足
       }
   }
   ```

3. **资源泄漏**
   ```java
   // 使用 try-with-resources 自动释放
   try (DatahubRustJniCache.DatahubRustJniCacheWrapper storage = 
        DatahubRustJniCache.createWrapper("/tmp/cache")) {
       // 使用存储...
   } // 自动释放
   ```

## 示例代码

完整的使用示例请参考：
- `DatahubRustJniCacheExample.java` - 基本使用示例
- `DatahubRustJniCache.java` - API 参考

## 注意事项

1. **资源管理**：务必调用 `release()` 或使用 try-with-resources 释放资源
2. **线程安全**：存储实例是线程安全的，可以多线程并发访问
3. **数据持久化**：数据会自动持久化到磁盘，重启后数据仍然存在
4. **性能监控**：定期检查统计信息，优化缓存配置
5. **错误处理**：妥善处理异常，避免资源泄漏

## 技术支持

如有问题，请联系 Flux Collaboration Team。
