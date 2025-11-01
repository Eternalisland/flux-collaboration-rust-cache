# 复杂 Java 对象缓存使用指南

## 概述

本指南详细说明如何在 Rust 缓存系统中存储和管理复杂的 Java 对象，特别是针对 `MonitorReceiveHeader` 这样的复杂监控对象。

## 目标问题

在大并发环境下，接口处理过程中：
- 接口下发数据到缓存框架
- 接口处理完后需要清理对应的 key  
- 每次请求的 key 都不一样
- 需要防止接口未处理完数据被清理
- 缓存满时自动清理历史数据
- 保证高性能要求

## 解决方案

### 方案一：使用 JSON 序列化（推荐）

这是最通用且性能良好的方案：

```java
import com.alibaba.fastjson.JSONObject;
import com.flux.collaboration.storage.model.MonitorReceiveHeader;

public class MonitorCacheUtil {
    private static final String CACHE_NAME = "monitor_cache";
    
    static {
        // 初始化缓存
        MonitorReceiveHeaderCache.createCache(
            CACHE_NAME,
            10000,  // 最大容量
            50 * 1024 * 1024,  // 50MB 磁盘阈值
            MonitorReceiveHeaderCache.SerializationFormat.JSON
        );
    }
    
    /**
     * 接口开始处理时调用
     */
    public static void startProcessing(MonitorReceiveHeader header) {
        String key = header.getMESSAGE_GROUP_SYSID();
        
        // 存储监控对象
        boolean stored = MonitorReceiveHeaderCache.putByMessageGroupSysId(
            CACHE_NAME, header
        );
        
        if (stored) {
            System.out.println("监控对象已创建: " + key);
        }
    }
    
    /**
     * 处理过程中更新状态
     */
    public static void updateProcessingStatus(String messageGroupSysId, 
                                            String status, String errorCode) {
        MonitorReceiveHeader header = MonitorReceiveHeaderCache
            .getByMessageGroupSysId(CACHE_NAME, messageGroupSysId);
        
        if (header != null) {
            header.setMESSAGE_STATUS_DESCR(status);
            header.setMESSAGE_ERRORCODE(errorCode);
            
            // 续租防止被清理
            MonitorReceiveHeaderCache.renewLease(CACHE_NAME, messageGroupSysId, 600);
            
            // 更新缓存
            MonitorReceiveHeaderCache.update(CACHE_NAME, messageGroupSysId, header);
        }
    }
    
    /**
     * 接口处理完成时调用
     */
    public static void finishProcessing(String messageGroupSysId, 
                                      String finalStatus, String errorCode) {
        MonitorReceiveHeader header = MonitorReceiveHeaderCache
            .getByMessageGroupSysId(CACHE_NAME, messageGroupSysId);
        
        if (header != null) {
            // 更新最终状态
            header.setMESSAGE_STATUS_DESCR(finalStatus);
            header.setMESSAGE_ERRORCODE(errorCode);
            header.setMESSAGE_ENDTIME(getCurrentTime());
            
            MonitorReceiveHeaderCache.update(CACHE_NAME, messageGroupSysId, header);
            
            // 处理完成后删除
            MonitorReceiveHeaderCache.removeByMessageGroupSysId(
                CACHE_NAME, messageGroupSysId
            );
            
            System.out.println("监控对象已清理: " + messageGroupSysId);
        }
    }
    
    private static String getCurrentTime() {
        return new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
            .format(new java.util.Date());
    }
}
```

### 方案二：使用 Java 原生序列化

如果不想引入外部依赖，可以使用 Java 原生序列化：

```java
public class NativeSerializationCache {
    
    /**
     * 序列化对象为 Base64 字符串
     */
    private static String serializeObject(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }
    
    /**
     * 从 Base64 字符串反序列化对象
     */
    @SuppressWarnings("unchecked")
    private static <T> T deserializeObject(String serializedData, Class<T> clazz) 
            throws IOException, ClassNotFoundException {
        byte[] bytes = Base64.getDecoder().decode(serializedData);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        T object = (T) ois.readObject();
        ois.close();
        return object;
    }
    
    public static void storeMonitorHeader(String key, MonitorReceiveHeader header) {
        try {
            String serializedData = serializeObject(header);
            DatahubRusCacheUnified.put(cacheId, key, serializedData);
        } catch (Exception e) {
            System.err.println("存储失败: " + e.getMessage());
        }
    }
    
    public static MonitorReceiveHeader getMonitorHeader(String key) {
        try {
            String serializedData = DatahubRusCacheUnified.get(cacheId, key);
            if (serializedData != null) {
                return deserializeObject(serializedData, MonitorReceiveHeader.class);
            }
        } catch (Exception e) {
            System.err.println("读取失败: " + e.getMessage());
        }
        return null;
    }
}
```

## 使用场景示例

### 典型接口处理流程

```java
public class InterfaceProcessor {
    
    public void processMessage(String requestData) {
        // 1. 创建监控对象
        MonitorReceiveHeader header = new MonitorReceiveHeader();
        String messageGroupSysId = generateMessageGroupSysId();
        header.setMESSAGE_GROUP_SYSID(messageGroupSysId);
        header.setMESSAGE_STATUS_DESCR("接收到消息");
        header.setMESSAGE_ERRORCODE("1000"); // 处理中
        header.setMESSAGE_CREATETIME(getCurrentTime());
        
        try {
            // 存储到缓存
            MonitorCacheUtil.startProcessing(header);
            
            // 2. 数据验证
            MonitorCacheUtil.updateProcessingStatus(
                messageGroupSysId, "数据验证中", "1001"
            );
            validateData(requestData);
            
            // 3. 业务处理
            MonitorCacheUtil.updateProcessingStatus(
                messageGroupSysId, "业务处理中", "1002"
            );
            String result = processBusiness(requestData);
            
            // 4. 处理完成
            MonitorCacheUtil.finishProcessing(
                messageGroupSysId, "处理完成", "0000"
            );
            
        } catch (Exception e) {
            // 5. 异常处理
            MonitorCacheUtil.finishProcessing(
                messageGroupSysId, "处理失败", "9999"
            );
            throw e;
        }
    }
    
    private String generateMessageGroupSysId() {
        return "MSG_" + System.currentTimeMillis() + "_" + 
               Thread.currentThread().getId();
    }
    
    // 其他处理方法...
}
```

### 批量处理场景

```java
public class BatchProcessor {
    
    public void processBatch(List<String> requests) {
        String batchId = "BATCH_" + System.currentTimeMillis();
        Map<String, MonitorReceiveHeader> batchHeaders = new HashMap<>();
        
        // 1. 创建批量监控对象
        for (int i = 0; i < requests.size(); i++) {
            MonitorReceiveHeader header = createBatchHeader(batchId, i);
            batchHeaders.put(header.getMESSAGE_GROUP_SYSID(), header);
        }
        
        // 2. 批量存储
        int storedCount = MonitorReceiveHeaderCache.putBatch(
            "monitor_cache", batchHeaders
        );
        System.out.println("批量存储: " + storedCount + "/" + requests.size());
        
        try {
            // 3. 批量处理
            for (Map.Entry<String, MonitorReceiveHeader> entry : batchHeaders.entrySet()) {
                String messageGroupSysId = entry.getKey();
                
                // 更新处理状态
                MonitorCacheUtil.updateProcessingStatus(
                    messageGroupSysId, "批量处理中", "1000"
                );
                
                // 处理逻辑...
                
                // 完成处理
                MonitorCacheUtil.finishProcessing(
                    messageGroupSysId, "批量处理完成", "0000"
                );
            }
            
        } catch (Exception e) {
            // 异常时批量清理
            for (String messageGroupSysId : batchHeaders.keySet()) {
                MonitorReceiveHeaderCache.forceRemove("monitor_cache", messageGroupSysId);
            }
            throw e;
        }
    }
}
```

## 性能优化建议

### 1. 缓存参数配置

```java
// 根据实际并发量调整
MonitorReceiveHeaderCache.createCache(
    "monitor_cache",
    50000,  // 最大容量：根据并发量设置
    100 * 1024 * 1024,  // 100MB 磁盘阈值：大对象使用磁盘
    MonitorReceiveHeaderCache.SerializationFormat.MESSAGE_PACK  // 更高性能
);
```

### 2. 租约管理

```java
// 长时间处理的接口要定期续租
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(() -> {
    // 对于长时间处理的任务续租
    MonitorReceiveHeaderCache.renewLease(cacheName, messageGroupSysId, 300);
}, 0, 120, TimeUnit.SECONDS);
```

### 3. 序列化格式选择

- **JSON**: 兼容性好，调试方便
- **MessagePack**: 性能更好，体积更小
- **FlatBuffers**: 极致性能，零拷贝反序列化

### 4. 批量操作

```java
// 使用批量操作提高性能
Map<String, MonitorReceiveHeader> batchData = new HashMap<>();
// ... 准备数据
int successCount = MonitorReceiveHeaderCache.putBatch(cacheName, batchData);
```

## 监控和统计

```java
// 定期检查缓存状态
public void monitorCacheHealth() {
    String metrics = MonitorReceiveHeaderCache.getMetrics("monitor_cache");
    System.out.println("缓存状态: " + metrics);
    
    // 解析 JSON 获取具体指标
    // {"entries": 1234, "disk_entries": 56, "expired_entries": 78, ...}
}
```

## 错误处理

```java
public class CacheErrorHandler {
    
    public static void handleCacheError(String operation, String key, Exception e) {
        System.err.println("缓存操作失败 - 操作: " + operation + 
                          ", 键: " + key + ", 错误: " + e.getMessage());
        
        // 记录到日志系统
        // logger.error("Cache operation failed", e);
        
        // 可以考虑降级策略，如使用本地临时存储
        fallbackToLocalStorage(key, operation);
    }
    
    private static void fallbackToLocalStorage(String key, String operation) {
        // 降级方案实现
    }
}
```

## 最佳实践

1. **及时清理**: 接口处理完成后立即删除缓存数据
2. **租约管理**: 长时间处理的任务要定期续租
3. **异常处理**: 确保异常情况下也能清理缓存
4. **批量操作**: 大量数据处理时使用批量接口
5. **监控告警**: 定期检查缓存健康状态
6. **性能测试**: 根据实际场景调整缓存参数

这样就可以很好地解决您提到的问题：防止数据泄漏、自动清理历史数据，同时保证高性能要求。

