# flux-collaboration-rust-cache

面向大数据/高吞吐业务场景的本地多级缓存组件。核心以 **MMAP + 自定义索引** 实现的数据引擎为主，辅以无锁热点缓存、智能预读、压缩与垃圾回收策略，并通过 JNI 暴露给 Java 服务端使用。本仓库同时也是团队 Rust 代码规范、CI 与多语言联调示例的集合。

## 核心特性

- **零拷贝 + 无锁读路径**：借助 `ArcSwap + Seqlock` 管理的 `memmap2::Mmap`，读操作不抢锁，延迟极低；写路径仅在必要时持有短暂锁。
- **多级缓存 + 预读**：DashMap 管理的 L1 热点缓存 + L2 索引，配合 `ArrayQueue` 预读线程提升读命中率，可按容量或条目数限制。
- **智能压缩与落盘**：提供 `CompressionConfig`，支持 LZ4/Zstd/Snappy，并可设置最小压缩阈值、压缩比与异步压缩策略，避免对小对象过度处理。
- **内存治理**：`MemoryLimitConfig` 定义软/硬限制、驱逐比例、检查频率，支持内存压力触发写入拒绝与后台清理。
- **观测性完善**：`HighPerfMmapStats` 和 `MemoryStats` 暴露写读次数、缓存命中、平均延迟、GC 数、内存占用等指标，可直接序列化上报。
- **JNI 桥接**：`DatahubRustJniCache` 覆盖创建、读写删、状态获取、显式 GC 等方法，可在 Java 侧使用 JSON 一键配置。

## 目录速览

| 路径 | 说明 |
| --- | --- |
| `src/high_perf_mmap_storage.rs` | 核心存储实现，含压缩、预读、GC、配置与统计 |
| `src/jni_bridge_datahub.rs` | JNI 接口封装，映射到 `com.flux...DatahubRustJniCache` |
| `examples/` | Rust 本地示例与基准脚本（`basic_ops.rs` 等） |
| `docs/configuration.md` | 参数说明与推荐调优策略 |
| `tests/` | 读写删性能与批量操作测试 |

## 快速开始

### 环境需求
- Rust stable（建议使用 `rustup` 安装；仓库自带 `rust-toolchain.toml` 固定版本）
- 必备组件：`cargo`, `rustfmt`, `clippy`

### 构建与测试

```bash
cargo build
cargo test          # 包含单测与性能基准
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

### 运行示例

```bash
# 运行最小读写示例
cargo run --example basic_ops

# 或直接运行性能测试
cargo test --test read_write_delete_performance -- --nocapture
```

## Rust API 快速示例

```rust
use flux_collaboration_rust_cache::{
    HighPerfMmapConfig, HighPerfMmapStorage, MemoryLimitConfig,
};
use std::{path::PathBuf, sync::Arc};

fn main() -> std::io::Result<()> {
    // 1. 定义配置
    let mut config = HighPerfMmapConfig::default();
    config.initial_file_size = 512 * 1024 * 1024; // 512MB
    config.enable_prefetch = true;
    config.auto_purge_after_secs = Some(4 * 60 * 60);

    let memory_limit = MemoryLimitConfig {
        heap_soft_limit: 256 * 1024 * 1024,
        heap_hard_limit: 512 * 1024 * 1024,
        ..Default::default()
    };

    // 2. 创建存储并启动后台任务（需要 Arc<Self>）
    let store = Arc::new(HighPerfMmapStorage::new_with_options(
        PathBuf::from("/tmp/flux-cache"),
        config,
        memory_limit,
        /*clear_on_start=*/ false,
    )?);
    store.start_background_tasks();

    // 3. 基础读写
    store.write("user:42", b"{\"payload\":\"hello\"}")?;
    if let Some(bytes) = store.read("user:42")? {
        println!("value = {}", String::from_utf8_lossy(&bytes));
    }

    // 4. 观测与维护
    println!("stats = {:?}", store.get_stats());
    store.flush()?;                // 刷新 mmap
    let reclaimed = store.garbage_collect()?; // 手动触发磁盘 GC
    println!("gc removed {reclaimed} entries");

    Ok(())
}
```

## 配置项速览

### `HighPerfMmapConfig`

用于描述磁盘文件、缓存策略、压缩和预读等维度。常用字段：

```rust
let mut cfg = HighPerfMmapConfig {
    initial_file_size: 1024 * 1024 * 1024,    // 初始文件大小
    growth_step: 128 * 1024 * 1024,           // 扩容步长
    growth_reserve_steps: 3,                  // 预留扩容次数，避免频繁 remap
    max_file_size: 10 * 1024 * 1024 * 1024,   // 单文件上限
    enable_compression: true,
    compression: CompressionConfig {
        algorithm: CompressionAlgorithm::Zstd,
        min_compress_size: 512 * 1024,
        min_compress_ratio: 0.85,
        ..Default::default()
    },
    l1_cache_size_limit: 256 * 1024 * 1024,
    l1_cache_entry_limit: 50_000,
    l2_cache_size_limit: 512 * 1024 * 1024,
    enable_prefetch: true,
    prefetch_queue_size: 512,
    auto_purge_after_secs: Some(8 * 60 * 60),      // 8 小时自动过期
    auto_purge_check_interval_secs: 300,           // 5 分钟检查一次
    ..Default::default()
};
```

- **自动租约/清理**：通过 `auto_purge_after_secs` 与 `auto_purge_check_interval_secs` 控制后台定期逐出过期条目，避免磁盘无限增长。
- **预读**：`enable_prefetch + prefetch_queue_size` 会在读 miss 时将 key 推入后台线程，主动填充 L1 缓存。

### `MemoryLimitConfig`

控制堆内存与热点缓存的上限：

```rust
let memory_limit = MemoryLimitConfig {
    heap_soft_limit: 512 * 1024 * 1024,
    heap_hard_limit: 768 * 1024 * 1024,
    l1_cache_hard_limit: 128 * 1024 * 1024,
    max_eviction_percent: 0.2,              // 每次最多驱逐 20%
    reject_writes_under_pressure: true,     // 压力过高报警/限流
    check_interval_ms: 500,
    ..Default::default()
};
```

配合 `get_memory_stats()` 可以实现动态调参或接入监控系统，详细调优建议参见 `docs/configuration.md`。

## 数据访问模式

- `write(key, data)`：智能压缩 + 原子写入 + 索引更新，并触发惰性删除老版本。
- `read(key)`：命中 L1/L2 则直接返回；否则从 mmap 读取并按需解压。
- `read_batch(keys)` / `read_batch_coalesced`：按 key 排序、聚合访问以减少页缺失；适合批量接口。
- `read_slice(key)`：返回 `ZeroCopySlice`，可以直接在 mmap 上切片读取，适合大对象的流式消费。
- `delete(key)` / `delete_lazy(key)`：立刻删或标记惰性删除，依赖 `garbage_collect` 回收磁盘空间。
- `save_index()`：序列化当前索引到 `index.bin`，用于宕机快速恢复。
- `clear_all_data()`：测试或紧急场景下一键清空磁盘 + 内存状态。

## JNI / Java 接入

`src/jni_bridge_datahub.rs` 暴露的 `DatahubRustJniCache` 方法与 Java 类 `com.flux.collaboration.utils.cache.rust.jni.DatahubRustJniCache` 一一对应，涵盖：

- `createHighPerfMmapStorage*`：支持简单参数或 JSON 字符串创建实例。
- `write/read/delete`：基础 CRUD。
- `getStatus/getStats/getCacheHitRate/getAvgReadLatency`：运行状态和性能指标。
- `saveIndex/forceGarbageCollect/release`：维护操作。

JSON 配置示例：

```json
{
  "disk_dir": "/data/cache",
  "clear_on_start": false,
  "config": {
    "initial_file_size": 536870912,
    "growth_step": 134217728,
    "max_file_size": 10737418240,
    "enable_compression": false,
    "enable_prefetch": true,
    "prefetch_queue_size": 256,
    "auto_purge_after_secs": 14400
  },
  "memory_limit": {
    "heap_soft_limit": 268435456,
    "heap_hard_limit": 402653184,
    "l1_cache_hard_limit": 67108864,
    "max_eviction_percent": 0.2
  }
}
```

Java 侧最小示例：

```java
long handle = DatahubRustJniCache.createHighPerfMmapStorageFromJson(jsonConfig);
DatahubRustJniCache.write(handle, "key", payloadBytes);
byte[] value = DatahubRustJniCache.read(handle, "key");
String stats = DatahubRustJniCache.getStatus(handle); // JSON -> 可直接上报监控
DatahubRustJniCache.forceGarbageCollect(handle);
DatahubRustJniCache.release(handle);
```

## 观测与调试

- `get_stats()` 返回 `HighPerfMmapStats`，包含写读次数、缓存命中、平均延迟、GC/扩容事件、错误码等。
- `get_status()` 包含上面的统计 + `MemoryStats` + 当前 `HighPerfMmapConfig`，可用于自检或暴露在 `/actuator` 类接口。
- `tests/` 下的性能测试可以在本地压测瓶颈，也可作为 CI 的冒烟用例。
- 更多配置策略请查阅 `docs/configuration.md` 与 `docs/big_data_optimization_summary.md`。

## 开发与贡献

1. Fork & clone 后使用 `cargo build` / `cargo test` 确认环境正常。
2. 修改代码时保持 `cargo fmt`、`cargo clippy -- -D warnings` 无报错。
3. 运行 `cargo test --examples --tests`，或根据改动范围挑选 `examples/`、`tests/` 中的脚本验证。
4. 如需扩展 JNI 接口，请同步更新 Java 侧签名，并在 README/docs 中补充说明。

欢迎 issue/PR，讨论新的缓存策略、压缩算法或跨语言集成需求。
