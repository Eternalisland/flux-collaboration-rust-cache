# flux-collaboration-rust-cache

一个用于演示与实验的 Rust 基础项目，包含固定工具链与简单的 CI 工作流。

## 快速开始

### 环境需求
- Rust（稳定版 stable），建议使用 `rustup` 安装
- 已安装组件：`rustfmt`、`clippy`

如果你使用 `rustup`，本仓库已提供 `rust-toolchain.toml`，会自动固定到稳定版并安装所需组件。

### 构建
```bash
cargo build
```

### 运行
```bash
cargo run
```

### 测试
```bash
cargo test
```

### 代码格式检查
```bash
cargo fmt --all -- --check
```

### 静态检查（Clippy）
```bash
cargo clippy --all-targets --all-features -- -D warnings
```

## 目录结构
- `src/main.rs`: 示例入口
- `Cargo.toml`: Cargo 配置
- `rust-toolchain.toml`: 固定 Rust 工具链与组件
- `.github/workflows/ci.yml`: GitHub Actions CI 配置

## 使用示例（本地缓存封装）

```rust
use flux_collaboration_rust_cache::{LocalCache, LocalCacheBuilder};
use std::time::Duration;

fn main() {
    let cache: LocalCache<String, String> = LocalCache::<String, String>::builder()
        .max_capacity(1_000)
        .time_to_live(Duration::from_secs(60))
        .time_to_idle(Duration::from_secs(30))
        // 若值序列化后超过 1KB，则落盘
        .disk_offload_threshold(1024)
        .build();

    cache.insert("k".into(), "v".into());
    assert_eq!(cache.get(&"k".into()).as_deref(), Some("v"));

    cache.upsert("k".into(), "v2".into());
    assert_eq!(cache.get(&"k".into()).as_deref(), Some("v2"));

    cache.remove(&"k".into());
    assert!(cache.get(&"k".into()).is_none());

    let m = cache.metrics();
    println!("entries={}, cap={:?}, ttl={:?}, tti={:?}", m.entries, m.max_capacity, m.time_to_live_secs, m.time_to_idle_secs);
}
```

## 租约机制（自动清理）

为解决接口下发数据后可能因异常未清理导致缓存无限增长的问题，实现了基于租约的自动清理机制：

### 核心特性

1. **自动过期清理**：每个条目都有租约时间，到期后自动清理
2. **手动续租**：接口处理过程中可延长租约时间
3. **自动续租**：获取缓存数据时自动续租（可配置）
4. **强制清理**：紧急情况下可忽略租约强制删除
5. **后台批量清理**：定期批量清理过期条目，避免性能影响

### 使用示例

```rust
use flux_collaboration_rust_cache::LocalCache;
use std::time::Duration;

fn main() {
    let cache: LocalCache<String, String> = LocalCache::<String, String>::builder()
        .max_capacity(10_000)
        .default_lease_duration(Duration::from_secs(300)) // 默认 5 分钟租约
        .cleanup_interval(Duration::from_secs(30))        // 30 秒清理一次
        .auto_renew_on_get(true)                          // 获取时自动续租（默认开启）
        .auto_renew_duration(Duration::from_secs(60))     // 自动续租 1 分钟
        .build();

    // 接口下发：添加数据（带租约）
    let request_id = "req_12345".to_string();
    cache.insert_with_lease(
        request_id.clone(),
        "response_data".to_string(),
        Duration::from_secs(60), // 1 分钟租约
        true                     // 允许自动续租
    );

    // 接口处理中：获取数据时会自动续租
    let data = cache.get(&request_id); // 自动续租 1 分钟
    
    // 手动续租（可选）
    cache.renew_lease(&request_id, Duration::from_secs(30));

    // 接口完成：正常清理
    cache.remove(&request_id);

    // 紧急情况：强制清理（忽略租约）
    // cache.force_remove(&request_id);
}
```

### 自动续租机制

**核心优势**：无需手动调用续租，获取缓存数据时自动续租，大大简化使用复杂度。

- **默认开启**：`auto_renew_on_get(true)` 默认开启自动续租
- **智能续租**：只有设置了 `auto_renew: true` 的条目才会自动续租
- **可配置时长**：`auto_renew_duration` 设置自动续租的时长
- **性能优化**：续租操作在获取数据时同步进行，无额外开销

```rust
// 创建支持自动续租的缓存
let cache = LocalCache::<String, String>::builder()
    .auto_renew_on_get(true)                    // 开启自动续租
    .auto_renew_duration(Duration::from_secs(60)) // 每次获取续租 1 分钟
    .build();

// 插入数据时设置允许自动续租
cache.insert_with_lease("key".into(), "value".into(), Duration::from_secs(30), true);

// 获取数据时自动续租（无需手动调用）
let value = cache.get(&"key".into()); // 自动续租 1 分钟
```

### 监控指标

```rust
let metrics = cache.metrics();
println!("总条目: {}, 租约条目: {}, 过期条目: {}", 
    metrics.entries, metrics.leased_entries, metrics.expired_entries);
```

## 磁盘溢出（大值落盘）

- 当 `V` 实现 `serde::Serialize + serde::de::DeserializeOwned` 时，若序列化后的字节数超过阈值（`disk_offload_threshold`），该条目会写入 `disk_directory` 指定目录（默认：系统临时目录下 `flux-cache`）。
- 读取时，若命中磁盘索引，将直接从文件反序列化返回。
- 删除与缓存淘汰会自动清理对应的落盘文件。

简单示例：

```rust
use flux_collaboration_rust_cache::LocalCache;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct BigData(Vec<u8>);

fn main() {
    let cache: LocalCache<String, BigData> = LocalCache::<String, BigData>::builder()
        .max_capacity(100)
        .disk_offload_threshold(2048) // 超过 2KB 落盘
        .build();

    let big = BigData(vec![0u8; 10 * 1024]);
    cache.insert("big".into(), big);
    let m = cache.metrics();
    assert!(m.disk_entries >= 1);
}
```


