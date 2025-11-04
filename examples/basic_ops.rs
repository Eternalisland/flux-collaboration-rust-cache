// tests/basic_ops.rs
// 替换成你的 crate 名称：例如 `use mmap_store as store;`
extern crate flux_collaboration_rust_cache as store;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};
use std::{thread, io};
use flux_collaboration_rust_cache::{HighPerfMmapConfig, HighPerfMmapStorage};

// ---- 简易临时目录 RAII ----
struct TempDir {
    path: PathBuf,
}
impl TempDir {
    fn new(prefix: &str) -> io::Result<Self> {
        let mut p = std::env::temp_dir();
        let ts = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid = std::process::id();
        p.push(format!("{prefix}-{pid}-{ts}"));
        fs::create_dir_all(&p)?;
        Ok(Self { path: p })
    }
    fn path(&self) -> &Path {
        &self.path
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        // 尽力清理；忽略错误（Windows 上文件句柄延迟关闭时可能失败）
        let _ = fs::remove_dir_all(&self.path);
    }
}

// 生成可重复的测试数据（不依赖 rand）
fn make_bytes(seed: u32, len: usize) -> Vec<u8> {
    let mut x = seed ^ 0x9E37_79B9;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        // xorshift32
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        out.push((x & 0xFF) as u8);
    }
    out
}

fn default_config() -> HighPerfMmapConfig {
    HighPerfMmapConfig {
        initial_file_size: 16 * 1024 * 1024, // 16MB
        growth_step: 8 * 1024 * 1024,        // 8MB
        growth_reserve_steps: 2,
        max_file_size: 512 * 1024 * 1024,    // 512MB
        enable_compression: false,
        l1_cache_size_limit: 32 * 1024 * 1024,
        l1_cache_entry_limit: 50_000,
        l2_cache_size_limit: 0,
        l2_cache_entry_limit: 2_000,
        enable_prefetch: true,               // 测基本功能可开可关
        prefetch_queue_size: 256,
        memory_pressure_threshold: 0.8,
        cache_degradation_threshold: 0.9,
        compression: Default::default(),
        auto_purge_after_secs: None,
        auto_purge_check_interval_secs: 300,
    }
}

#[test]
fn basic_write_read_delete_and_persist() -> io::Result<()> {
    let tmp = TempDir::new("hpms-basic")?;
    let cfg = default_config();

    // 1) 初始化存储
    let store = HighPerfMmapStorage::new(tmp.path().to_path_buf(), cfg.clone())?;

    // （可选）不开后台预读线程，避免干扰最小功能验证
    // let store = std::sync::Arc::new(store);
    // store.start_background_tasks();

    let key = String::from( "key-0000");
    let value1 = String::from("value1-0000");
    let value2 = String::from("value1-0001");

    let vb1 = value1.as_bytes();
    let result = store.write(&key, &vb1);

    let vec = store.read(&key).expect("write should be success ").unwrap().to_vec();
    println!("{}", String::from_utf8_lossy(&vec));

    let vb2 = value2.as_bytes();
    let result2 = store.write(&key, &vb2);

    let vec = store.read(&key).expect("write should be success ").unwrap().to_vec();
    print!("{}", String::from_utf8_lossy(&vec));



    // 2) 写入一批键值并立即读回校验
    let n = 200usize;
    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        let k = format!("key-{i:04}");
        let v = make_bytes(i as u32, 64 + (i % 128));
        store.write(&k, &v)?;
        // 立即读回
        let got = store.read(&k)?.expect("must read just written value");
        assert_eq!(v, got, "value mismatch on immediate read for {k}");
        keys.push((k, v));
    }

    // 3) 批量读取部分键校验（覆盖 read_batch 路径）
    let sample_keys: Vec<String> = keys.iter().step_by(3).map(|kv| kv.0.clone()).collect();
    let batch = store.read_batch(&sample_keys)?;
    assert_eq!(batch.len(), sample_keys.len());
    for (k, v_expected) in keys.iter().step_by(3) {
        let v = batch.get(k).expect("batch must contain key");
        assert_eq!(v, v_expected, "batch value mismatch for {k}");
    }

    // 4) 删除一部分键并验证读不到
    for (idx, (k, _)) in keys.iter().enumerate() {
        if idx % 5 == 0 {
            let ok = store.delete(k)?;
            assert!(ok, "delete should return true for existing key");
            let none = store.read(k)?;
            assert!(none.is_none(), "deleted key {} should return None", k);
        }
    }

    // 5) 刷盘/保存索引（可选），然后做一次 GC 验证不崩
    store.flush()?;
    let _ = store.garbage_collect()?; // 只要不报错即可

    // 6) 读统计/错误统计的 sanity check
    let stats = store.get_stats();
    assert!(stats.total_writes as usize >= n, "total_writes too small");
    assert!(stats.total_reads > 0, "total_reads should be > 0");
    assert_eq!(stats.error_count, 0, "no errors expected in basic test");

    // 7) 模拟重启：Drop 旧实例，再用同目录 new 一个，验证持久化索引可用
    drop(stats);
    drop(store);
    // 若你改了 Drop 行为或索引保存时机，这里多等 50ms 保险
    thread::sleep(Duration::from_millis(50));

    let store2 = HighPerfMmapStorage::new(tmp.path().to_path_buf(), cfg)?;
    // 已删除的 key 读不到，未删除的可读
    for (idx, (k, v_expected)) in keys.iter().enumerate() {
        let got = store2.read(k)?;
        if idx % 5 == 0 {
            assert!(got.is_none(), "after reload, deleted key {} should be None", k);
        } else {
            let v = got.expect("existing key should be readable after reload");
            assert_eq!(v, *v_expected, "after reload, value mismatch for {}", k);
        }
    }


    let bench_secs = 3600;
    let t0 = Instant::now();
    while t0.elapsed().as_secs() < bench_secs {

        let n = 1024usize;
        // let mut keys = Vec::with_capacity(n);
        for i in 0..n {
            let k = format!("key-{i:024}");
            let v = make_bytes(i as u32, 1024 * 1024 * 10 + (i % 2048));
            store2.write(&k, &v)?;
            // 立即读回
            let got = store2.read(&k)?.expect("must read just written value");
            assert_eq!(v, got, "value mismatch on immediate read for {k}");

            store2.delete(&k).expect("TODO: panic message");

            if( i == 1000usize) {
                let stat = store2.get_stats();
                // let mem_stat = store2.get_memory_stats();

                println!("{:?}", stat);
                // println!("{:?}", mem_stat);
            }
        }
    }

    // 可选：最终刷盘
    store2.flush()?;
    Ok(())
}

fn main() {}
