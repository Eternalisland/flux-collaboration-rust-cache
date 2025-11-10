// =============================
// HighPerfMmapStorage 小型基准/自测脚本
// 放置路径建议：src/bin/hpmm_bench.rs
// 依赖：
//   rand = "0.8"
//   libc = "0.2"     // 仅 Unix 获取 page faults；Windows 会自动降级
// 用法示例：
//   cargo run --release --bin hpmm_bench -- \
//     --dir ./benchdata --writers 4 --readers 8 --secs 30 --keyspace 100000 \
//     --vmean 1024 --vmax 4096 --batch 32 --profile dev
// =============================

use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

// 根据你的工程结构，调整下面这行的导入路径：
// 1) 如果本 crate 有 lib.rs 并且在里面 `pub mod high_perf_mmap_storage;`
//    则使用：
// use crate::high_perf_mmap_storage::{HighPerfMmapStorage, HighPerfMmapConfig};
// 2) 若 bench 是 bin crate，且 main.rs 同目录有 `mod high_perf_mmap_storage;`
//    则使用：
// mod high_perf_mmap_storage; use high_perf_mmap_storage::{HighPerfMmapStorage, HighPerfMmapConfig};
// 3) 大多数项目：直接：
use flux_collaboration_rust_cache::high_perf_mmap_storage::{HighPerfMmapStorage, HighPerfMmapConfig};

#[cfg(unix)]
fn get_page_faults() -> Option<(u64, u64)> {
    // 使用 getrusage 获取次/主缺页
    use libc::{getrusage, rusage, RUSAGE_SELF};
    unsafe {
        let mut ru: rusage = std::mem::zeroed();
        if getrusage(RUSAGE_SELF, &mut ru) == 0 {
            let minflt = ru.ru_minflt as u64; // Minor page faults
            let majflt = ru.ru_majflt as u64; // Major page faults
            return Some((minflt, majflt));
        }
    }
    None
}
#[cfg(not(unix))]
fn get_page_faults() -> Option<(u64, u64)> { None }

#[derive(Clone, Debug)]
struct Args {
    dir: PathBuf,
    writers: usize,
    readers: usize,
    secs: u64,
    keyspace: u64,
    vmean: usize,
    vmax: usize,
    batch: usize,
    profile: String,
    prefetch: Option<bool>,
}
fn parse_args() -> Args {
    let mut a = std::env::args().skip(1);
    let mut dir = PathBuf::from("./benchdata");
    let mut writers = 2usize;
    let mut readers = 4usize;
    let mut secs = 120u64;
    let mut keyspace = 100_000u64;
    let mut vmean = 1024usize;
    let mut vmax = 4096usize;
    let mut batch = 32usize;
    let mut profile = "dev".to_string();
    let mut prefetch: Option<bool> = None;

    while let Some(k) = a.next() {
        match k.as_str() {
            "--dir" => dir = PathBuf::from(a.next().expect("--dir value")),
            "--writers" => writers = a.next().unwrap().parse().unwrap(),
            "--readers" => readers = a.next().unwrap().parse().unwrap(),
            "--secs" => secs = a.next().unwrap().parse().unwrap(),
            "--keyspace" => keyspace = a.next().unwrap().parse().unwrap(),
            "--vmean" => vmean = a.next().unwrap().parse().unwrap(),
            "--vmax" => vmax = a.next().unwrap().parse().unwrap(),
            "--batch" => batch = a.next().unwrap().parse().unwrap(),
            "--profile" => profile = a.next().unwrap(),
            "--prefetch" => {
                let v = a.next().unwrap();
                prefetch = Some(matches!(v.as_str(), "1"|"true"|"on"))
            },
            other => panic!("unknown arg: {}", other),
        }
    }

    Args { dir, writers, readers, secs, keyspace, vmean, vmax, batch, profile, prefetch }
}

fn config_from_profile(mut cfg: HighPerfMmapConfig, profile: &str) -> HighPerfMmapConfig {
    match profile {
        // 轻量设备/笔记本
        "dev" => {
            cfg.l1_cache_size_limit = 128 * 1024 * 1024; // 128MB
            cfg.l1_cache_entry_limit = 200_000;
            cfg.l2_cache_size_limit = 0; // 若未实现 L2，可忽略
            cfg.prefetch_queue_size = 4096;
        }
        // 中等服务器（64~128GB 内存）
        "med" => {
            cfg.l1_cache_size_limit = 512 * 1024 * 1024; // 512MB
            cfg.l1_cache_entry_limit = 1_000_000;
            cfg.prefetch_queue_size = 16_384;
        }
        // 高内存/高并发
        "heavy" => {
            cfg.l1_cache_size_limit = 2 * 1024 * 1024 * 1024; // 2GB
            cfg.l1_cache_entry_limit = 2_000_000;
            cfg.prefetch_queue_size = 65_536;
        }
        _ => {}
    }
    cfg
}

#[derive(Default, Clone)]
struct ThreadStats {
    write_attempts: u64,
    write_success: u64,
    read_attempts: u64,
    read_hits: u64,
    delete_attempts: u64,
    delete_success: u64,
    batch_attempts: u64,
    batch_success: u64,
    read_lat_us: Vec<u64>,
    write_lat_us: Vec<u64>,
}

fn percentile_us(v: &mut [u64], p: f64) -> u64 {
    if v.is_empty() { return 0; }
    v.sort_unstable();
    let n = v.len();
    let pos = ((n as f64 - 1.0) * p).round() as usize;
    v[pos]
}

fn rand_value(rng: &mut StdRng, vmean: usize, vmax: usize) -> Vec<u8> {
    let len = if vmax <= vmean { vmean } else {
        // 三角分布近似：更多生成接近 vmean 的大小
        let a = rng.gen_range(vmean/2..=vmax);
        let b = rng.gen_range(vmean/2..=vmax);
        std::cmp::min(a, b)
    };
    let len = std::cmp::max(32, len);
    let mut v = vec![0u8; len];
    rng.fill(&mut v[..]);
    v
}

fn main() -> std::io::Result<()> {
    let args = parse_args();
    fs::create_dir_all(&args.dir)?;

    
    let mut cfg = HighPerfMmapConfig::default();
    cfg = config_from_profile(cfg, &args.profile);
    if let Some(pf) = args.prefetch { cfg.enable_prefetch = pf; }

    // 推荐：首文件 512MB，步长 128MB（可按需改）
    cfg.initial_file_size = 1024 * 1024 * 1024 * 10;
    cfg.growth_step = 1024 * 1024 * 1024;
    cfg.max_file_size = 100 * 1024 * 1024 * 1024; // 100GB
    cfg.enable_prefetch = false;
    // cfg.auto_purge_after_secs = None;

    println!("== Config ==\n{:?}", cfg);

    let store = Arc::new(HighPerfMmapStorage::new(args.dir.clone(), cfg.clone())?);
    store.start_background_tasks();
    let stats_before = store.get_stats();
    // 运行标志与全局 key 计数
    let running = Arc::new(AtomicBool::new(true));
    let next_id = Arc::new(AtomicU64::new(0));

    // 基准时间窗口
    let bench_secs = args.secs;

    // writers
    let mut handles = vec![];
    for t in 0..args.writers {
        let s = store.clone();
        let running = running.clone();
        let next_id = next_id.clone();
        let vmean = args.vmean;
        let vmax = args.vmax;
        let mut ts = ThreadStats::default();
        let h = thread::spawn(move || {
            let mut rng = StdRng::from_os_rng();
            while running.load(Ordering::Relaxed) {
                let id = next_id.fetch_add(1, Ordering::Relaxed) %  args.keyspace;
                let key = format!("k_{}", id);
                let val = rand_value(&mut rng, vmean, vmax);
                ts.write_attempts += 1;
                let st = Instant::now();
                match s.write(&key, &val) {
                    Ok(_) => {
                        let us = st.elapsed().as_micros() as u64;
                        ts.write_success += 1;
                        ts.write_lat_us.push(us);
                    }
                    Err(_) => {
                        // 失败时不记录延迟，按照 attempts 统计
                    }
                }
            }
            ts
        });
        handles.push(h);
    }

    // readers（混合读/删/批量读）
    for t in 0..args.readers {
        let s = store.clone();
        let running = running.clone();
        let next_id = next_id.clone();
        let batch_n = args.batch;
        let mut ts = ThreadStats::default();
        let h = thread::spawn(move || {
            let mut rng = StdRng::from_os_rng();
            while running.load(Ordering::Relaxed) {
                let dice: u8 = rng.gen_range(0..100);
                if dice < 80 { // read 单读
                    let hi = next_id.load(Ordering::Relaxed).max(1);
                    let id = rng.gen_range(0..hi.min( args.keyspace ));
                    let key = format!("k_{}", id);
                    ts.read_attempts += 1;
                    let st = Instant::now();
                    match s.read(&key) {
                        Ok(Some(_)) => {
                            let us = st.elapsed().as_micros() as u64;
                            ts.read_hits += 1;
                            ts.read_lat_us.push(us);
                        }
                        Ok(None) | Err(_) => {
                            // miss 或错误：只统计尝试次数
                        }
                    }
                } else if dice  < 100 { // delete
                    ts.delete_attempts += 1;
                    let hi = next_id.load(Ordering::Relaxed).max(1);
                    let id = rng.gen_range(0..hi.min( args.keyspace ));
                    let key = format!("k_{}", id);
                    if matches!(s.delete(&key), Ok(true)) {
                        ts.delete_success += 1;
                    }
                } /*else { // batch read
                    let hi = next_id.load(Ordering::Relaxed).max(1);
                    let mut keys = Vec::with_capacity(batch_n);
                    for _ in 0..batch_n { 
                        let id = rng.gen_range(0..hi.min( args.keyspace ));
                        keys.push(format!("k_{}", id));
                    }
                    let st = Instant::now();
                    let _ = s.read_batch(&keys);
                    let us = st.elapsed().as_micros() as u64;
                    ts.batch_attempts += 1;
                    ts.batch_success += 1; // treat as hit when batch succeeds
                    ts.read_lat_us.push(us); // 将一次批量当作一次读事件计时
                }*/
            }
            ts
        });
        handles.push(h);
    }

    // 统计开始前的 page faults
    let pf_before = get_page_faults();

    // 运行
    let t0 = Instant::now();
    while t0.elapsed().as_secs() < bench_secs {
        thread::sleep(Duration::from_millis(200));
    }
    running.store(false, Ordering::Relaxed);

    // 汇总
    let mut total = ThreadStats::default();
    for h in handles {
        let ts = h.join().unwrap();
        total.write_attempts += ts.write_attempts;
        total.write_success += ts.write_success;
        total.read_attempts += ts.read_attempts;
        total.read_hits += ts.read_hits;
        total.delete_attempts += ts.delete_attempts;
        total.delete_success += ts.delete_success;
        total.batch_attempts += ts.batch_attempts;
        total.batch_success += ts.batch_success;
        total.read_lat_us.extend(ts.read_lat_us);
        total.write_lat_us.extend(ts.write_lat_us);
    }

    // 刷盘、保存索引、尝试一次 GC
    let _ = store.flush();
    let _ = store.save_index();
    let _ = store.garbage_collect();

    // page faults
    let pf_after = get_page_faults();

    // 输出指标
    let stats_after = store.get_stats();
    let mem_stats = store.get_memory_stats();

    let reads_success = stats_after.total_reads.saturating_sub(stats_before.total_reads);
    let writes_success = stats_after.total_writes.saturating_sub(stats_before.total_writes);
    let total_success_ops = reads_success
        .saturating_add(writes_success)
        .saturating_add(total.delete_success)
        .saturating_add(total.batch_success);

    let total_attempt_ops = total.read_attempts
        .saturating_add(total.write_attempts)
        .saturating_add(total.delete_attempts)
        .saturating_add(total.batch_attempts);

    let secs = bench_secs as f64;
    let reads_per_sec = reads_success as f64 / secs;
    let writes_per_sec = writes_success as f64 / secs;
    let deletes_per_sec = total.delete_success as f64 / secs;
    let total_per_sec = total_success_ops as f64 / secs;

    println!("\n==== RESULTS ====\nDuration: {} s", bench_secs);
    println!(
        "Ops (attempted): total={} (reads={} writes={} deletes={} batches={})",
        total_attempt_ops,
        total.read_attempts,
        total.write_attempts,
        total.delete_attempts,
        total.batch_attempts
    );
    println!(
        "Ops (successful): total={} (reads={} writes={} deletes={} batches={})",
        total_success_ops,
        reads_success,
        writes_success,
        total.delete_success,
        total.batch_success
    );
    println!(
        "Throughput (successful ops): reads={:.2}/s writes={:.2}/s deletes={:.2}/s total={:.2}/s",
        reads_per_sec,
        writes_per_sec,
        deletes_per_sec,
        total_per_sec
    );

    // 计算分位
    let mut r = total.read_lat_us.clone();
    let mut w = total.write_lat_us.clone();
    let rp50 = percentile_us(&mut r, 0.50);
    let rp95 = percentile_us(&mut r, 0.95);
    let rp99 = percentile_us(&mut r, 0.99);
    let wp50 = percentile_us(&mut w, 0.50);
    let wp95 = percentile_us(&mut w, 0.95);
    let wp99 = percentile_us(&mut w, 0.99);
    println!("Latency (us): READ p50={} p95={} p99={} | WRITE p50={} p95={} p99={}", rp50, rp95, rp99, wp50, wp95, wp99);

    // page faults 输出
    if let (Some((bmin, bmaj)), Some((amin, amaj))) = (pf_before, pf_after) {
        println!("Page Faults: minor={} major={}", amin.saturating_sub(bmin), amaj.saturating_sub(bmaj));
    } else {
        println!("Page Faults: (unavailable on this platform)");
    }

    // 打印内部统计（如有）
    println!("\nInternal Stats: {:?}", stats_after);
    println!(
        "Memory Usage: total_heap={:.2} MB, L1_cache={:.2} MB (entries={}), mmap_size={:.2} MB, under_pressure={}",
        mem_stats.total_heap_bytes as f64 / (1024.0 * 1024.0),
        mem_stats.l1_cache_bytes as f64 / (1024.0 * 1024.0),
        mem_stats.l1_cache_entries,
        mem_stats.mmap_bytes as f64 / (1024.0 * 1024.0),
        mem_stats.under_memory_pressure
    );

    store.stop_background_tasks();
    Ok(())
}
