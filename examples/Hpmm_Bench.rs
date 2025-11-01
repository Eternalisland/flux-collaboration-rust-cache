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
    let mut secs = 60u64;
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
    writes: u64,
    reads: u64,
    deletes: u64,
    batches: u64,
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


    #[cfg(debug_assertions)]
    std::thread::spawn(|| loop {
        std::thread::sleep(std::time::Duration::from_secs(10));
        let deadlocks = parking_lot::deadlock::check_deadlock();
        if !deadlocks.is_empty() {
            eprintln!("⚠️ detected deadlocks: {}", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                eprintln!("deadlock #{}", i);
                for t in threads {
                    eprintln!(" - thread id: {:?}\n - backtrace:\n{:?}", t.thread_id(), t.backtrace());
                }
            }
        }
    });
    
    let mut cfg = HighPerfMmapConfig::default();
    cfg = config_from_profile(cfg, &args.profile);
    if let Some(pf) = args.prefetch { cfg.enable_prefetch = pf; }

    // 推荐：首文件 512MB，步长 128MB（可按需改）
    cfg.initial_file_size = 1024 * 1024 * 1024;
    cfg.growth_step = 512 * 1024 * 1024;
    cfg.max_file_size = 64 * 1024 * 1024 * 1024; // 64GB

    println!("== Config ==\n{:?}", cfg);

    let store = Arc::new(HighPerfMmapStorage::new(args.dir.clone(), cfg.clone())?);
    store.start_background_tasks();
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
            let mut rng = StdRng::from_entropy();
            while running.load(Ordering::Relaxed) {
                let id = next_id.fetch_add(1, Ordering::Relaxed) %  args.keyspace;
                let key = format!("k_{}", id);
                let val = rand_value(&mut rng, vmean, vmax);
                let st = Instant::now();
                let _ = s.write(&key, &val);
                let us = st.elapsed().as_micros() as u64;
                ts.writes += 1;
                ts.write_lat_us.push(us);
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
            let mut rng = StdRng::from_entropy();
            while running.load(Ordering::Relaxed) {
                let dice: u8 = rng.gen_range(0..100);
                if dice < 80 { // read 单读
                    let hi = next_id.load(Ordering::Relaxed).max(1);
                    let id = rng.gen_range(0..hi.min( args.keyspace ));
                    let key = format!("k_{}", id);
                    let st = Instant::now();
                    let _ = s.read(&key);
                    let us = st.elapsed().as_micros() as u64;
                    ts.reads += 1;
                    ts.read_lat_us.push(us);
                } else if dice < 90 { // delete
                    let hi = next_id.load(Ordering::Relaxed).max(1);
                    let id = rng.gen_range(0..hi.min( args.keyspace ));
                    let key = format!("k_{}", id);
                    let _ = s.delete(&key);
                    ts.deletes += 1;
                } else { // batch read
                    let hi = next_id.load(Ordering::Relaxed).max(1);
                    let mut keys = Vec::with_capacity(batch_n);
                    for _ in 0..batch_n { 
                        let id = rng.gen_range(0..hi.min( args.keyspace ));
                        keys.push(format!("k_{}", id));
                    }
                    let st = Instant::now();
                    let _ = s.read_batch(&keys);
                    let us = st.elapsed().as_micros() as u64;
                    ts.batches += 1;
                    ts.read_lat_us.push(us); // 将一次批量当作一次读事件计时
                }
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
    for h in handles { let ts = h.join().unwrap();
        total.writes += ts.writes; total.reads += ts.reads; total.deletes += ts.deletes; total.batches += ts.batches;
        total.read_lat_us.extend(ts.read_lat_us); total.write_lat_us.extend(ts.write_lat_us);
    }

    // 刷盘、保存索引、尝试一次 GC
    let _ = store.flush();
    let _ = store.save_index();
    let _ = store.garbage_collect();

    // page faults
    let pf_after = get_page_faults();

    // 输出指标
    let secs = bench_secs as f64;
    let ops = (total.reads + total.writes + total.deletes + total.batches) as f64;
    let rps = (total.reads as f64) / secs;
    let wps = (total.writes as f64) / secs;

    println!("\n==== RESULTS ====\nDuration: {} s", bench_secs);
    println!("Ops: total={} (reads={} writes={} deletes={} batches={})",
             total.reads + total.writes + total.deletes + total.batches,
             total.reads, total.writes, total.deletes, total.batches);
    println!("Throughput: reads={:.2}/s writes={:.2}/s total={:.2}/s", rps, wps, ops / secs);

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
    let stats = store.get_stats();
    println!("\nInternal Stats: {:?}", stats);

    store.stop_background_tasks();
    Ok(())
}
