use dashmap::DashMap;
use std::sync::Arc;

fn main() {
    let map: Arc<DashMap<String, (u64, u64)>> = Arc::new(DashMap::new());
    map.insert("test".to_string(), (1u64, 2u64));
    println!("DashMap test successful");
}