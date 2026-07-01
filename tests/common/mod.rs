#![allow(dead_code)]

use tempfile::TempDir;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use barkv::BarKV;

pub const BAG: &str = "test-bag";

pub fn new_engine() -> (TempDir, BarKV) {
    let dir = TempDir::new().unwrap();
    let engine = BarKV::create(dir.path().to_str().unwrap()).unwrap();
    (dir, engine)
}

pub fn new_engine_with_bag(bag: &str) -> (TempDir, BarKV) {
    let (dir, engine) = new_engine();
    engine.create_bag(&bag.to_string()).unwrap();
    (dir, engine)
}

pub fn reopen(dir: &TempDir) -> BarKV {
    BarKV::open(dir.path().to_str().unwrap()).unwrap()
}

/// Write enough unique entries to push the active file past MIN_LOCK_SIZE
/// (10 000 bytes), triggering at least `rotations` file rotations.
///
/// Each entry is ~(25 header + key_bytes + value_bytes).
/// With an 8-byte key and 100-byte value that is 133 bytes/entry.
/// ⌈10 000 / 133⌉ ≈ 76 entries per file, so `76 * rotations + margin`.
pub fn fill_to_rotations(engine: &BarKV, bag: &str, rotations: usize) {
    let value = vec![0xABu8; 100];
    let count = 80 * rotations;
    for i in 0..count {
        engine
            .set(&bag.to_string(), &format!("key-{i:05}"), &value)
            .unwrap();
    }
}

pub fn now_millis() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(time) => time.as_millis(),
        Err(_e) => 0,
    }
}
