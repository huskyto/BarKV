
use crc32fast::Hasher;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;


pub const MAGIC_BYTES: [u8; 5] = [0x42, 0x61, 0x72, 0x4B, 0x56];
pub const VERSION: [u8; 3] = [0x00, 0x00, 0x01];

pub fn current_timestamp_sec() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(time) => time.as_secs(),
        Err(_e) => 0,
    }
}

pub fn current_timestamp() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(time) => time.as_millis(),
        Err(_e) => 0,
    }
}

pub fn calculate_crc(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

pub fn calculate_incremental_crc(initial_crc: u32, appended_data: &[u8]) -> u32 {
    let mut hasher = Hasher::new_with_initial(initial_crc);
    hasher.update(appended_data);
    hasher.finalize()
}
