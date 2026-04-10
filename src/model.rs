
use std::fs::File;
use std::collections::HashMap;

pub type EntryKey = String;
pub type BagKey = String;


pub struct StoreArchive {
    pub bag: HashMap<BagKey, Bag>
}

pub struct BagRootEntry {
    pub key: BagKey,
    pub root_path: String
}

pub struct Bag {
    pub key: BagKey,
    pub entries: HashMap<EntryKey, IMEntry>,
    pub root_path: String,
    pub active_path: String,
    pub file_handle: File
}

pub struct IMEntry {
    pub key: EntryKey,
    pub file: String,
    pub offset: u64,
    pub size: u64
}

pub struct KVPair {
    pub key: EntryKey,
    pub value: Vec<u8>
}

pub struct ODIntermediateEntry {
    pub key: EntryKey,
    pub value: Vec<u8>,
    pub expiry: Option<u64>,
    pub is_tombstone: bool,
}
impl ODIntermediateEntry {
    pub fn make_tombstone(key: EntryKey) -> Self {
        Self {
            key,
            value: Vec::new(),
            expiry: None,
            is_tombstone: true,
        }
    }
    pub fn make_update(key: EntryKey, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            expiry: None,
            is_tombstone: false,
        }
    }
}


#[derive(Debug)]
pub enum EngineError {
    IOReadError,
    IOWriteError,
    NoSuchKeyError,
    DeletedEntryError,
    ExpiredEntryError
}

// pub struct ODEntry {
//     pub crc: Vec<u8>,
//     pub timestamp: u128,
//     pub flags: u8,
//     pub key_size: u32,
//     pub val_size: u64,
//     pub expiry: Option<u64>
//     pub key: EntryKey,
//     pub value: String,
// }
