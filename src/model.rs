
use std::fs::File;
use std::path::PathBuf;
use std::collections::HashMap;

pub type EntryKey = String;
pub type BagKey = String;


pub struct StoreArchive {
    pub bags: HashMap<BagKey, Bag>
}

pub struct BagRootEntry {
    pub key: BagKey,
    pub root_path: String
}

pub struct Bag {
    pub key: BagKey,
    pub entries: HashMap<EntryKey, IMEntry>,
    pub root_path: PathBuf,
    pub active_path: PathBuf,
    pub file_handle: File
}

pub struct IMEntry {
    pub key: EntryKey,
    pub file: PathBuf,
    pub offset: u64,
    pub size: u64
}

pub struct BaseEntryRebuildData {
    pub key: String,
    pub size: u64,
    pub deleted: bool
}
pub struct OffsetEntryRebuildData {
    pub key: String,
    pub size: u64,
    pub offset: u64,
    pub deleted: bool,
}
impl BaseEntryRebuildData {
    pub fn with_offset(self, offset: u64) -> OffsetEntryRebuildData {
        OffsetEntryRebuildData {
            key: self.key,
            size: self.size,
            offset,
            deleted: self.deleted,
        }
    }
}

pub struct BagStoreFileData {
    pub is_sealed: bool,
    pub rebuild_data: Vec<OffsetEntryRebuildData>,
    pub next_file: Option<PathBuf>
}

pub struct KVPair {
    pub key: EntryKey,
    pub value: Vec<u8>
}

pub struct ODIntermediateEntry {
    pub key: EntryKey,
    pub value: Vec<u8>,
    pub expiry: Option<u128>,
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
    pub fn make_expiring(key: EntryKey, value: Vec<u8>, expiry: u128) -> Self {
        Self {
            key,
            value,
            expiry: Some(expiry),
            is_tombstone: false,
        }
    }
}


