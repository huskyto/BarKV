
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::collections::HashMap;

use crate::util;

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
    pub file_handle: File,
    pub current_file_id: usize
}

pub struct SealHelperFile {
    pub next_file: PathBuf,
    pub entries: Vec<OffsetEntryRebuildData>
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
pub struct OffsetEntryRebuildData {
    pub key: String,
    pub size: u64,
    pub offset: u64,
    pub deleted: bool,
}
impl OffsetEntryRebuildData {
    pub fn to_im_entry(&self, file: &Path) -> IMEntry {
        IMEntry {
            key: self.key.clone(),
            file: file.into(),
            offset: self.offset,
            size: self.size,
        }
    }
}

pub struct BagStoreFileHeaders {
    pub is_sealed: bool,
    pub is_locked: bool,
    pub is_deleted: bool,
}
impl BagStoreFileHeaders {
    pub fn for_init() -> Self {
        Self {
            is_sealed: false,
            is_locked: false,
            is_deleted: false,
        }
    }
    pub fn from_flags(flags: u8) -> Self {
        let is_deleted = (flags & 0b0000_0001) != 0;
        let is_sealed  = (flags & 0b0000_0010) != 0;
        let is_locked  = (flags & 0b0000_0100) != 0;

        Self { is_sealed, is_locked,is_deleted, }
    }
}

pub struct BagStoreFileData {
    pub headers: BagStoreFileHeaders,
    pub rebuild_data: Vec<OffsetEntryRebuildData>,
    pub next_file: Option<PathBuf>
}
impl BagStoreFileData {
    pub fn for_init() -> Self {
        Self {
            headers: BagStoreFileHeaders {
                is_sealed: false,
                is_locked: false,
                is_deleted: false,
            },
            rebuild_data: Vec::new(),
            next_file: None,
        }
    }
}

pub struct BagStoreFileDataIntermediateEntries {
    pub flags: u8,
    pub int_entries: Vec<ODIntermediateEntry>,
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
    pub timestamp: u64
}
impl ODIntermediateEntry {
    pub fn make_tombstone(key: EntryKey) -> Self {
        Self {
            key,
            value: Vec::new(),
            expiry: None,
            is_tombstone: true,
            timestamp: util::current_timestamp_sec()
        }
    }
    pub fn make_update(key: EntryKey, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            expiry: None,
            is_tombstone: false,
            timestamp: util::current_timestamp_sec()
        }
    }
    pub fn make_expiring(key: EntryKey, value: Vec<u8>, expiry: u128) -> Self {
        Self {
            key,
            value,
            expiry: Some(expiry),
            is_tombstone: false,
            timestamp: util::current_timestamp_sec()
        }
    }
    pub fn to_tombstone(self) -> Self {
        Self {
            key: self.key,
            value: Vec::new(),
            expiry: None,
            is_tombstone: true,
            timestamp: util::current_timestamp_sec()
        }
    }
}


