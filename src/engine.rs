
use std::fs;
use std::io::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::collections::HashMap;
use thiserror::Error;

use crate::io;
use crate::util;
use crate::model::BagStoreFileHeaders;
use crate::upkeep;
use crate::encoding;
use crate::encoding::EncodingError;
use crate::encoding::SizeMismatchType;
use crate::model::Bag;
use crate::model::KVPair;
use crate::model::BagKey;
use crate::model::IMEntry;
use crate::model::EntryKey;
use crate::model::StoreArchive;
use crate::model::ODIntermediateEntry;
use crate::validation;
use crate::validation::ValidationFailure;


pub(crate) const STORE_FILENAME: &str = "barkv.store";
const MIN_LOCK_SIZE: usize = 10_000;        // TODO move to config

pub struct BarKVEngine {
    pub(super) store: RwLock<StoreArchive>,
    pub(super) root_path: PathBuf,
    closed: AtomicBool,
}

impl BarKVEngine {

            // VALUES //

    pub fn get(&self, bag_key: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        self.check_open()?;
        self.with_bag(bag_key, |bag| {
            Self::retrieve_value(key, bag)
        })
    }

    pub fn set(&self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        self.check_open()?;
        let new_size = self.with_bag(bag_key, |bag| {
            Self::insert_value(bag, key, value)
        })?;
    
        self.lock_if_needed(bag_key, new_size)?;
        Ok(())
    }
    
    pub fn delete(&self, bag_key: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
        self.check_open()?;
        let new_size = self.with_bag(bag_key, |bag| {
            Self::remove_value(key, bag)
        })?;
        
        self.lock_if_needed(bag_key, new_size)?;
        Ok(())
    }

    pub fn exists(&self, bag_key: &BagKey, key: &EntryKey) -> Result<bool, EngineError> {
        self.check_open()?;
        self.with_bag(bag_key, |bag| {
            Ok(bag.entries.contains_key(key))
        })
    }

    pub fn list_keys(&self, bag_key: &BagKey) -> Result<Vec<EntryKey>, EngineError> {
        self.check_open()?;
        self.with_bag(bag_key, |bag| {
            let keys = bag.entries.keys().cloned().collect();
            Ok(keys)
        })
    }


            // BAGS //

    pub fn create_bag(&self, bag_key: &BagKey) -> Result<(), EngineError> {
        self.check_open()?;
        let mut store = self.store.write().map_err(|_| EngineError::LockPoisoned)?;

        if store.bags.contains_key(bag_key) {
            return Err(EngineError::BagAlreadyExistsError(bag_key.clone()))
        }

        if !self.root_path.is_dir(){
            return Err(EngineError::RootPathError);
        }

        let bag_filename = format!("{bag_key}-0.bkv");
        let bag_root_path = self.root_path.join(bag_filename);
        let mut file_handle = io::create_file_to_append(&bag_root_path)?;

                // Init bag store file with header
        let header_data = BagStoreFileHeaders::for_init(0);
        let header = encoding::encode_bag_store_file_header(&header_data)?;
        io::write_all(&mut file_handle, &header)?;

        let new_bag = Bag {
            key: bag_key.clone(),
            entries: HashMap::new(),
            root_path: bag_root_path.clone(),
            active_path: bag_root_path,
            file_handle,
            current_file_id: 0,
        };

        let bag_arc = Arc::new(Mutex::new(new_bag));
        store.bags.insert(bag_key.clone(), bag_arc);

                // Update the store file.
        let encoded_store = encoding::encode_store_file(&store)?;
        io::overwrite(&self.get_store_file_path(), &encoded_store)?;

        Ok(())
    }

    pub fn drop_bag(&self, bag_key: &BagKey) -> Result<(), EngineError> {
        self.check_open()?;
        let mut store = self.store.write().map_err(|_| EngineError::LockPoisoned)?;

        let bag_arc = match store.bags.remove(bag_key) {
            Some(b) => b,
            None => return Err(EngineError::NoSuchBagKeyError(bag_key.clone())),
        };


        let encoded_store = encoding::encode_store_file(&store)?;
        io::overwrite(&self.get_store_file_path(), &encoded_store)?;

                // Remove bag files
        let bag_lock = bag_arc.lock().map_err(|_| EngineError::LockPoisoned)?;
        let bag_files = upkeep::get_bag_file_chain(&bag_lock)?;
        for file in bag_files {
            fs::remove_file(file)?;
        }

        Ok(())
    }

    pub fn len_bag(&self, bag_key: &BagKey) -> Result<usize, EngineError> {
        self.check_open()?;
        self.with_bag(bag_key, |bag| {
            Ok(bag.entries.len())
        })
    }

    pub fn list_bags(&self) -> Result<Vec<BagKey>, EngineError> {
        self.check_open()?;
        let store = self.store.read().map_err(|_| EngineError::LockPoisoned)?;
        Ok(store.bags.keys()
                .cloned()
                .collect())
    }


            // LIFECYCLE //

    pub fn open(path: &str) -> Result<BarKVEngine, EngineError> {
        let path = PathBuf::from(path);
        if !path.is_dir() {
            return Err(EngineError::RootPathError);
        }

        let root_file_path = path.join(STORE_FILENAME);
        if !root_file_path.is_file() {
            return Err(EngineError::RootFileNotFound);
        }

        let mut root_file_handle = io::open_file_for_read(&root_file_path)?;
        let root_file_data = io::read_all_file(&mut root_file_handle)?;
        let bag_roots = encoding::decode_store_roots(&root_file_data)?;
        let mut bags = HashMap::new();

        for bag_root in bag_roots {
            let rebuilt_bag = upkeep::rebuild_bag_history(&bag_root)?;
            bags.insert(bag_root.key.clone(), Arc::new(Mutex::new(rebuilt_bag)));
        }

        let engine = BarKVEngine {
            store: RwLock::new(StoreArchive { bags }),
            root_path: path,
            closed: AtomicBool::new(false),
        };

        Ok(engine)
    }

    pub fn create(path: &str) -> Result<BarKVEngine, EngineError> {
        let path = PathBuf::from(path);
        if !path.is_dir() {
            return Err(EngineError::RootPathError)
        }

        if fs::read_dir(&path)?.next().is_some() {
            return Err(EngineError::RootPathNotEmpty)
        }

        let root_file_path = path.join(STORE_FILENAME);
        let mut root_file = io::create_file_to_append(&root_file_path)?;

        let res = BarKVEngine {
            store: RwLock::new(StoreArchive {
                bags: HashMap::new(),
            }),
            root_path: path,
            closed: AtomicBool::new(false),
        };

        let init_data = {
            let store = &res.store.read()
                    .map_err(|_| EngineError::LockPoisoned)?;
            encoding::encode_store_file(store)?
        };

        io::write_all(&mut root_file, &init_data)?;
        Ok(res)
    }

    pub fn open_or_create(path: &str) -> Result<BarKVEngine, EngineError> {
        match Self::open(path) {
            Ok(engine) => Ok(engine),
            Err(e) => {
                match e {
                    EngineError::RootFileNotFound => Self::create(path),
                    e => Err(e)
                }
            },
        }
    }

    pub fn close(&self) -> Result<(), EngineError> {
        self.check_open()?;
        self.closed.store(true, Ordering::Release);

        let bag_keys: Vec<BagKey> = {
            let store = self.store.read().map_err(|_| EngineError::LockPoisoned)?;
            store.bags.keys().cloned().collect()
        };
        for bag_key in bag_keys {
            self.with_bag(&bag_key, |bag| {
                io::close_file(&mut bag.file_handle)?;
                Ok(())
            })?;
        }

        Ok(())
    }

    pub fn compact_active(&self) -> Vec<(BagKey, Result<(), EngineError>)> {
        if let Err(e) = self.check_open() {
            return vec![("".to_string(), Err(e))];
        }

        let bag_keys: Vec<BagKey> = {
            let store = match self.store.read() {
                Ok(s) => s,
                Err(_) => return vec![("".to_string(), Err(EngineError::LockPoisoned))],
            };
            store.bags.keys().cloned().collect()
        };
        let mut res = Vec::new();
        for bag_key in bag_keys {
            let _ = self.with_bag(&bag_key, |bag| {
                res.push((bag_key.clone(), upkeep::compact_partial(bag, None).map(|_| ())));
                Ok(())
            });
        }

        res
    }

    pub fn full_compaction(&self) -> Vec<(BagKey, Result<(), EngineError>)>  {
        if let Err(e) = self.check_open() {
            return vec![("".to_string(), Err(e))];
        }

        let bag_keys: Vec<BagKey> = {
            let store = match self.store.read() {
                Ok(s) => s,
                Err(_) => return vec![("".to_string(), Err(EngineError::LockPoisoned))],
            };
            store.bags.keys().cloned().collect()
        };
        let mut res = Vec::new();
        for bag_key in bag_keys {
            let _ = self.with_bag(&bag_key, |bag| {
                res.push((bag_key.clone(), upkeep::full_compaction(bag, &self.root_path).map(|_| ())));
                Ok(())
            });
        }

        res
    }


            // ATOMIC //
            
    pub fn get_or_set(&self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<Vec<u8>, EngineError> {
        self.check_open()?;
        let (value, new_size) = self.with_bag(bag_key, |bag| {
            if bag.entries.contains_key(key) {
                Ok((Self::retrieve_value(key, bag)?, None))
            } else {
                
                Ok((value.to_vec(), Some(Self::insert_value(bag, key, value)?)))
            }
        })?;

        if let Some(new_size) = new_size { self.lock_if_needed(bag_key, new_size)?; }
        Ok(value)
    }

    pub fn update_if_different(&self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        self.check_open()?;
        let new_size = self.with_bag(bag_key, |bag| {
            let current_value = Self::retrieve_value(key, bag)?;
            if current_value != value {
                Ok(Some(Self::insert_value(bag, key, value)?))
            }
            else { Ok(None) }
        })?;

        if let Some(new_size) = new_size {
            self.lock_if_needed(bag_key, new_size)?;
        }
        Ok(())
    }

    pub fn get_and_delete(&self, bag_key: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        self.check_open()?;
        let (value, new_size) = self.with_bag(bag_key, |bag| {
            let value = Self::retrieve_value(key, bag)?;
            Ok((value, Self::remove_value(key, bag)?))
        })?;

        self.lock_if_needed(bag_key, new_size)?;
        Ok(value)
    }


            // BATCH OPS //

    pub fn get_many(&self, bag_key: &BagKey, keys: &[&EntryKey]) -> Result<Vec<KVPair>, EngineError> {
        self.check_open()?;
        self.with_bag(bag_key, |bag| {
            let mut res = Vec::new();

            for &key in keys {
                if !bag.entries.contains_key(key) { continue }

                let value = Self::retrieve_value(key, bag)?;

                res.push(KVPair { key: key.into(), value });
            }

            Ok(res)
        })
    }

    pub fn set_many(&self, bag_key: &BagKey, pairs: Vec<KVPair>) -> Result<(), EngineError> {
        self.check_open()?;
        let mut new_size = 0;

        self.with_bag(bag_key, |bag| {
            for pair in pairs {
                new_size = Self::insert_value(bag, &pair.key, &pair.value)?;
            }
            Ok(())
        })?;

        self.lock_if_needed(bag_key, new_size)?;
        Ok(())
    }

    pub fn delete_many(&self, bag_key: &BagKey, keys: &[&EntryKey]) -> Result<(), EngineError> {
        self.check_open()?;
        let mut new_size = 0;

        self.with_bag(bag_key, |bag| {
            for &key in keys {
                if !bag.entries.contains_key(key) { continue }
        
                new_size = Self::remove_value(key, bag)?;
            }
            Ok(())
        })?;

        self.lock_if_needed(bag_key, new_size)?;
        Ok(())
    }


            // TTL
    
    pub fn set_with_expiry(&self, bag_key: &BagKey, key: &EntryKey,
                value: &[u8], ttl: u128) -> Result<(), EngineError> {
        self.check_open()?;
        let new_size = self.with_bag(bag_key, |bag| {
            let expiry = util::current_timestamp() + ttl;
            let od_in_entry = ODIntermediateEntry::make_expiring(key.into(), value.to_vec(), expiry);
            let encoded_entry = encoding::encode_od_entry(&od_in_entry)?;
            let offset = io::append(&mut bag.file_handle, &encoded_entry)?;

            let im_entry = IMEntry {
                key: key.into(),
                file: bag.active_path.clone(),
                offset,
                size: encoded_entry.len() as u64,
            };

            bag.entries.insert(key.clone(), im_entry);

            Ok(offset as usize + encoded_entry.len())
        })?;

        self.lock_if_needed(bag_key, new_size)?;
        Ok(())
    }

    pub fn ttl(&self, bag_key: &BagKey, key: &EntryKey) -> Result<u128, EngineError> {
        self.check_open()?;
        self.with_bag(bag_key, |bag| {
            let entry = bag.entries.get(key)
                    .ok_or_else(|| EngineError::NoSuchEntryKeyError(key.to_string()))?;

            let header_size = encoding::KV_ENTRY_HEADER_BASE_SIZE + 8;
            if entry.size < header_size as u64 {
                return Err(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))?
            }

            let read_chunk = if entry.file == bag.active_path {
                let (offset, size) = (entry.offset, entry.size);
                io::read_chunk(&mut bag.file_handle, offset, size)?
            } else {
                let mut file_handle = io::open_file_for_read(&entry.file)?;
                io::read_chunk(&mut file_handle, entry.offset, entry.size)?
            };        
            let expiry = encoding::get_expiry_entry_data(&read_chunk)?;

            Ok(expiry)
        })
    }

    pub fn persist(&self, bag_key: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
        self.check_open()?;
        let new_size = self.with_bag(bag_key, |bag| {
            let value = Self::retrieve_value(key, bag)?;
            Self::insert_value(bag, key, &value)
        })?;

        self.lock_if_needed(bag_key, new_size)?;
        Ok(())
    }


            // STATE // TODO Extension.
    
    pub fn stats(&self) {
        todo!()     // TODO Stats model
    }

    pub fn validate(&self) -> Vec<ValidationFailure> {
        validation::validate(self)
    }


            // INTERNAL //

    fn get_bag_arc(&self, bag_key: &BagKey) -> Result<Arc<Mutex<Bag>>, EngineError> {
        let store = self.store.read().map_err(|_| EngineError::LockPoisoned)?;
        Ok(store.bags.get(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?
                .clone())
    }

    fn with_bag<T, F>(&self, bag_key: &BagKey, f: F) -> Result<T, EngineError>
    where
        F: FnOnce(&mut Bag) -> Result<T, EngineError>
    {
        let bag_arc = self.get_bag_arc(bag_key)?;
        let mut bag = bag_arc.lock().map_err(|_| EngineError::LockPoisoned)?;
        f(&mut bag)
    }

    fn check_open(&self) -> Result<(), EngineError> {
        if self.closed.load(Ordering::Acquire) {
            Err(EngineError::StoreClosed)
        } else {
            Ok(())
        }
    }

    fn lock_if_needed(&self, bag_key: &BagKey, new_size: usize) -> Result<(), EngineError> {
        if new_size >= MIN_LOCK_SIZE {
            self.lock_active(bag_key)
        } else { Ok(()) }
    }

    fn get_store_file_path(&self) -> PathBuf {
        self.root_path.join(STORE_FILENAME)
    }

    pub(super) fn lock_active(&self, bag_key: &BagKey) -> Result<(), EngineError> {
        let bag_arc = self.get_bag_arc(bag_key)?;
        let mut bag = bag_arc.lock().map_err(|_| EngineError::LockPoisoned)?;

        let updated_headers = BagStoreFileHeaders::for_locked(bag.current_file_id);
        let offset_data = upkeep::compact_partial(&mut bag, Some(updated_headers))?;

                // Create sealed helper file
        let next_file_path = upkeep::build_bag_path(&self.root_path,
                    bag_key, bag.current_file_id as usize + 1);
        upkeep::created_sealed_helper_file(&bag.active_path, offset_data, &next_file_path)?;

                // Update Bag
        let mut next_file_handle = io::create_file_to_append(&next_file_path)?;

        bag.current_file_id += 1;
        bag.active_path = next_file_path;

        let header_data = BagStoreFileHeaders::for_init(bag.current_file_id);
        let header = encoding::encode_bag_store_file_header(&header_data)?;
        io::write_all(&mut next_file_handle, &header)?;

        bag.file_handle = next_file_handle;

        Ok(())
    }

    fn retrieve_value(key: &EntryKey, bag: &mut Bag) -> Result<Vec<u8>, EngineError> {
        let entry = bag.entries.get(key)
            .ok_or_else(|| EngineError::NoSuchEntryKeyError(key.to_string()))?;

        let read_chunk = if entry.file == bag.active_path {
            let (offset, size) = (entry.offset, entry.size);
            io::read_chunk(&mut bag.file_handle, offset, size)?
        } else {
            let mut file_handle = io::open_file_for_read(&entry.file)?;
            io::read_chunk(&mut file_handle, entry.offset, entry.size)?
        };

        encoding::get_value_from_entry_data(&read_chunk).map_err(EngineError::from)
    }

    fn insert_value(bag: &mut Bag, key: &EntryKey, value: &[u8]) -> Result<usize, EngineError> {
        let od_in_entry = ODIntermediateEntry::make_update(key.into(), value.to_vec());
        let encoded_entry = encoding::encode_od_entry(&od_in_entry)?;
        let offset = io::append(&mut bag.file_handle, &encoded_entry)?;

        let im_entry = IMEntry {
            key: key.into(),
            file: bag.active_path.clone(),
            offset,
            size: encoded_entry.len() as u64,
        };

        bag.entries.insert(key.clone(), im_entry);
        Ok(offset as usize + encoded_entry.len())
    }

    fn remove_value(key: &String, bag: &mut Bag) -> Result<usize, EngineError> {
        if !bag.entries.contains_key(key) {
            return Err(EngineError::NoSuchEntryKeyError(key.to_string()));
        }

        let od_in_entry = ODIntermediateEntry::make_tombstone(key.into());
        let encoded_entry = encoding::encode_od_entry(&od_in_entry)?;
        let offset = io::append(&mut bag.file_handle, &encoded_entry)?;

        bag.entries.remove(key);

        Ok(offset as usize + encoded_entry.len())
    }

}



#[derive(Debug, Error)]
pub enum EngineError {
    #[error("EntryKey does not exist in bag: {0}")]
    NoSuchEntryKeyError(EntryKey),
    #[error("BagKey does not exist in store: {0}")]
    NoSuchBagKeyError(BagKey),
    #[error("Bag already exists in store: {0}")]
    BagAlreadyExistsError(BagKey),
    #[error("Entry was queried, but is deleted: {0}")]
    DeletedEntryError(EntryKey),
    #[error("Entry was queried, but is expired: {0}")]
    ExpiredEntryError(EntryKey),
    #[error("Root path doesn't exist, or has an error")]
    RootPathError,
    #[error("Root path for new store is not empty")]
    RootPathNotEmpty,
    #[error("Root file not found in path")]
    RootFileNotFound,
    #[error("Entry properties are inconsistent")]
    EntryConsistencyError,
    #[error("Store is closed")]
    StoreClosed,
    #[error("A lock was poisoned")]
    LockPoisoned,

    #[error("Wrapped encoding error: {0}")]
    WrappedEncodingError(#[from] EncodingError),
    #[error("Wrapped io error: {0}")]
    WrappedIOError(#[from] Error),
}
