
use std::fs;
use std::io::Error;
use std::path::PathBuf;
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
use crate::model::SealHelperFile;
use crate::model::ODIntermediateEntry;
use crate::validation;
use crate::validation::ValidationFailure;


pub(crate) const STORE_FILENAME: &str = "barkv.store";
const MIN_LOCK_SIZE: usize = 10_000;        // TODO move to config

pub struct BarKVEngine {
    pub(super) store: StoreArchive,
    pub(super) root_path: PathBuf         // This should be a folder
}

impl BarKVEngine {

            // VALUES //

    pub fn get(&mut self, bag_key: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;
        let entry = bag.entries.get(key)
                .ok_or_else(|| EngineError::NoSuchEntryKeyError(key.to_string()))?;

        let read_chunk = if entry.file == bag.active_path {
            io::read_chunk(&mut bag.file_handle, entry.offset, entry.size)?
        } else {
            let mut file_handle = io::open_file_for_read(&entry.file)?;
            io::read_chunk(&mut file_handle, entry.offset, entry.size)?
        };
        let value = encoding::get_value_from_entry_data(&read_chunk)?;

        Ok(value)
    }

    pub fn set(&mut self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;

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

        let new_size = offset as usize + encoded_entry.len();
        self.lock_if_needed(bag_key, new_size)?;

        Ok(())
    }

    fn lock_if_needed(&mut self, bag_key: &BagKey, new_size: usize) -> Result<(), EngineError> {
        if new_size >= MIN_LOCK_SIZE {
            self.lock_active(bag_key)
        } else { Ok(()) }
    }

    pub fn delete(&mut self, bag_key: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;
        
        if !bag.entries.contains_key(key) {
            return Err(EngineError::NoSuchEntryKeyError(key.to_string()));
        }

        let od_in_entry = ODIntermediateEntry::make_tombstone(key.into());
        let encoded_entry = encoding::encode_od_entry(&od_in_entry)?;
        let offset = io::append(&mut bag.file_handle, &encoded_entry)?;

        bag.entries.remove(key);

        let new_size = offset as usize + encoded_entry.len();
        self.lock_if_needed(bag_key, new_size)?;

        Ok(())
    }

    pub fn exists(&self, bag_key: &BagKey, key: &EntryKey) -> bool {
        self.store.bags.get(bag_key)
                .is_some_and(|bag| bag.entries.contains_key(key))
    }

    pub fn list_keys(&self, bag_key: &BagKey) -> Result<Vec<EntryKey>, EngineError> {
        let bag = self.store.bags.get(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;
        let keys = bag.entries.keys().cloned().collect();

        Ok(keys)
    }

    // pub fn list_entries(&self, bag_key: &BagKey) -> Result<Vec<KVPair>, EngineError> {
    //     todo!()
    // }


            // BAGS //

    pub fn create_bag(&mut self, bag_key: &BagKey) -> Result<(), EngineError> {
        if self.store.bags.contains_key(bag_key) {
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

        self.store.bags.insert(bag_key.clone(), new_bag);

                // Update the store file.
        let encoded_store = encoding::encode_store_file(&self.store)?;
        io::overwrite(&self.get_store_file_path(), &encoded_store)?;

        Ok(())
    }

    pub fn drop_bag(&mut self, bag_key: &BagKey) -> Result<(), EngineError> {
        let bag = match self.store.bags.remove(bag_key) {
            Some(b) => b,
            None => return Err(EngineError::NoSuchBagKeyError(bag_key.clone())),
        };


        let encoded_store = encoding::encode_store_file(&self.store)?;
        io::overwrite(&self.get_store_file_path(), &encoded_store)?;

                // Remove bag files
        let bag_files = upkeep::get_bag_file_chain(&bag)?;
        for file in bag_files {
            fs::remove_file(file)?;
        }

        Ok(())
    }

    pub fn len_bag(&self, bag_key: &BagKey) -> Result<usize, EngineError> {
        let bag = self.store.bags.get(bag_key)
                .ok_or(EngineError::NoSuchBagKeyError(bag_key.clone()))?;

        Ok(bag.entries.len())
    }

    pub fn list_bags(&self) -> Vec<BagKey> {
        self.store.bags.values()
                .map(|b| b.key.clone())
                .collect()
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
            bags.insert(bag_root.key.clone(), rebuilt_bag);
        }

        let engine = BarKVEngine {
            store: StoreArchive { bags },
            root_path: path,
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
            store: StoreArchive {
                bags: HashMap::new(),
            },
            root_path: path,
        };

        let init_data = encoding::encode_store_file(&res.store)?;
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

    pub fn close(&mut self) -> Result<(), EngineError> {
        for bag in self.store.bags.values_mut() {
            io::close_file(&mut bag.file_handle)?;
        }

        Ok(())
    }

    // pub fn sync(&mut self) {
    //     todo!()     // what what this supposed to do? probably no need... I think
    // }
    
    pub fn compact_active(&mut self) -> Vec<(BagKey, Result<(), EngineError>)> {
        self.store.bags.iter_mut()
                .map(|(key, bag)| {
                    (key.clone(), upkeep::compact_partial(bag, None).map(|_| ()))
                })
                .collect()
    }

    pub fn full_compaction(&mut self) -> Vec<(BagKey, Result<(), EngineError>)>  {
        self.store.bags.iter_mut()
                .map(|(key, bag)| {
                    (key.clone(), upkeep::full_compaction(bag, &self.root_path).map(|_| ()))
                })
                .collect()
    }


            // ATOMIC //
            
            // TODO Make these actually atomic once concurrency is added.
    
    pub fn get_or_set(&mut self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<Vec<u8>, EngineError> {
        if self.exists(bag_key, key) {
            self.get(bag_key, key)
        } else {
            self.set(bag_key, key, value)?;
            Ok(value.to_vec())
        }
    }

    pub fn update_if_different(&mut self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        let current = self.get(bag_key, key)?;
        if current != value {
            self.set(bag_key, key, value)?;
        }
        Ok(())
    }

    pub fn get_and_delete(&mut self, bag_key: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        let value = self.get(bag_key, key)?;
        self.delete(bag_key, key)?;
        Ok(value)
    }


            // BATCH OPS //

    pub fn get_many(&mut self, bag_key: &BagKey, keys: &[&EntryKey]) -> Result<Vec<KVPair>, EngineError> {
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;

        let mut res = Vec::new();

        for &key in keys {
            let entry = match bag.entries.get(key) {
                Some(ime) => ime,
                None => continue,
            };

            let read_chunk = if entry.file == bag.active_path {
                io::read_chunk(&mut bag.file_handle, entry.offset, entry.size)?
            } else {
                let mut file_handle = io::open_file_for_read(&entry.file)?;
                io::read_chunk(&mut file_handle, entry.offset, entry.size)?
            };
            let value = encoding::get_value_from_entry_data(&read_chunk)?;

            res.push(KVPair { key: key.into(), value });
        }

        Ok(res)
    }

    pub fn set_many(&mut self, bag_key: &BagKey, pairs: Vec<KVPair>) -> Result<(), EngineError> {
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;

        let mut new_size = 0;

        for pair in pairs {
            let key = pair.key.clone();

            let od_in_entry = ODIntermediateEntry::make_update(key.clone(), pair.value);
            let encoded_entry = encoding::encode_od_entry(&od_in_entry)?;
            let offset = io::append(&mut bag.file_handle, &encoded_entry)?;

            let im_entry = IMEntry {
                key: key.clone(),
                file: bag.active_path.clone(),
                offset,
                size: encoded_entry.len() as u64,
            };

            bag.entries.insert(key, im_entry);

            new_size = offset as usize + encoded_entry.len();
        }

        self.lock_if_needed(bag_key, new_size)?;

        Ok(())
    }

    pub fn delete_many(&mut self, bag_key: &BagKey, keys: &[&EntryKey]) -> Result<(), EngineError> {
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;

        let mut new_size = 0;

        for &key in keys {
            if !bag.entries.contains_key(key) {
                continue;
            }
    
            let od_in_entry = ODIntermediateEntry::make_tombstone(key.into());
            let encoded_entry = encoding::encode_od_entry(&od_in_entry)?;
            let offset = io::append(&mut bag.file_handle, &encoded_entry)?;
    
            bag.entries.remove(key);

            new_size = offset as usize + encoded_entry.len();
        }

        self.lock_if_needed(bag_key, new_size)?;

        Ok(())
    }


            // TTL
    
    pub fn set_with_expiry(&mut self, bag_key: &BagKey, key: &EntryKey, value: &[u8], ttl: u128) -> Result<(), EngineError>{
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;

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

        let new_size = offset as usize + encoded_entry.len();
        self.lock_if_needed(bag_key, new_size)?;

        Ok(())
    }

    pub fn ttl(&mut self, bag_key: &BagKey, key: &EntryKey) -> Result<u128, EngineError> {
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;
        let entry = bag.entries.get(key)
                .ok_or_else(|| EngineError::NoSuchEntryKeyError(key.to_string()))?;

        let header_size = encoding::KV_ENTRY_HEADER_BASE_SIZE + 8;
        if entry.size < header_size as u64 {
            return Err(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))?
        }

        let read_chunk = if entry.file == bag.active_path {
            io::read_chunk(&mut bag.file_handle, entry.offset, entry.size)?
        } else {
            let mut file_handle = io::open_file_for_read(&entry.file)?;
            io::read_chunk(&mut file_handle, entry.offset, entry.size)?
        };        
        let expiry = encoding::get_expiry_entry_data(&read_chunk)?;

        Ok(expiry)
    }

    pub fn persist(&mut self, bag_key: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;
        let entry = bag.entries.get(key)
                .ok_or_else(|| EngineError::NoSuchEntryKeyError(key.to_string()))?;

        let read_chunk = if entry.file == bag.active_path {
            io::read_chunk(&mut bag.file_handle, entry.offset, entry.size)?
        } else {
            let mut file_handle = io::open_file_for_read(&entry.file)?;
            io::read_chunk(&mut file_handle, entry.offset, entry.size)?
        };
        let value = encoding::get_value_from_entry_data(&read_chunk)?;

        let od_in_entry = ODIntermediateEntry::make_update(key.into(), value);
        let encoded_entry = encoding::encode_od_entry(&od_in_entry)?;
        let offset = io::append(&mut bag.file_handle, &encoded_entry)?;

        let im_entry = IMEntry {
            key: key.into(),
            file: bag.active_path.clone(),
            offset,
            size: encoded_entry.len() as u64,
        };

        bag.entries.insert(key.clone(), im_entry);

        let new_size = offset as usize + encoded_entry.len();
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
    
    fn seal(&mut self, bag_key: &BagKey) -> Result<String, EngineError> {
        todo!()
    }

    fn get_store_file_path(&self) -> PathBuf {
        self.root_path.join(STORE_FILENAME)
    }

    pub(super) fn lock_active(&mut self, bag_key: &BagKey) -> Result<(), EngineError> {
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;

        let updated_headers = BagStoreFileHeaders {
            is_sealed: false,
            is_locked: true,
            is_deleted: false,
            file_id: bag.current_file_id
        };

        let offset_data = upkeep::compact_partial(bag, Some(updated_headers))?;

                // Create sealed helper file
        let next_file_path = upkeep::build_bag_path(&self.root_path,
                    bag_key, bag.current_file_id as usize + 1);
        let seal_helper_data = SealHelperFile {
            next_file: next_file_path.clone(),
            entries: offset_data,
        };
        let encoded_seal_file = encoding::encode_seal_helper_file(&seal_helper_data)?;
        let seal_file_path = upkeep::get_sealed_file_path(&bag.active_path);
        let mut seal_file_handle = io::create_file_to_append(&seal_file_path)?;
        io::write_all(&mut seal_file_handle, &encoded_seal_file)?;
        io::close_file(&mut seal_file_handle)?;

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
    // #[error("Wrapped ParseError: {0}")]
    // ParseError(#[from] ParseError)
    #[error("Wrapped encoding error: {0}")]
    WrappedEncodingError(#[from] EncodingError),
    #[error("Wrapped io error: {0}")]
    WrappedIOError(#[from] Error),
}
