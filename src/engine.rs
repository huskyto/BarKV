
use std::fs;
use std::io::Error;
use std::path::PathBuf;
use std::collections::HashMap;
use thiserror::Error;

use crate::io;
use crate::encoding;
use crate::encoding::EncodingError;
use crate::model::Bag;
use crate::model::KVPair;
use crate::model::BagKey;
use crate::model::IMEntry;
use crate::model::EntryKey;
use crate::model::StoreArchive;
use crate::model::ODIntermediateEntry;


const STORE_FILENAME: &str = "barkv.store";

pub struct BarKVEngine {
    store: StoreArchive,
    root_path: PathBuf         // This should be a folder
}

impl BarKVEngine {

            // VALUES //

    pub fn get(&mut self, bag_key: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;
        let entry = bag.entries.get(key)
                .ok_or_else(|| EngineError::NoSuchEntryKeyError(key.to_string()))?;

        let read_chunk = io::read_chunk(&mut bag.file_handle, entry.offset, entry.size)?;
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

        Ok(())
    }

    pub fn delete(&mut self, bag_key: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
        let bag = self.store.bags.get_mut(bag_key)
                .ok_or_else(|| EngineError::NoSuchBagKeyError(bag_key.to_string()))?;
        
        if !bag.entries.contains_key(key) {
            return Err(EngineError::NoSuchEntryKeyError(key.to_string()));
        }

        let od_in_entry = ODIntermediateEntry::make_tombstone(key.into());
        let encoded_entry = encoding::encode_od_entry(&od_in_entry)?;
        io::append(&mut bag.file_handle, &encoded_entry)?;

        bag.entries.remove(key);

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

    pub fn list_entries(&self, bag_key: &BagKey) -> Result<Vec<KVPair>, EngineError> {
        todo!()     // TODO
    }


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
        let file_handle = io::create_file_to_append(&bag_root_path)?;
        let new_bag = Bag {
            key: bag_key.clone(),
            entries: HashMap::new(),
            root_path: bag_root_path.clone(),
            active_path: bag_root_path,
            file_handle,
        };
        self.store.bags.insert(bag_key.clone(), new_bag);

                // Update the store file.
        let encoded_store = encoding::encode_store_file(&self.store)?;
        io::overwrite(&self.get_store_file_path(), &encoded_store)?;

        Ok(())
    }

    pub fn drop_bag(&mut self, bag_key: &BagKey) -> Result<(), EngineError> {
        if !self.store.bags.contains_key(bag_key) {
            return Err(EngineError::NoSuchBagKeyError(bag_key.clone()))
        }

        self.store.bags.remove(bag_key);

        let encoded_store = encoding::encode_store_file(&self.store)?;
        io::overwrite(&self.get_store_file_path(), &encoded_store)?;

        // TODO delete the chain of bag files.

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
        todo!()     // TODO
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
        todo!()     // TODO
    }

    pub fn close(&mut self) {
        todo!()     // TODO Result and async?
    }

    pub fn sync(&mut self) {
        todo!()     // TODO need ?
    }
    
    pub fn compact(&mut self) {
        todo!()     // TODO
    }


            // ATOMIC // TODO extension.
    
    pub fn get_or_set(&mut self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<Vec<u8>, EngineError> {
        todo!()     // TODO
    }

    pub fn update_if_different(&mut self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        todo!()     // TODO Maybe?
    }

    pub fn get_and_delete(&mut self, bag_key: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        todo!()     // TODO
    }


            // BATCH OPS // TODO Extension.

    pub fn get_many(bag_key: &BagKey, keys: &[&EntryKey]) -> Result<Vec<KVPair>, EngineError> {
        todo!()     // TODO
    }

    pub fn set_many(bag_key: &BagKey, pairs: &[KVPair]) -> Result<(), EngineError> {
        todo!()     // TODO
    }

    pub fn delete_many(bag_key: &BagKey, keys: &[&EntryKey]) -> Result<(), EngineError> {
        todo!()     // TODO
    }


            // TTL // TODO Extension. Maybe.
    
    pub fn set_with_expiry(bag_key: &BagKey, key: &EntryKey, value: &[u8], ttl: u128) -> Result<(), EngineError>{
        todo!()     // TODO
    }

    pub fn ttl(bag_key: &BagKey, key: &EntryKey) -> Result<u128, EngineError> {
        todo!()     // TODO
    }

    pub fn persist(bag_key: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
        todo!()     // TODO
    }


            // STATE // TODO Extension.
    
    pub fn stats() {
        todo!()     // TODO Stats model
    }

    pub fn validate() {
        todo!()     // TODO check crcs
    }


            // INTERNAL //
    
    fn seal(bag_key: &BagKey) -> Result<String, EngineError> {
        todo!()
    }

    fn get_store_file_path(&self) -> PathBuf {
        self.root_path.join(STORE_FILENAME)
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
    // #[error("Wrapped ParseError: {0}")]
    // ParseError(#[from] ParseError)
    #[error("Wrapped encoding error: {0}")]
    WrappedEncodingError(#[from] EncodingError),
    #[error("Wrapped io error: {0}")]
    WrappedIOError(#[from] Error),
}
