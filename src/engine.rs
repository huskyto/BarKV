
use thiserror::Error;

use crate::model::KVPair;
use crate::model::BagKey;
use crate::model::EntryKey;
use crate::model::StoreArchive;


pub struct BarKVEngine {
    store: StoreArchive
}

impl BarKVEngine {
            // VALUES //

    pub fn get(&self, bag: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        todo!()     // TODO
    }

    pub fn set(&mut self, bag: &BagKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        todo!()     // TODO
    }

    pub fn delete(&mut self, bag: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
        todo!()     // TODO
    }

    pub fn exists(&self, bag: &BagKey, key: &EntryKey) -> bool {
        todo!()     // TODO
    }

    pub fn list_keys(&self, bag: &BagKey) -> Result<Vec<EntryKey>, EngineError> {
        todo!()     // TODO
    }

    pub fn list_entries(&self, bag: &BagKey) -> Result<Vec<KVPair>, EngineError> {
        todo!()     // TODO
    }


            // BAGS //

    pub fn create_bag(&mut self, bag: &BagKey) -> Result<(), EngineError> {
        todo!()     // TODO
    }

    pub fn drop_bag(&mut self, bag: &BagKey) -> Result<(), EngineError> {
        todo!()     // TODO
    }

    pub fn list_bags(&self) -> Vec<EntryKey> {
        todo!()     // TODO
    }

    pub fn count_bag(&self) -> Result<usize, EngineError> {
        todo!()     // TODO
    }


            // LIFECYCLE //

    pub fn open(path: &str) -> Result<BarKVEngine, EngineError>{
        todo!()     // TODO
    }

    pub fn close() {
        todo!()     // TODO Result and async?
    }

    pub fn sync() {
        todo!()     // TODO need ?
    }
    
    pub fn compact() {
        todo!()     // TODO
    }


            // ATOMIC // TODO extension.
    
    pub fn get_or_set(bag: &BagKey, key: &EntryKey, value: &[u8]) -> Result<Vec<u8>, EngineError> {
        todo!()     // TODO
    }

    pub fn update_if_different(bag: &BagKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        todo!()     // TODO Maybe?
    }

    pub fn get_and_delete(bag: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        todo!()     // TODO
    }


            // BATCH OPS // TODO Extension.

    pub fn get_many(bag: &BagKey, keys: &[&EntryKey]) -> Result<Vec<KVPair>, EngineError> {
        todo!()     // TODO
    }

    pub fn set_many(bag: &BagKey, pairs: &[KVPair]) -> Result<(), EngineError> {
        todo!()     // TODO
    }

    pub fn delete_many(bag: &BagKey, keys: &[&EntryKey]) -> Result<(), EngineError> {
        todo!()     // TODO
    }


            // TTL // TODO Extension. Maybe.
    
    pub fn set_with_expiry(bag: &BagKey, key: &EntryKey, value: &[u8], ttl: u128) -> Result<(), EngineError>{
        todo!()     // TODO
    }

    pub fn ttl(bag: &BagKey, key: &EntryKey) -> Result<u128, EngineError> {
        todo!()     // TODO
    }

    pub fn persist(bag: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
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
    
    pub fn seal(bag: &BagKey) -> Result<String, EngineError> {
        todo!()
    }

}


#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Wrapped IO read error: {0}")]
    IOReadError(std::io::Error),
    #[error("Wrapped IO write error: {0}")]
    IOWriteError(std::io::Error),
    #[error("EntryKey does not exist in bag: {0}")]
    NoSuchEntryKeyError(EntryKey),
    #[error("BagKey does not exist in store: {0}")]
    NoSuchBagKeyError(EntryKey),
    #[error("Entry was queried, but is deleted: {0}")]
    DeletedEntryError(EntryKey),
    #[error("Entry was queried, but is expired: {0}")]
    ExpiredEntryError(EntryKey),
    // #[error("Wrapped ParseError: {0}")]
    // ParseError(#[from] ParseError)
}
