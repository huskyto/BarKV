
use crate::model::EntryKey;
use crate::model::KeyValue;
use crate::model::EngineError;
use crate::model::StoreArchive;


pub struct BarKVEngine {
    store: StoreArchive
}

impl BarKVEngine {
            // VALUES //

    pub fn get(&self, bag: &EntryKey, key: &EntryKey) -> &[u8] {
        todo!()     // TODO
    }

    pub fn set(&mut self, bag: &EntryKey, key: &EntryKey, value: &[u8]) {
        todo!()     // TODO
    }

    pub fn delete(&mut self, bag: &EntryKey, key: &EntryKey) {
        todo!()     // TODO
    }

    pub fn exists(&self, bag: &EntryKey, key: &EntryKey) {
        todo!()     // TODO
    }

    pub fn list_keys(&self, bag: &EntryKey) -> &Vec<EntryKey> {
        todo!()     // TODO
    }

    pub fn list_entries(&self, bag: &EntryKey) -> &Vec<KeyValue> {
        todo!()     // TODO
    }


            // BAGS //

    pub fn create_bag(&mut self, bag: &EntryKey) {
        todo!()     // TODO
    }

    pub fn drop_bag(&mut self, bag: &EntryKey) {
        todo!()     // TODO
    }

    pub fn list_bags(&self) -> &Vec<EntryKey> {
        todo!()     // TODO
    }

    pub fn count_bag(&self) -> usize {
        todo!()     // TODO
    }


            // LIFECYCLE //

    pub fn open(path: &str) -> BarKVEngine {
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
    
    pub fn get_or_set(bag: &EntryKey, key: &EntryKey, value: &[u8]) -> Result<Vec<u8>, EngineError> {
        todo!()     // TODO
    }

    pub fn update_if_different(bag: &EntryKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        todo!()     // TODO Maybe?
    }

    pub fn get_and_delete(bag: &EntryKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        todo!()     // TODO
    }


            // BATCH OPS // TODO Extension.

    pub fn get_many(bag: &EntryKey, keys: &Vec<&EntryKey>) -> Result<Vec<KeyValue>, EngineError> {
        todo!()     // TODO
    }

    pub fn set_many(bag: &EntryKey, pairs: &Vec<KeyValue>) -> Result<(), EngineError> {
        todo!()     // TODO
    }

    pub fn delete_many(bag: &EntryKey, keys: &Vec<&EntryKey>) -> Result<(), EngineError> {
        todo!()     // TODO
    }


            // TTL // TODO Extension. Maybe.
    
    pub fn set_with_expiry(bag: &EntryKey, key: &EntryKey, value: &[u8], ttl: u128) {
        todo!()     // TODO
    }

    pub fn ttl(bag: &EntryKey, key: &EntryKey) -> Result<u128, EngineError> {
        todo!()     // TODO
    }

    pub fn persist(bag: &EntryKey, key: &EntryKey) {
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
    
    pub fn seal(bag: &EntryKey) -> Result<String, EngineError> {
        todo!()
    }

}

