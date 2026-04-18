

mod io;
mod util;
mod upkeep;
mod encoding;
mod validation;
pub mod model;

pub(crate) mod engine;
pub use engine::EngineError;
use std::sync::Arc;

use crate::model::KVPair;
use crate::model::BagKey;
use crate::model::EntryKey;
use crate::engine::BarKVEngine;

#[derive(Clone)]
pub struct BarKV {
    engine: Arc<BarKVEngine>,
}

impl BarKV {
    pub fn open(path: &str) -> Result<BarKV, EngineError> {
        BarKVEngine::open(path).map(|e| BarKV { engine: Arc::new(e) })
    }

    pub fn create(path: &str) -> Result<BarKV, EngineError> {
        BarKVEngine::create(path).map(|e| BarKV { engine: Arc::new(e) })
    }

    pub fn open_or_create(path: &str) -> Result<BarKV, EngineError> {
        BarKVEngine::open_or_create(path).map(|e| BarKV { engine: Arc::new(e) })
    }

    pub fn get(&self, bag_key: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        self.engine.get(bag_key, key)
    }

    pub fn set(&self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        self.engine.set(bag_key, key, value)
    }

    pub fn delete(&self, bag_key: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
        self.engine.delete(bag_key, key)
    }

    pub fn exists(&self, bag_key: &BagKey, key: &EntryKey) -> Result<bool, EngineError> {
        self.engine.exists(bag_key, key)
    }

    pub fn list_keys(&self, bag_key: &BagKey) -> Result<Vec<EntryKey>, EngineError> {
        self.engine.list_keys(bag_key)
    }

    pub fn create_bag(&self, bag_key: &BagKey) -> Result<(), EngineError> {
        self.engine.create_bag(bag_key)
    }

    pub fn drop_bag(&self, bag_key: &BagKey) -> Result<(), EngineError> {
        self.engine.drop_bag(bag_key)
    }

    pub fn len_bag(&self, bag_key: &BagKey) -> Result<usize, EngineError> {
        self.engine.len_bag(bag_key)
    }

    pub fn list_bags(&self) -> Result<Vec<BagKey>, EngineError> {
        self.engine.list_bags()
    }

    pub fn close(&self) -> Result<(), EngineError> {
        self.engine.close()
    }

    pub fn compact_active(&self) -> Vec<(BagKey, Result<(), EngineError>)> {
        self.engine.compact_active()
    }

    pub fn full_compaction(&self) -> Vec<(BagKey, Result<(), EngineError>)> {
        self.engine.full_compaction()
    }

    pub fn get_or_set(&self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<Vec<u8>, EngineError> {
        self.engine.get_or_set(bag_key, key, value)
    }

    pub fn update_if_different(&self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        self.engine.update_if_different(bag_key, key, value)
    }

    pub fn get_and_delete(&self, bag_key: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        self.engine.get_and_delete(bag_key, key)
    }

    pub fn get_many(&self, bag_key: &BagKey, keys: &[&EntryKey]) -> Result<Vec<KVPair>, EngineError> {
        self.engine.get_many(bag_key, keys)
    }

    pub fn set_many(&self, bag_key: &BagKey, pairs: Vec<KVPair>) -> Result<(), EngineError> {
        self.engine.set_many(bag_key, pairs)
    }

    pub fn delete_many(&self, bag_key: &BagKey, keys: &[&EntryKey]) -> Result<(), EngineError> {
        self.engine.delete_many(bag_key, keys)
    }

    pub fn set_with_expiry(&self, bag_key: &BagKey, key: &EntryKey, value: &[u8], ttl: u128) -> Result<(), EngineError> {
        self.engine.set_with_expiry(bag_key, key, value, ttl)
    }

    pub fn ttl(&self, bag_key: &BagKey, key: &EntryKey) -> Result<u128, EngineError> {
        self.engine.ttl(bag_key, key)
    }

    pub fn persist(&self, bag_key: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
        self.engine.persist(bag_key, key)
    }

    pub fn validate(&self) -> Vec<validation::ValidationFailure> {
        self.engine.validate()
    }
}
