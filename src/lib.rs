
//! `BarKV` is a log-structured key-value store.
//!
//! Values are grouped into named bags and persisted to append-only files.
//! [`BarKV`] is the public handle: it is cheap to clone and safe to share
//! across threads.

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

/// Handle for a `BarKV` store.
///
/// Cloning shares the same underlying store; they are cheap and can be
/// used concurrently from multiple threads.
///
/// Every operation on an open store returns [`EngineError::StoreClosed`] if it
/// is called after [`close`](BarKV::close).
/// 
/// Individual methods document the errors specific to them.
#[derive(Clone)]
pub struct BarKV {
    engine: Arc<BarKVEngine>,
}

impl BarKV {
    /// Opens an existing store at `path`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::RootPathError`] if `path` is not a directory.
    /// - [`EngineError::RootFileNotFound`] if it holds no store file.
    pub fn open(path: &str) -> Result<BarKV, EngineError> {
        BarKVEngine::open(path).map(|e| BarKV { engine: Arc::new(e) })
    }

    /// Creates a new, empty store at `path`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::RootPathError`] if `path` is not a directory.
    /// - [`EngineError::RootPathNotEmpty`] if the directory is not empty.
    pub fn create(path: &str) -> Result<BarKV, EngineError> {
        BarKVEngine::create(path).map(|e| BarKV { engine: Arc::new(e) })
    }

    /// Opens the store at `path`, creating it if no store file is present.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::RootPathError`] if `path` is not a directory.
    /// - [`EngineError::RootPathNotEmpty`] if a new store is needed but the
    ///   directory is not empty.
    pub fn open_or_create(path: &str) -> Result<BarKV, EngineError> {
        BarKVEngine::open_or_create(path).map(|e| BarKV { engine: Arc::new(e) })
    }

    /// Returns the value stored for `key` in `bag_key`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    /// - [`EngineError::NoSuchEntryKeyError`] if the key is absent.
    pub fn get(&self, bag_key: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        self.engine.get(bag_key, key)
    }

    /// Stores `value` under `key` in `bag_key`, replacing any existing value.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    pub fn set(&self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        self.engine.set(bag_key, key, value)
    }

    /// Removes `key` from `bag_key`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    /// - [`EngineError::NoSuchEntryKeyError`] if the key is absent.
    pub fn delete(&self, bag_key: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
        self.engine.delete(bag_key, key)
    }

    /// Returns whether `key` is present in `bag_key`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    pub fn exists(&self, bag_key: &BagKey, key: &EntryKey) -> Result<bool, EngineError> {
        self.engine.exists(bag_key, key)
    }

    /// Returns all keys held in `bag_key`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    pub fn list_keys(&self, bag_key: &BagKey) -> Result<Vec<EntryKey>, EngineError> {
        self.engine.list_keys(bag_key)
    }

    /// Creates a new bag.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::BagAlreadyExistsError`] if the bag already exists.
    pub fn create_bag(&self, bag_key: &BagKey) -> Result<(), EngineError> {
        self.engine.create_bag(bag_key)
    }

    /// Drops a bag and deletes its on-disk files.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    pub fn drop_bag(&self, bag_key: &BagKey) -> Result<(), EngineError> {
        self.engine.drop_bag(bag_key)
    }

    /// Returns the number of entries in `bag_key`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    pub fn len_bag(&self, bag_key: &BagKey) -> Result<usize, EngineError> {
        self.engine.len_bag(bag_key)
    }

    /// Returns the keys of all bags in the store.
    ///
    /// # Errors
    ///
    /// Returns:
    pub fn list_bags(&self) -> Result<Vec<BagKey>, EngineError> {
        self.engine.list_bags()
    }

    /// Closes the store, blocking further operations.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::StoreClosed`] if the store is already closed.
    pub fn close(&self) -> Result<(), EngineError> {
        self.engine.close()
    }

    /// Runs a partial compaction on each bag's active file.
    ///
    /// Returns a per-bag result; a failure on one bag does not stop the others.
    #[must_use = "compaction reports per-bag failures through the returned Vec"]
    pub fn compact_active(&self) -> Vec<(BagKey, Result<(), EngineError>)> {
        self.engine.compact_active()
    }

    /// Runs a full compaction across every bag, including sealed files.
    ///
    /// Returns a per-bag result; a failure on one bag does not stop the others.
    #[must_use = "compaction reports per-bag failures through the returned Vec"]
    pub fn full_compaction(&self) -> Vec<(BagKey, Result<(), EngineError>)> {
        self.engine.full_compaction()
    }

    /// Returns the value for `key`, or stores and returns `value` if absent.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    pub fn get_or_set(&self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<Vec<u8>, EngineError> {
        self.engine.get_or_set(bag_key, key, value)
    }

    /// Stores `value` under `key` only if it differs from the current value.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    /// - [`EngineError::NoSuchEntryKeyError`] if the key is absent.
    pub fn update_if_different(&self, bag_key: &BagKey, key: &EntryKey, value: &[u8]) -> Result<(), EngineError> {
        self.engine.update_if_different(bag_key, key, value)
    }

    /// Returns the value for `key` and removes it in a single operation.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    /// - [`EngineError::NoSuchEntryKeyError`] if the key is absent.
    pub fn get_and_delete(&self, bag_key: &BagKey, key: &EntryKey) -> Result<Vec<u8>, EngineError> {
        self.engine.get_and_delete(bag_key, key)
    }

    /// Returns the values for `keys` that are present in `bag_key`.
    ///
    /// Missing keys are skipped rather than reported as errors.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    pub fn get_many(&self, bag_key: &BagKey, keys: &[&EntryKey]) -> Result<Vec<KVPair>, EngineError> {
        self.engine.get_many(bag_key, keys)
    }

    /// Stores each key-value pair in `pairs` into `bag_key`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    pub fn set_many(&self, bag_key: &BagKey, pairs: Vec<KVPair>) -> Result<(), EngineError> {
        self.engine.set_many(bag_key, pairs)
    }

    /// Removes each of `keys` from `bag_key`.
    ///
    /// Missing keys are skipped rather than reported as errors.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    pub fn delete_many(&self, bag_key: &BagKey, keys: &[&EntryKey]) -> Result<(), EngineError> {
        self.engine.delete_many(bag_key, keys)
    }

    /// Stores `value` under `key` with a time-to-live of `ttl` milliseconds.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    pub fn set_with_expiry(&self, bag_key: &BagKey, key: &EntryKey, value: &[u8], ttl: u128) -> Result<(), EngineError> {
        self.engine.set_with_expiry(bag_key, key, value, ttl)
    }

    /// Returns the expiry timestamp (milliseconds since epoch) for `key`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    /// - [`EngineError::NoSuchEntryKeyError`] if the key is absent.
    pub fn ttl(&self, bag_key: &BagKey, key: &EntryKey) -> Result<u128, EngineError> {
        self.engine.ttl(bag_key, key)
    }

    /// Rewrites `key` without its expiry, making the value permanent.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`EngineError::NoSuchBagKeyError`] if the bag does not exist.
    /// - [`EngineError::NoSuchEntryKeyError`] if the key is absent.
    pub fn persist(&self, bag_key: &BagKey, key: &EntryKey) -> Result<(), EngineError> {
        self.engine.persist(bag_key, key)
    }

    /// Validates the on-disk store and returns any failures found.
    ///
    /// An empty vector means the store passed validation.
    #[must_use = "validation results are reported through the returned Vec"]
    pub fn validate(&self) -> Vec<validation::ValidationFailure> {
        self.engine.validate()
    }
}
