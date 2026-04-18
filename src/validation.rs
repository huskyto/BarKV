
use thiserror::Error;

use crate::io;
use crate::upkeep;
use crate::encoding;
use crate::encoding::EncodingError;
use crate::engine::EngineError;
use crate::engine::BarKVEngine;
use crate::model::BagKey;
use crate::model::EntryKey;
use crate::model::BagStoreFileHeaders;

use crate::engine::STORE_FILENAME;


pub fn validate(engine: &BarKVEngine) -> Vec<ValidationFailure> {
    let mut failures = Vec::new();
    let root_file_path = engine.root_path.join(STORE_FILENAME);
            // Root validations
    let mut root_file_handle = match io::open_file_for_read(&root_file_path) {
        Ok(f) => f,
        Err(e) => {
            failures.push(ValidationFailure::root(ValidationError::IO(e)));
            return failures;
        },
    };
    let root_file_data = match io::read_all_file(&mut root_file_handle) {
        Ok(d) => d,
        Err(e) => {
            failures.push(ValidationFailure::root(ValidationError::IO(e)));
            return failures;
        },
    };
    if let Err(e) = encoding::validate_root_store(&root_file_data) {
        failures.push(ValidationFailure::root(ValidationError::Encoding(e)));
    }

    let dummy_headers = BagStoreFileHeaders::for_init(0);

    let store = match engine.store.read() {
        Ok(s) => s,
        Err(_) => {
            failures.push(ValidationFailure::root(ValidationError::LockPoisoned));
            return failures;
        },
    };

            // Bag validations.
    for (bag_key, bag_arc) in &store.bags {
        let bag = match bag_arc.lock() {
            Ok(b) => b,
            Err(_) => {
                failures.push(ValidationFailure::bag(bag_key.clone(), ValidationError::LockPoisoned));
                return failures;
            },
        };

        let bag_file_chain = match upkeep::get_bag_file_chain(&bag) {
            Ok(c) => c,
            Err(e) => {
                failures.push(ValidationFailure::bag(bag_key.clone(), ValidationError::Engine(e)));
                continue;
            }
        };
        for bag_file in bag_file_chain {
            let mut bag_file_handle = match io::open_file_for_read(&bag_file) {
                Ok(f) => f,
                Err(e) => {
                    failures.push(ValidationFailure::bag(bag_key.clone(), ValidationError::IO(e)));
                    continue;
                },
            };
            let bag_file_data = match io::read_all_file(&mut bag_file_handle) {
                Ok(d) => d,
                Err(e) => {
                    failures.push(ValidationFailure::bag(bag_key.clone(), ValidationError::IO(e)));
                    continue;
                },
            };

                    // TODO add granularity and depth.
                    // Right now it fails on the first encountered error.
                    // And lacks per-entry information.
            if upkeep::is_file_seal(&bag_file) {
                if let Err(e) = encoding::decode_seal_store_file(&bag_file_data, &dummy_headers) {
                    failures.push(ValidationFailure::bag(bag_key.clone(), ValidationError::Encoding(e)));
                }
            }
            else if let Err(e) = encoding::decode_bag_store_file(&bag_file_data) {
                failures.push(ValidationFailure::bag(bag_key.clone(), ValidationError::Encoding(e)));
            }
        }

    }

    failures
}

#[derive(Debug)]
pub struct ValidationFailure {
    pub bag_key: Option<BagKey>,
    pub entry_key: Option<EntryKey>,
    pub error: ValidationError
}
impl ValidationFailure {
    pub fn root(error: ValidationError) -> Self {
        Self { bag_key: None, entry_key: None, error }
    }
    pub fn bag(bag_key: BagKey, error: ValidationError) -> Self {
        Self { bag_key: Some(bag_key), entry_key: None, error }
    }
    pub fn entry(bag_key: BagKey, entry_key: EntryKey, error: ValidationError) -> Self {
        Self { bag_key: Some(bag_key), entry_key: Some(entry_key), error }
    }
}
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Wrapped engine error: {0}")]
    Engine(#[from] EngineError),
    #[error("Wrapped encoding error: {0}")]
    Encoding(#[from] EncodingError),
    #[error("Wrapped io error: {0}")]
    IO(#[from] std::io::Error),
    #[error("Lock poisoned")]
    LockPoisoned,
}
