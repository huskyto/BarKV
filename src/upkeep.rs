
use std::path::Path;
use std::path::PathBuf;
use std::collections::HashMap;

use crate::io;
use crate::model::BagRootEntry;
use crate::model::BagStoreFileHeaders;
use crate::model::OffsetEntryRebuildData;
use crate::util;
use crate::encoding;
use crate::engine::EngineError;
use crate::model::Bag;
use crate::model::BagKey;
use crate::model::ODIntermediateEntry;



pub(super) fn get_sealed_file_path(base_file_path: &Path) -> PathBuf {
    base_file_path.with_extension("seal")
}
pub(super) fn is_file_seal(path: &Path) -> bool {
    path.extension().is_some_and(|ext| ext == "seal")
}
pub(super) fn build_bag_path(store_root_path: &Path, bag_key: &BagKey, file_id: usize) -> PathBuf {
    let bag_filename = format!("{bag_key}-{file_id}.bkv");
    store_root_path.join(bag_filename)
}

        // Replay //

pub(super) fn rebuild_bag_history(bag_root: &BagRootEntry) -> Result<Bag, EngineError> {
    let root_file_path = PathBuf::from(&bag_root.root_path);

    let mut next_file = Some(root_file_path.clone());
    let mut entries_map = HashMap::new();

    let mut active_path = root_file_path.clone();
    let mut current_id = 0;

    while let Some(next_path) = &next_file {
        active_path = next_path.clone();
        let mut file_handle = io::open_file_for_read(next_path)?;

        let header_data = io::read_chunk(&mut file_handle, 0, encoding::STORE_FILE_HEADER_SIZE as u64)?;
        let store_headers = encoding::decode_bag_store_file_header(&header_data)?;
        current_id = store_headers.file_id;

        let decode_data = if store_headers.is_locked || store_headers.is_sealed {
            let seal_file_path = get_sealed_file_path(next_path);
            let mut seal_file_handle = io::open_file_for_read(&seal_file_path)?;
            let seal_file_data = io::read_all_file(&mut seal_file_handle)?;
            io::close_file(&mut seal_file_handle)?;
            encoding::decode_seal_store_file(&seal_file_data, &store_headers)?
        }
        else {
            let data = io::read_all_file(&mut file_handle)?;
            encoding::decode_bag_store_file(&data)?
        };

        for entry in decode_data.rebuild_data {
            if entry.deleted {
                    // Maintain tombstone regardless on partial compaction
                entries_map.remove(&entry.key);
            }
            else {
                let im_entry = entry.to_im_entry(next_path);
                entries_map.insert(entry.key.clone(), im_entry);
            }
        }

        io::close_file(&mut file_handle)?;
        next_file = decode_data.next_file;
    }

    let active_file_handle = io::open_file_to_append(&active_path)?;
    let bag = Bag {
        key: bag_root.key.clone(),
        entries: entries_map,
        root_path: root_file_path,
        active_path,
        file_handle: active_file_handle,
        current_file_id: current_id,
    };

    Ok(bag)
}


        // Sealing and Compaction //


pub(super) fn compact_partial(bag: &mut Bag, updated_headers: Option<BagStoreFileHeaders>) -> Result<Vec<OffsetEntryRebuildData>, EngineError> {
    let path = &bag.active_path;

    let mut file_handle = io::open_file_for_read(path)?;
    let data = io::read_all_file(&mut file_handle)?;
    io::close_file(&mut file_handle)?;
    let decode_data = encoding::decode_bag_store_file_int_entries(&data)?;
    let current_timestamp_millis = util::current_timestamp();
    let mut entries_map = HashMap::new();

    for entry in decode_data.int_entries {
        if entry.is_tombstone {
                // Maintain tombstone regardless on partial compaction
            entries_map.insert(entry.key.clone(), entry);
        }
        else if let Some(expiry) = entry.expiry && expiry <= current_timestamp_millis  {
            entries_map.insert(entry.key.clone(), entry.to_tombstone());
        }
        else {
            entries_map.insert(entry.key.clone(), entry);
        }
    }

            // Write updated data.

    let entries: Vec<ODIntermediateEntry> = entries_map.into_values().collect();
    let new_headers = match updated_headers {
        Some(headers) => headers,
        None => decode_data.headers,
    };
    let (bag_store_data, offset_data) = encoding::encode_bag_store_file_full(&new_headers, &entries)?;
    io::overwrite(path, &bag_store_data)?;

            // Update IMEntries

    bag.entries.retain(|_, ime| &ime.file != path);
    
    for offset_entry in &offset_data {
        let updated_im_entry = offset_entry.to_im_entry(path);
        bag.entries.insert(offset_entry.key.clone(), updated_im_entry);
    }

    Ok(offset_data)
}

pub(super) fn get_bag_file_chain(bag: &Bag) -> Result<Vec<PathBuf>, EngineError> {
    let mut res = Vec::new();
    res.push(bag.root_path.clone());
    let mut root_file = io::open_file_for_read(&bag.root_path)?;
    let data = io::read_chunk(&mut root_file, 0, encoding::STORE_FILE_HEADER_SIZE as u64)?;
    let decoded = encoding::decode_bag_store_file(&data)?;
    let mut next_file = if decoded.headers.is_sealed {
        Some(get_sealed_file_path(&bag.root_path))
    } else { None };

    while let Some(next) = &next_file {
        res.push(next.clone());
        let is_sealed = is_file_seal(next);
        let mut file_handle = io::open_file_for_read(next)?;
        if is_sealed {
            let seal_data = io::read_chunk(&mut file_handle,
                    0, encoding::SEAL_HELPER_FILE_HEADER_SIZE as u64)?;
            let decoded_seal = encoding::decode_seal_store_file(&seal_data, &decoded.headers)?;
            next_file = decoded_seal.next_file;
        }
        else {
            let data = io::read_chunk(&mut file_handle, 0, encoding::STORE_FILE_HEADER_SIZE as u64)?;
            let decoded = encoding::decode_bag_store_file(&data)?;
            next_file = if decoded.headers.is_sealed {
                Some(get_sealed_file_path(next))
            } else { None };
        }
    }

    Ok(res)
}
