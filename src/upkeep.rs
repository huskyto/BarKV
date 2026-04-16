
use std::path::Path;
use std::path::PathBuf;
use std::collections::HashMap;

use crate::io;
use crate::model::IMEntry;
use crate::model::EntryKey;
use crate::model::BagRootEntry;
use crate::model::SealHelperFile;
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

        // REPLAY //

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


        // COMPACTION //

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

#[derive(Clone)]
struct FileInfo {
    filepath: PathBuf,
    file_id: u16,
    is_locked: bool,
    is_sealed: bool
}

pub (super) fn full_compaction(bag: &mut Bag, store_root_path: &Path) -> Result<(), EngineError> {
    let file_chain = get_bag_file_chain(bag)?;
    let mut full_entries_map: HashMap<EntryKey, (FileInfo, ODIntermediateEntry)> = HashMap::new();
    let current_timestamp_millis = util::current_timestamp();

    for file_path in file_chain {
                    // Process Entries //
        let mut file_handle = io::open_file_for_read(&file_path)?;
        let data = io::read_all_file(&mut file_handle)?;
        io::close_file(&mut file_handle)?;
        let decode_data = encoding::decode_bag_store_file_int_entries(&data)?;

        let file_info = FileInfo {
            filepath: file_path.clone(),
            file_id: decode_data.headers.file_id,
            is_locked: decode_data.headers.is_locked,
            is_sealed: decode_data.headers.is_sealed,
        };

        for entry in decode_data.int_entries {
            if entry.is_tombstone {
                    // We can use tombstone to remove previous ones with full compaction.
                full_entries_map.remove(&entry.key);
            }
            else if let Some(expiry) = entry.expiry && expiry <= current_timestamp_millis  {
                    // Directly remove in full compaction.
                full_entries_map.remove(&entry.key);
            }
            else {
                    // Overwrite is still enough.
                full_entries_map.insert(entry.key.clone(), (file_info.clone(), entry));
            }
        }
    }

    let mut map_by_file: HashMap<u16, (FileInfo, Vec<ODIntermediateEntry>)> = HashMap::new();

            // TODO possible target for optimization. Verify with perf data.
    for (_, (file_info, entry)) in full_entries_map {
        map_by_file.entry(file_info.file_id)
            .or_insert_with(|| (file_info.clone(), Vec::new()))
            .1.push(entry);
    }

    let mut sorted_by_file_id: Vec<(u16, (FileInfo, Vec<ODIntermediateEntry>))> = map_by_file.into_iter().collect();
    sorted_by_file_id.sort_by_key(|(id, _)| *id);

    let mut new_im_index: HashMap<String, IMEntry> = HashMap::new();

    for (file_id, (file_info, file_entries)) in sorted_by_file_id {
        if file_info.is_sealed {
                        // Update sealed helper file
            let next_file_path = build_bag_path(store_root_path,
                    &bag.key, file_id as usize + 1);

            let mut offset_data = Vec::new();
            for file_entry in file_entries {
                let entry_key = &file_entry.key;
                if let Some(c_entry) = bag.entries.get(entry_key) {
                    if c_entry.file != file_info.filepath {
                                // Entry should be in same file.
                        return Err(EngineError::EntryConsistencyError);
                    }
                    let entry_rebuild_data = OffsetEntryRebuildData::from_im_entry(c_entry);
                    offset_data.push(entry_rebuild_data);
                    new_im_index.insert(c_entry.key.clone(), c_entry.clone());
                }
                else {
                            // Entry should always be present.
                    return Err(EngineError::EntryConsistencyError);
                }
            }

            let seal_helper_data = SealHelperFile {
                next_file: next_file_path.clone(),
                entries: offset_data,
            };
            let encoded_seal_file = encoding::encode_seal_helper_file(&seal_helper_data)?;
            let seal_file_path = get_sealed_file_path(&file_info.filepath);
            let mut seal_file_handle = io::create_file_to_append(&seal_file_path)?;
            io::write_all(&mut seal_file_handle, &encoded_seal_file)?;
            io::close_file(&mut seal_file_handle)?;
        }
        else if file_info.is_locked {
            let new_headers = BagStoreFileHeaders::for_sealed(file_info.file_id);
            let (bag_store_data, offset_data) = 
                    encoding::encode_bag_store_file_full(&new_headers, &file_entries)?;

            io::overwrite(&file_info.filepath, &bag_store_data)?;

                        // Update IMEntries
            for offset_entry in &offset_data {
                let updated_im_entry = offset_entry.to_im_entry(&file_info.filepath);
                new_im_index.insert(offset_entry.key.clone(), updated_im_entry);
            }

                        // Create sealed helper file
            let next_file_path = build_bag_path(store_root_path,
                    &bag.key, file_id as usize + 1);
            let seal_helper_data = SealHelperFile {
                next_file: next_file_path.clone(),
                entries: offset_data,
            };
            let encoded_seal_file = encoding::encode_seal_helper_file(&seal_helper_data)?;
            let seal_file_path = get_sealed_file_path(&file_info.filepath);
            let mut seal_file_handle = io::create_file_to_append(&seal_file_path)?;
            io::write_all(&mut seal_file_handle, &encoded_seal_file)?;
            io::close_file(&mut seal_file_handle)?;
        }
        else {
            let new_headers = BagStoreFileHeaders::for_init(file_info.file_id);
            let (bag_store_data, offset_data) = 
            encoding::encode_bag_store_file_full(&new_headers, &file_entries)?;
            
                        // Update IMEntries
            for offset_entry in &offset_data {
                let updated_im_entry = offset_entry.to_im_entry(&file_info.filepath);
                new_im_index.insert(offset_entry.key.clone(), updated_im_entry);
            }
            
                        // Store updated file.
            io::overwrite(&file_info.filepath, &bag_store_data)?;
        }
    }

    bag.entries = new_im_index;
    bag.file_handle = io::open_file_to_append(&bag.active_path)?;
    Ok(())
}


        // HELPERS //

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
