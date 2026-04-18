
use std::path::PathBuf;
use std::string::FromUtf8Error;

use thiserror::Error;

use crate::util;
use crate::model::Bag;
use crate::model::BagRootEntry;
use crate::model::StoreArchive;
use crate::model::SealHelperFile;
use crate::model::BagStoreFileData;
use crate::model::BagStoreFileHeaders;
use crate::model::ODIntermediateEntry;
use crate::model::BaseEntryRebuildData;
use crate::model::OffsetEntryRebuildData;
use crate::model::BagStoreFileDataIntermediateEntries;


pub const ROOT_STORE_FILE_HEADER_SIZE: usize = 12;
pub const STORE_FILE_HEADER_SIZE: usize = 3;
pub const SEAL_HELPER_FILE_HEADER_SIZE: usize = 6;
pub const KV_ENTRY_HEADER_BASE_SIZE: usize = 25;
pub const SHORT_ENTRY_HEADER_SIZE: usize = 20;

pub fn validate_root_store(data: &[u8]) -> Result<(), EncodingError> {
    if data.len() < ROOT_STORE_FILE_HEADER_SIZE {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::StoreHeader))
    }

    let magic_bytes = &data[..5];
    if magic_bytes != util::MAGIC_BYTES {
        return Err(EncodingError::IncorrectMagicBytes)
    }

    // let version = &data[5..8];
    // Not currently enforcing version.

    let header_crc = &data[8..12];
    let real_crc = util::calculate_crc(&data[12..]);

    if header_crc != real_crc.to_be_bytes() {
        Err(EncodingError::CorruptStore)
    } else { Ok(()) }
}

pub fn decode_store_roots(data: &[u8]) -> Result<Vec<BagRootEntry>, EncodingError> {
    validate_root_store(data)?;

    let mut roots = Vec::new();
    let mut head = 12;
    while head < data.len() {
        let (entry, offset) = decode_bag_root_entry(&data[head..])?;
        roots.push(entry);
        head += offset;
    }

    Ok(roots)
}

fn decode_bag_root_entry(data: &[u8]) -> Result<(BagRootEntry, usize), EncodingError> {
    if data.len() < 4 {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::BagRootEntry))
    }

    let name_size = u16::from_be_bytes([data[0], data[1]]);
    let path_size = u16::from_be_bytes([data[2], data[3]]);

    let expected_size = 4 + name_size as usize + path_size as usize;
    if data.len() < expected_size {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::BagRootEntry))
    }
    
    let name_bytes = &data[4..4 + name_size as usize];
    let name = String::from_utf8(name_bytes.to_vec())
            .map_err(EncodingError::StringDecodeError)?;

    let path_offset = 4 + name_size as usize;
    let path_bytes = &data[path_offset..path_offset + path_size as usize];
    let path = String::from_utf8(path_bytes.to_vec())
            .map_err(EncodingError::StringDecodeError)?;

    let entry = BagRootEntry {
        key: name,
        root_path: path,
    };

    Ok((entry, expected_size))
}

pub fn decode_bag_store_file(data: &[u8]) -> Result<BagStoreFileData, EncodingError> {
    if data.len() < STORE_FILE_HEADER_SIZE {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::StoreFile))
    }

    let flags = data[0];
        // currently unused
    let is_deleted = (flags & 0b0000_0001) != 0;
    let is_sealed  = (flags & 0b0000_0010) != 0;
    let is_locked  = (flags & 0b0000_0100) != 0;

    let file_id_data = [data[1], data[2]];
    let file_id = u16::from_be_bytes(file_id_data);

    if is_sealed {
        return Ok(BagStoreFileData {
            headers: BagStoreFileHeaders {
                is_sealed,
                is_locked,
                is_deleted,
                file_id
            },
            rebuild_data: Vec::new(),
            next_file: None
        });
    }

    let mut rebuild_entries = Vec::new();
    let mut head = STORE_FILE_HEADER_SIZE;
    while head < data.len() {
        let rebuild_data = decode_entry_rebuild_data(&data[head..])?;
        let offset = head as u64;
        head += rebuild_data.size as usize;
        rebuild_entries.push(rebuild_data.with_offset(offset));
    }

    Ok(BagStoreFileData {
        headers: BagStoreFileHeaders {
            is_sealed,
            is_locked,
            is_deleted,
            file_id
        },
        rebuild_data: rebuild_entries,
        next_file: None,
    })
}

pub fn decode_bag_store_file_header(data: &[u8]) -> Result<BagStoreFileHeaders, EncodingError> {
    if data.len() < STORE_FILE_HEADER_SIZE {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::StoreFile))
    }

    let flags = data[0];
    let file_id_data = [data[1], data[2]];
    let file_id = u16::from_be_bytes(file_id_data);

    Ok(BagStoreFileHeaders::from_flags_and_id(flags, file_id))
}

pub fn decode_seal_store_file(data: &[u8], base_headers: &BagStoreFileHeaders) -> Result<BagStoreFileData, EncodingError> {
    let header_size = SEAL_HELPER_FILE_HEADER_SIZE;
    if data.len() < header_size {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::SealHelperFile))
    }

    let header_crc = &data[0..4];
    let real_crc = util::calculate_crc(&data[4..]);

    if header_crc != real_crc.to_be_bytes() {
        return Err(EncodingError::CorruptSealHelperFile)
    }

    let nf_size_bytes = [data[4], data[5]];
    let nf_size = u16::from_be_bytes(nf_size_bytes);

    if data.len() < header_size + nf_size as usize {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::SealHelperFile))
    }

    let nf_bytes = &data[header_size..header_size + nf_size as usize];
    let next_file_str = String::from_utf8(nf_bytes.to_vec())
            .map_err(EncodingError::StringDecodeError)?;
    let next_file = PathBuf::from(next_file_str);

    let mut rebuild_entries = Vec::new();
    let mut head = header_size + nf_size as usize;
    while head < data.len() {
        let (rebuild_data, offset) = decode_entry_rebuild_data_from_short_entry(&data[head..])?;
        head += offset;
        rebuild_entries.push(rebuild_data);
    }

    Ok(BagStoreFileData {
        headers: base_headers.clone(),
        rebuild_data: rebuild_entries,
        next_file: Some(next_file),
    })
}

fn decode_entry_rebuild_data(data: &[u8]) -> Result<BaseEntryRebuildData, EncodingError> {
    if data.len() < KV_ENTRY_HEADER_BASE_SIZE {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))
    }

    let header_crc = &data[0..4];

        // Currently unused.
    let _timestamp_bytes = &data[4..12];

    let flags = data[12];
    let is_deleted = (flags & 0b0000_0001) != 0;
    let has_expiry = (flags & 0b0000_0010) != 0;

    let expiry_offset = if has_expiry { 16 } else { 0 };

    let key_size_bytes: [u8; 4] = data[13..17].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let key_size = u32::from_be_bytes(key_size_bytes);

    let val_size_bytes: [u8; 8] = data[17..25].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let value_size = u64::from_be_bytes(val_size_bytes);

    let expected_size = (KV_ENTRY_HEADER_BASE_SIZE as u64 + expiry_offset + key_size as u64)
            .checked_add(value_size)
            .ok_or(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))?;

    let expected_size_usize = usize::try_from(expected_size)
        .map_err(|_| EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))?;

    if data.len() < expected_size_usize {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))
    }

    let real_crc = util::calculate_crc(&data[4..expected_size_usize]);
    if header_crc != real_crc.to_be_bytes() {
        return Err(EncodingError::CorruptEntry)
    }

    // if has_expiry {
    //     let expiry_bytes: [u8; 16] = data[25..41].try_into()
    //             .map_err(|_| EncodingError::SliceCohersionError)?;
    //     let expiry_u128 = u128::from_be_bytes(expiry_bytes);
    //     if expiry_u128 <= util::current_timestamp() {
    //         // return Err(EncodingError::ExpiredEntry)
    //         is_deleted = true
    //     }
    // }
    
    let key_offset = 25 + expiry_offset as usize;
    let key_bytes = &data[key_offset..key_offset + key_size as usize];
    let key = String::from_utf8(key_bytes.to_vec())
            .map_err(EncodingError::StringDecodeError)?;

    let data = BaseEntryRebuildData {
        key,
        size: expected_size,
        deleted: is_deleted,
    };

    Ok(data)
}

pub fn get_value_from_entry_data(data: &[u8]) -> Result<Vec<u8>, EncodingError> {
    if data.len() < KV_ENTRY_HEADER_BASE_SIZE {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))
    }

    let header_crc = &data[0..4];

        // Currently unused.
    // let timestamp_bytes = &data[4..12];

    let flags = data[12];
    let is_deleted = (flags & 0b0000_0001) != 0;
    let has_expiry = (flags & 0b0000_0010) != 0;

    if is_deleted {
        return Err(EncodingError::DeletedEntry);
    }

    let expiry_offset = if has_expiry { 16 } else { 0 };

    let key_size_bytes: [u8; 4] = data[13..17].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let key_size = u32::from_be_bytes(key_size_bytes);

    let val_size_bytes: [u8; 8] = data[17..25].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let value_size = u64::from_be_bytes(val_size_bytes);

    let expected_size = (25 + expiry_offset + key_size as u64)
            .checked_add(value_size)
            .ok_or(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))?;

    let expected_size_usize = usize::try_from(expected_size)
        .map_err(|_| EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))?;

    if data.len() < expected_size_usize {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))
    }

    let real_crc = util::calculate_crc(&data[4..expected_size_usize]);
    if header_crc != real_crc.to_be_bytes() {
        return Err(EncodingError::CorruptEntry);
    }

    if has_expiry {
        let expiry_bytes: [u8; 16] = data[25..41].try_into()
                .map_err(|_| EncodingError::SliceCohersionError)?;
        let expiry_u128 = u128::from_be_bytes(expiry_bytes);
        if expiry_u128 <= util::current_timestamp() {
            return Err(EncodingError::ExpiredEntry)
        }
    }
    
    let value_offset = 25 + expiry_offset as usize + key_size as usize;
    let value_bytes = &data[value_offset..value_offset + value_size as usize];

    Ok(value_bytes.to_vec())
}

fn decode_entry_rebuild_data_from_short_entry(data: &[u8]) -> Result<(OffsetEntryRebuildData, usize), EncodingError> {
    if data.len() < 20 {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::ShortEntry))
    }

    let offset_bytes: [u8; 8] = data[..8].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let offset = u64::from_be_bytes(offset_bytes);

    let entry_size_bytes: [u8; 8] = data[8..16].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let entry_size = u64::from_be_bytes(entry_size_bytes);

    let key_size_bytes: [u8; 4] = data[16..20].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let key_size = u32::from_be_bytes(key_size_bytes);

    let key_bytes = &data[20..20 + key_size as usize];
    let key = String::from_utf8(key_bytes.to_vec())
            .map_err(EncodingError::StringDecodeError)?;

        // Sealed files should not contain tombstones.
    let data = OffsetEntryRebuildData {
        key,
        size: entry_size,
        offset,
        deleted: false,
    };

    Ok((data, 20 + key_size as usize))
}


pub fn decode_bag_store_file_int_entries(data: &[u8]) -> Result<BagStoreFileDataIntermediateEntries, EncodingError> {
    if data.len() < STORE_FILE_HEADER_SIZE {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::StoreFile))
    }

    let flags = data[0];

    let file_id_data = [data[1], data[2]];
    let file_id = u16::from_be_bytes(file_id_data);

    let mut entries = Vec::new();
    let mut head = STORE_FILE_HEADER_SIZE;
    while head < data.len() {
        let (entry, size) = decode_entry_to_intermediate(&data[head..])?;
        head += size;
        entries.push(entry);
    }

    Ok(BagStoreFileDataIntermediateEntries {
        headers: BagStoreFileHeaders::from_flags_and_id(flags, file_id),
        int_entries: entries,
    })
}

fn decode_entry_to_intermediate(data: &[u8]) -> Result<(ODIntermediateEntry, usize), EncodingError> {
    if data.len() < KV_ENTRY_HEADER_BASE_SIZE {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))
    }

    let header_crc = &data[0..4];

        // Currently unused.
    let timestamp_bytes: [u8; 8] = data[4..12].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let timestamp = u64::from_be_bytes(timestamp_bytes);

    let flags = data[12];
    let mut is_deleted = (flags & 0b0000_0001) != 0;
    let has_expiry = (flags & 0b0000_0010) != 0;

    let expiry_offset = if has_expiry { 16 } else { 0 };

    let key_size_bytes: [u8; 4] = data[13..17].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let key_size = u32::from_be_bytes(key_size_bytes);

    let val_size_bytes: [u8; 8] = data[17..25].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let value_size = u64::from_be_bytes(val_size_bytes);

    let expected_size = (25 + expiry_offset + key_size as u64)
            .checked_add(value_size)
            .ok_or(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))?;

    let expected_size_usize = usize::try_from(expected_size)
        .map_err(|_| EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))?;

    if data.len() < expected_size_usize {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))
    }

    let real_crc = util::calculate_crc(&data[4..expected_size_usize]);
    if header_crc != real_crc.to_be_bytes() {
        return Err(EncodingError::CorruptEntry)
    }

    let mut expiry = None;
    if has_expiry {
        let expiry_bytes: [u8; 16] = data[25..41].try_into()
                .map_err(|_| EncodingError::SliceCohersionError)?;
        let expiry_u128 = u128::from_be_bytes(expiry_bytes);
            // TODO decide if this is what we want, or if we want to keep it as expiring
        if expiry_u128 <= util::current_timestamp() {
            is_deleted = true
        }
        else {
            expiry = Some(expiry_u128)
        }
    }
    
    let key_offset = 25 + expiry_offset as usize;
    let key_bytes = &data[key_offset..key_offset + key_size as usize];
    let key = String::from_utf8(key_bytes.to_vec())
            .map_err(EncodingError::StringDecodeError)?;

    let value_offset = 25 + expiry_offset as usize + key_size as usize;
    let value_bytes = &data[value_offset..value_offset + value_size as usize];

    let entry = ODIntermediateEntry {
        key,
        value: value_bytes.to_vec(),
        expiry,
        is_tombstone: is_deleted,
        timestamp
    };

    Ok((entry, expected_size_usize))
}


pub fn get_expiry_entry_data(data: &[u8]) -> Result<u128, EncodingError> {
    if data.len() < KV_ENTRY_HEADER_BASE_SIZE + 16 {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))
    }

    let flags = data[12];
    let is_deleted = (flags & 0b0000_0001) != 0;
    let has_expiry = (flags & 0b0000_0010) != 0;

    if is_deleted {
        return Err(EncodingError::DeletedEntry);
    }

    if !has_expiry {
        return Err(EncodingError::NotExpiringEntry)
    }

    let expiry_bytes: [u8; 16] = data[25..41].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let expiry_u128 = u128::from_be_bytes(expiry_bytes);
    if expiry_u128 <= util::current_timestamp() {
        return Err(EncodingError::ExpiredEntry)
    }

    Ok(expiry_u128)
}

pub fn encode_od_entry(entry: &ODIntermediateEntry) -> Result<Vec<u8>, EncodingError> {
    let key_size = entry.key.len() as u32;
    let val_size = entry.value.len() as u64;
    let has_expiry = entry.expiry.is_some();
    let flags = (entry.is_tombstone as u8)
            | ((has_expiry as u8) << 1);
    let timestamp = entry.timestamp;
    let mut entry_size = 25 + key_size as u64 + val_size;
    if has_expiry {
        entry_size += 16;
    }

    let mut res = Vec::with_capacity(entry_size as usize);

    res.extend_from_slice(&[0; 4]); // reserve for CRC
    res.extend_from_slice(&timestamp.to_be_bytes());
    res.push(flags);
    res.extend_from_slice(&key_size.to_be_bytes());
    res.extend_from_slice(&val_size.to_be_bytes());
    if let Some(expiry) = entry.expiry {
        res.extend_from_slice(&expiry.to_be_bytes());
    }
    res.extend_from_slice(entry.key.as_bytes());
    res.extend_from_slice(&entry.value);

    let crc = util::calculate_crc(&res[4..]);
    res[..4].copy_from_slice(&crc.to_be_bytes());

    Ok(res)
}

pub fn encode_store_file(store: &StoreArchive) -> Result<Vec<u8>, EncodingError> {
    let mut data = Vec::new();
    data.extend_from_slice(&util::MAGIC_BYTES);
    data.extend_from_slice(&util::VERSION);
    data.extend_from_slice(&[0; 4]);      // Reserve for CRC.

    for bag_arc in store.bags.values() {
        let bag = bag_arc.lock().map_err(|_| EncodingError::LockPoisoned)?;
        let encoded_bag = encode_bag_root(&bag)?;
        data.extend_from_slice(&encoded_bag);
    }

    let crc = util::calculate_crc(&data[12..]);
    data[8..12].copy_from_slice(&crc.to_be_bytes());

    Ok(data)
}

fn encode_bag_root(bag: &Bag) -> Result<Vec<u8>, EncodingError> {
    let key = &bag.key;
    let path = bag.root_path.to_str()
            .ok_or(EncodingError::CorruptPath)?;

    let key_len: u16 = key.len().try_into()
            .map_err(|_| EncodingError::IntoU16Failed)?;
    let path_len: u16 = path.len().try_into()
            .map_err(|_| EncodingError::IntoU16Failed)?;

    let mut data = Vec::with_capacity(4 + key_len as usize + path_len as usize);
    data.extend_from_slice(&key_len.to_be_bytes());
    data.extend_from_slice(&path_len.to_be_bytes());
    data.extend_from_slice(key.as_bytes());
    data.extend_from_slice(path.as_bytes());

    Ok(data)
}

pub fn encode_bag_store_file_header(headers: &BagStoreFileHeaders) -> Result<Vec<u8>, EncodingError> {
    let mut data = Vec::new();
    let flags = (headers.is_deleted as u8)
            | ((headers.is_sealed as u8) << 1)
            | ((headers.is_locked as u8) << 2);

    data.push(flags);
    data.extend_from_slice(&headers.file_id.to_be_bytes());
    Ok(data)
}

pub fn encode_bag_store_file_full(headers: &BagStoreFileHeaders, entries: &[ODIntermediateEntry])
            -> Result<(Vec<u8>, Vec<OffsetEntryRebuildData>), EncodingError> {
    let encoded_header = encode_bag_store_file_header(headers)?;
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    let mut head = STORE_FILE_HEADER_SIZE;
    data.extend_from_slice(&encoded_header);
    for entry in entries {
        let encoded_entry = encode_od_entry(entry)?;
        data.extend_from_slice(&encoded_entry);
        let offset_data = OffsetEntryRebuildData {
            key: entry.key.clone(),
            size: encoded_entry.len() as u64,
            offset: head as u64,
            deleted: entry.is_tombstone,
        };
        offsets.push(offset_data);
        head += encoded_entry.len();
    }

    Ok((data, offsets))
}

pub fn encode_seal_helper_file(seal_helper_data: &SealHelperFile) -> Result<Vec<u8>, EncodingError> {
    let mut data = Vec::new();
    data.extend_from_slice(&[0; 4]);      // Reserve for CRC.

    let next_file = seal_helper_data.next_file.to_str()
            .ok_or(EncodingError::CorruptPath)?
            .as_bytes();
    let nf_size: u16 = next_file.len().try_into()
            .map_err(|_| EncodingError::IntoU16Failed)?;

    data.extend_from_slice(&nf_size.to_be_bytes());
    data.extend_from_slice(next_file);

    for entry in &seal_helper_data.entries {
        let encoded_short_entry = encode_short_entry(entry)?;
        data.extend_from_slice(&encoded_short_entry);
    }

    let crc = util::calculate_crc(&data[4..]);
    data[..4].copy_from_slice(&crc.to_be_bytes());

    Ok(data)
}

fn encode_short_entry(entry: &OffsetEntryRebuildData) -> Result<Vec<u8>, EncodingError> {
    let key_bytes = entry.key.as_bytes();
    let key_size: u32 = key_bytes.len().try_into()
            .map_err(|_| EncodingError::IntoU32Failed)?;

    let mut data = Vec::with_capacity(SHORT_ENTRY_HEADER_SIZE + key_size as usize);

    data.extend_from_slice(&entry.offset.to_be_bytes());
    data.extend_from_slice(&entry.size.to_be_bytes());
    data.extend_from_slice(&key_size.to_be_bytes());
    data.extend_from_slice(key_bytes);

    Ok(data)
}


#[derive(Debug, Error)]
pub enum EncodingError {
    #[error("Encode error: {0}")]
    EncodeError(String),
    #[error("Decode error: {0}")]
    DecodeError(String),
    #[error("Size mismatch for entry: {0}")]
    SizeMismatch(SizeMismatchType),
    #[error("Store header doesn't have the correct Magic Bytes")]
    IncorrectMagicBytes,
    #[error("Store header CRC does not match real CRC")]
    CorruptStore,
    #[error("On Disk Entry header CRC does not match real CRC")]
    CorruptEntry,
    #[error("TTL has run out for requested entry")]
    ExpiredEntry,
    #[error("Requested entry is deleted")]
    DeletedEntry,
    #[error("Queried 'expiry' on non-expiring entry")]
    NotExpiringEntry,
    #[error("Seal Helper file header CRC does not match real CRC")]
    CorruptSealHelperFile,
    #[error("Failed to coherse slice into defined size")]
    SliceCohersionError,
    #[error("Failed to serialize path")]
    CorruptPath,
    #[error("Failed to coherce value to u16")]
    IntoU16Failed,
    #[error("Failed to coherce value to u32")]
    IntoU32Failed,
    #[error("A lock was poisoned")]
    LockPoisoned,

    #[error("Failed to recreate UTF8 String")]
    StringDecodeError(#[from] FromUtf8Error)
}

#[derive(Debug, Error)]
pub enum SizeMismatchType {
    #[error("Store header is too small")]
    StoreHeader,
    #[error("Bag root entry is too small")]
    BagRootEntry,
    #[error("Store file data is too small")]
    StoreFile,
    #[error("On Disk data store entry is too small")]
    OnDiskEntry,
    #[error("Seal Helper File is too small")]
    SealHelperFile,
    #[error("Short Entry is too small")]
    ShortEntry,
}
