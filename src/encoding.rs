
use std::path::PathBuf;
use std::string::FromUtf8Error;

use thiserror::Error;

use crate::util;
use crate::model::Bag;
use crate::model::BagRootEntry;
use crate::model::StoreArchive;
use crate::model::BagStoreFileData;
use crate::model::BaseEntryRebuildData;
use crate::model::ODIntermediateEntry;
use crate::model::OffsetEntryRebuildData;


pub const STORE_FILE_HEADER_SIZE: usize = 5;
pub const SEAL_HELPER_FILE_HEADER_SIZE: usize = 6;
pub const KV_ENTRY_HEADER_BASE_SIZE: usize = 25;

pub fn decode_store_roots(data: &[u8]) -> Result<Vec<BagRootEntry>, EncodingError> {
    if data.len() < 12 {
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
        return Err(EncodingError::CorruptStore)
    }

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

    let header_crc = &data[0..4];
    let real_crc = util::calculate_crc(&data[4..]);

    if header_crc != real_crc.to_be_bytes() {
        return Err(EncodingError::CorruptStore)
    }

    let flags = data[4];
        // currently unused
    // let is_deleted = (flags & 0b0000_0001) != 0;
    let is_sealed  = (flags & 0b0000_0010) != 0;

    if is_sealed {
        return Ok(BagStoreFileData {
            is_sealed,
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
        is_sealed,
        rebuild_data: rebuild_entries,
        next_file: None
    })
}

pub fn decode_seal_store_file(data: &[u8]) -> Result<BagStoreFileData, EncodingError> {
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
        is_sealed: true,
        rebuild_data: rebuild_entries,
        next_file: Some(next_file),
    })
}

fn decode_entry_rebuild_data(data: &[u8]) -> Result<BaseEntryRebuildData, EncodingError> {
    if data.len() < 25 {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::OnDiskEntry))
    }

    let header_crc = &data[0..4];
    // let real_crc = calculate_crc(&data[4..]);

    // if header_crc != real_crc.to_be_bytes() {
    //     return Err(EncodingError::CorruptEntry)
    // }

        // Currently unused.
    let timestamp_bytes = &data[4..12];

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

pub fn encode_bag_entry(bag: &Bag) -> Result<Vec<u8>, EncodingError> {
    let bag_root_path = bag.root_path.to_str()
            .ok_or(EncodingError::CorruptPath)?;
    let key_size = bag.key.len() as u16;
    let path_size = bag_root_path.len() as u16;
    let entry_size = 4 + key_size + path_size;
    let mut res = Vec::with_capacity(entry_size as usize);

    res.extend_from_slice(&key_size.to_be_bytes());
    res.extend_from_slice(&path_size.to_be_bytes());
    res.extend_from_slice(bag.key.as_bytes());
    res.extend_from_slice(bag_root_path.as_bytes());

    Ok(res)
}

pub fn encode_od_entry(entry: &ODIntermediateEntry) -> Result<Vec<u8>, EncodingError> {
    let key_size = entry.key.len() as u32;
    let val_size = entry.value.len() as u64;
    let has_expiry = entry.expiry.is_some();
    let flags = (entry.is_tombstone as u8)
            | ((has_expiry as u8) << 1);
    let timestamp = util::current_timestamp();
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

    for bag in store.bags.values() {
        let encoded_bag = encode_bag_root(bag)?;
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
