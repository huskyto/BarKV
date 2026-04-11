
use std::string::FromUtf8Error;

use thiserror::Error;

use crate::util;
use crate::model::Bag;
use crate::model::BagRootEntry;
use crate::model::BagStoreFileData;
use crate::model::BaseEntryRebuildData;
use crate::model::ODIntermediateEntry;
use crate::model::OffsetEntryRebuildData;


const MAGIC_BYTES: [u8; 5] = [0x42, 0x61, 0x72, 0x4B, 0x56];

pub fn decode_store_roots(data: &[u8]) -> Result<Vec<BagRootEntry>, EncodingError> {
    if data.len() < 12 {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::StoreHeader))
    }

    let magic_bytes = &data[..5];
    if magic_bytes != MAGIC_BYTES {
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

pub fn decode_bag_root_entry(data: &[u8]) -> Result<(BagRootEntry, usize), EncodingError> {
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
    if data.len() < 5 {
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
    let mut head = 5;
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
    if data.len() < 6 {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::SealHelperFile))
    }

    let header_crc = &data[0..4];
    let real_crc = util::calculate_crc(&data[4..]);

    if header_crc != real_crc.to_be_bytes() {
        return Err(EncodingError::CorruptSealHelperFile)
    }

    let nf_size_bytes = [data[4], data[5]];
    let nf_size = u16::from_be_bytes(nf_size_bytes);

    if data.len() < 6 + nf_size as usize {
        return Err(EncodingError::SizeMismatch(SizeMismatchType::SealHelperFile))
    }

    let nf_bytes = &data[6..6 + nf_size as usize];
    let next_file = String::from_utf8(nf_bytes.to_vec())
            .map_err(EncodingError::StringDecodeError)?;

    let mut rebuild_entries = Vec::new();
    let mut head = 6 + nf_size as usize;
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

pub fn decode_entry_rebuild_data(data: &[u8]) -> Result<BaseEntryRebuildData, EncodingError> {
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

    let expiry_offset = if has_expiry { 8 } else { 0 };

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
    if data.len() < 25 {
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

    let expiry_offset = if has_expiry { 8 } else { 0 };

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

    let expiry_bytes: [u8; 8] = data[25..33].try_into()
            .map_err(|_| EncodingError::SliceCohersionError)?;
    let expiry_u64 = u64::from_be_bytes(expiry_bytes);
    if expiry_u64 <= util::current_timestamp() {
        return Err(EncodingError::ExpiredEntry)
    }
    
    let value_offset = 25 + expiry_offset as usize + key_size as usize;
    let value_bytes = &data[value_offset..value_offset + value_size as usize];

    Ok(value_bytes.to_vec())
}


pub fn decode_entry_rebuild_data_from_short_entry(data: &[u8]) -> Result<(OffsetEntryRebuildData, usize), EncodingError> {
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

pub fn encode_bag_entry(bag: &Bag) -> Result<Vec<u8>, EncodingError> {
    let key_size = bag.key.len() as u16;
    let path_size = bag.root_path.len() as u16;
    let entry_size = 4 + key_size + path_size;
    let mut res = Vec::with_capacity(entry_size as usize);

    res.extend_from_slice(&key_size.to_be_bytes());
    res.extend_from_slice(&path_size.to_be_bytes());
    res.extend_from_slice(bag.key.as_bytes());
    res.extend_from_slice(bag.root_path.as_bytes());

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
        entry_size += 8;
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
    #[error("Seal Helper file header CRC does not match real CRC")]
    CorruptSealHelperFile,
    #[error("Failed to coherse slice into defined size")]
    SliceCohersionError,

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
