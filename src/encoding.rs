
use std::string::FromUtf8Error;

use thiserror::Error;

use crate::model::Bag;
use crate::model::BagRootEntry;
use crate::model::ODIntermediateEntry;


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
    let real_crc = calculate_crc(&data[12..]);

    if header_crc != real_crc.to_be_bytes() {
        return Err(EncodingError::CorruptStore)
    }

    let mut roots = Vec::new();
    let mut header = 12;
    while header < data.len() {
        let (entry, offset) = decode_bag_root_entry(&data[header..])?;
        roots.push(entry);
        header += offset;
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

pub fn decode_bag_store_file(data: &[u8]) -> Result<(), EncodingError> {
    todo!()
}

pub fn decode_seal_store_file(data: &[u8]) -> Result<(), EncodingError> {
    todo!()
}


pub fn decode_od_entry(data: &[u8]) -> Result<ODIntermediateEntry, EncodingError> {
    todo!()
}



pub fn encode_bag_entry(bag: &Bag) -> Result<Vec<u8>, EncodingError> {
    let key_size = bag.key.len();
    let path_size = bag.root_path.len();
    let entry_size = 4 + key_size + path_size;
    let mut res = Vec::with_capacity(entry_size);

    res.extend_from_slice(&key_size.to_be_bytes());
    res.extend_from_slice(&path_size.to_be_bytes());
    res.extend_from_slice(bag.key.as_bytes());
    res.extend_from_slice(bag.root_path.as_bytes());

    Ok(res)
}

pub fn encode_od_entry(entry: &ODIntermediateEntry) -> Result<Vec<u8>, EncodingError> {
    let key_size = entry.key.len();
    let val_size = entry.value.len();
    let has_expiry = entry.expiry.is_some();
    let flags = (entry.is_tombstone as u8)
            | ((has_expiry as u8) << 1);
    let timestamp = current_timestamp();
    let mut entry_size = 25 + key_size + val_size;
    if has_expiry {
        entry_size += 8;
    }

    let mut res = Vec::with_capacity(entry_size);

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

    let crc = calculate_crc(&res[4..]);
    res[..4].copy_from_slice(&crc.to_be_bytes());

    Ok(res)
}



fn current_timestamp() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(time) => time.as_secs(),
        Err(_e) => 0,
    }
}

fn calculate_crc(data: &[u8]) -> u32 {
    crc32fast::hash(data)
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

    #[error("Failed to recreate UTF8 String")]
    StringDecodeError(#[from] FromUtf8Error)
}

#[derive(Debug, Error)]
pub enum SizeMismatchType {
    #[error("Store header is too small")]
    StoreHeader,
    #[error("Bag root entry is too small")]
    BagRootEntry
}
