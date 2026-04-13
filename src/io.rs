
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::Read;
use std::io::Error;
use std::io::Write;
use std::io::SeekFrom;
use std::path::Path;


pub fn read_all_file(file: &mut File) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut buffer)?;

    Ok(buffer)
}

pub fn read_chunk(file: &mut File, offset: u64, size: u64) -> Result<Vec<u8>, Error> {
    file.seek(SeekFrom::Start(offset))?;
    let mut chunk_buffer = Vec::with_capacity(size as usize);
    file.take(size).read_to_end(&mut chunk_buffer)?;

    Ok(chunk_buffer)
}

pub fn append(file: &mut File, data: &[u8]) -> Result<u64, Error> {
    let len = file.metadata()?.len();
    file.write_all(data)?;
    Ok(len)
}

pub fn write_all(file: &mut File, data: &[u8]) -> Result<(), Error> {
    file.write_all(data)?;
    Ok(())
}

pub fn overwrite(path: &Path, data: &[u8]) -> Result<(), Error> {
    let tmp_path = path.with_added_extension(".tmp");
    let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
    write_all(&mut tmp_file, data)?;

    let back_path = path.with_added_extension(".bak");
    fs::rename(path, &back_path)?;
    fs::rename(tmp_path, path)?;
    fs::remove_file(back_path)?;

    Ok(())
}

pub fn create_file_to_append(path: &Path) -> Result<File, Error> {
    OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
}

pub fn open_file_for_read(path: &Path) -> Result<File, Error> {
    OpenOptions::new()
            .read(true)
            .open(path)
}
