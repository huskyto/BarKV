mod common;
use common::*;

use tempfile::TempDir;

use barkv::BarKV;
use barkv::EngineError;


#[test]
fn lifecycle_create_then_open() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    drop(BarKV::create(path).unwrap());
    let engine = BarKV::open(path).unwrap();
    assert!(engine.list_bags().unwrap().is_empty());
}

#[test]
fn lifecycle_open_or_create_on_empty_dir() {
    let dir = TempDir::new().unwrap();
    let engine = BarKV::open_or_create(dir.path().to_str().unwrap()).unwrap();
    assert!(engine.list_bags().unwrap().is_empty());
}

#[test]
fn lifecycle_open_or_create_opens_existing_store() {
    let (dir, engine) = new_engine();
    engine.create_bag(&BAG.to_string()).unwrap();
    drop(engine);
    let engine = BarKV::open_or_create(dir.path().to_str().unwrap()).unwrap();
    assert!(engine.list_bags().unwrap().contains(&BAG.to_string()));
}

#[test]
fn lifecycle_create_on_non_empty_dir_fails() {
    let (dir, _engine) = new_engine();
    let result = BarKV::create(dir.path().to_str().unwrap());
    assert!(matches!(result, Err(EngineError::RootPathNotEmpty)));
}

#[test]
fn lifecycle_open_on_dir_without_store_file_fails() {
    let dir = TempDir::new().unwrap();
    let result = BarKV::open(dir.path().to_str().unwrap());
    assert!(matches!(result, Err(EngineError::RootFileNotFound)));
}

#[test]
fn lifecycle_open_on_invalid_path_fails() {
    let result = BarKV::open("/this/path/does/not/exist/at/all");
    assert!(result.is_err());
}
