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


// ── Close ─────────────────────────────────────────────────────────────────────

#[test]
fn lifecycle_close_succeeds() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v").unwrap();
    assert!(engine.close().is_ok());
}

#[test]
fn lifecycle_close_blocks_subsequent_ops() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v").unwrap();
    engine.close().unwrap();

    assert!(matches!(
        engine.get(&BAG.to_string(), &"k".to_string()),
        Err(EngineError::StoreClosed)
    ));
    assert!(matches!(
        engine.set(&BAG.to_string(), &"k".to_string(), b"v2"),
        Err(EngineError::StoreClosed)
    ));
    assert!(matches!(
        engine.delete(&BAG.to_string(), &"k".to_string()),
        Err(EngineError::StoreClosed)
    ));
    assert!(matches!(
        engine.list_keys(&BAG.to_string()),
        Err(EngineError::StoreClosed)
    ));
    assert!(matches!(engine.list_bags(), Err(EngineError::StoreClosed)));
    assert!(matches!(
        engine.create_bag(&"other".to_string()),
        Err(EngineError::StoreClosed)
    ));
}

#[test]
fn lifecycle_close_twice_errors() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.close().unwrap();
    // A closed store rejects further operations, including close() itself.
    assert!(matches!(engine.close(), Err(EngineError::StoreClosed)));
}

#[test]
fn lifecycle_close_then_reopen_data_intact() {
    let (dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k1".to_string(), b"v1").unwrap();
    engine.set(&BAG.to_string(), &"k2".to_string(), b"v2").unwrap();
    engine.close().unwrap();

    let engine = reopen(&dir);
    assert_eq!(engine.get(&BAG.to_string(), &"k1".to_string()).unwrap(), b"v1");
    assert_eq!(engine.get(&BAG.to_string(), &"k2".to_string()).unwrap(), b"v2");
}
