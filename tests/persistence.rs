mod common;
use common::*;

use barkv::EngineError;


#[test]
fn persist_data_survives_reopen() {
    let (dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k1".to_string(), b"v1").unwrap();
    engine.set(&BAG.to_string(), &"k2".to_string(), b"v2").unwrap();
    drop(engine);

    let engine = reopen(&dir);
    assert_eq!(engine.get(&BAG.to_string(), &"k1".to_string()).unwrap(), b"v1");
    assert_eq!(engine.get(&BAG.to_string(), &"k2".to_string()).unwrap(), b"v2");
}

#[test]
fn persist_deletes_survive_reopen() {
    let (dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k1".to_string(), b"v1").unwrap();
    engine.set(&BAG.to_string(), &"k2".to_string(), b"v2").unwrap();
    engine.delete(&BAG.to_string(), &"k1".to_string()).unwrap();
    drop(engine);

    let engine = reopen(&dir);
    assert!(matches!(
        engine.get(&BAG.to_string(), &"k1".to_string()),
        Err(EngineError::NoSuchEntryKeyError(_))
    ));
    assert_eq!(engine.get(&BAG.to_string(), &"k2".to_string()).unwrap(), b"v2");
}

#[test]
fn persist_overwrites_survive_reopen_with_latest_value() {
    let (dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v1").unwrap();
    engine.set(&BAG.to_string(), &"k".to_string(), b"v2").unwrap();
    engine.set(&BAG.to_string(), &"k".to_string(), b"v3").unwrap();
    drop(engine);

    let engine = reopen(&dir);
    assert_eq!(engine.get(&BAG.to_string(), &"k".to_string()).unwrap(), b"v3");
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), 1);
}

#[test]
fn persist_delete_then_reset_survives_reopen() {
    let (dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v1").unwrap();
    engine.delete(&BAG.to_string(), &"k".to_string()).unwrap();
    engine.set(&BAG.to_string(), &"k".to_string(), b"v2").unwrap();
    drop(engine);

    let engine = reopen(&dir);
    assert_eq!(engine.get(&BAG.to_string(), &"k".to_string()).unwrap(), b"v2");
}

#[test]
fn persist_empty_bag_survives_reopen() {
    let (dir, engine) = new_engine_with_bag(BAG);
    drop(engine);

    let engine = reopen(&dir);
    assert!(engine.list_bags().unwrap().contains(&BAG.to_string()));
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), 0);
}
