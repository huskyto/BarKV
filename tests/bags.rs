mod common;
use common::*;

use barkv::EngineError;

#[test]
fn bag_create_and_list() {
    let (_dir, engine) = new_engine();
    engine.create_bag(&"bag-a".to_string()).unwrap();
    engine.create_bag(&"bag-b".to_string()).unwrap();
    let bags = engine.list_bags().unwrap();
    assert_eq!(bags.len(), 2);
    assert!(bags.contains(&"bag-a".to_string()));
    assert!(bags.contains(&"bag-b".to_string()));
}

#[test]
fn bag_create_duplicate_fails() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let result = engine.create_bag(&BAG.to_string());
    assert!(matches!(result, Err(EngineError::BagAlreadyExistsError(_))));
}

#[test]
fn bag_drop_removes_from_list() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.drop_bag(&BAG.to_string()).unwrap();
    assert!(engine.list_bags().unwrap().is_empty());
}

#[test]
fn bag_drop_nonexistent_fails() {
    let (_dir, engine) = new_engine();
    let result = engine.drop_bag(&"ghost".to_string());
    assert!(matches!(result, Err(EngineError::NoSuchBagKeyError(_))));
}

#[test]
fn bag_dropped_bag_rejects_further_ops() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set(&BAG.to_string(), &"k".to_string(), b"v")
        .unwrap();
    engine.drop_bag(&BAG.to_string()).unwrap();
    assert!(engine.get(&BAG.to_string(), &"k".to_string()).is_err());
    assert!(
        engine
            .set(&BAG.to_string(), &"k".to_string(), b"v")
            .is_err()
    );
    assert!(engine.list_keys(&BAG.to_string()).is_err());
}

#[test]
fn bag_len_empty() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), 0);
}

#[test]
fn bag_len_after_set_and_delete() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set(&BAG.to_string(), &"k1".to_string(), b"v1")
        .unwrap();
    engine
        .set(&BAG.to_string(), &"k2".to_string(), b"v2")
        .unwrap();
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), 2);
    engine.delete(&BAG.to_string(), &"k1".to_string()).unwrap();
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), 1);
}

#[test]
fn bag_overwrite_does_not_inflate_len() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set(&BAG.to_string(), &"k".to_string(), b"v1")
        .unwrap();
    engine
        .set(&BAG.to_string(), &"k".to_string(), b"v2")
        .unwrap();
    engine
        .set(&BAG.to_string(), &"k".to_string(), b"v3")
        .unwrap();
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), 1);
}

#[test]
fn bag_persist_across_reopen() {
    let (dir, engine) = new_engine();
    engine.create_bag(&"a".to_string()).unwrap();
    engine.create_bag(&"b".to_string()).unwrap();
    drop(engine);

    let engine = reopen(&dir);
    let bags = engine.list_bags().unwrap();
    assert_eq!(bags.len(), 2);
    assert!(bags.contains(&"a".to_string()));
    assert!(bags.contains(&"b".to_string()));
}

#[test]
fn bag_drop_persists_across_reopen() {
    let (dir, engine) = new_engine();
    engine.create_bag(&"a".to_string()).unwrap();
    engine.create_bag(&"b".to_string()).unwrap();
    engine.drop_bag(&"a".to_string()).unwrap();
    drop(engine);

    let engine = reopen(&dir);
    assert_eq!(engine.list_bags().unwrap(), vec!["b".to_string()]);
}
