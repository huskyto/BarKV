mod common;
use common::*;

#[test]
fn atomic_get_or_set_on_missing_creates_entry() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let val = engine
        .get_or_set(&BAG.to_string(), &"k".to_string(), b"default")
        .unwrap();
    assert_eq!(val, b"default");
    assert!(engine.exists(&BAG.to_string(), &"k".to_string()).unwrap());
}

#[test]
fn atomic_get_or_set_on_existing_returns_existing_without_overwriting() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set(&BAG.to_string(), &"k".to_string(), b"original")
        .unwrap();
    let val = engine
        .get_or_set(&BAG.to_string(), &"k".to_string(), b"default")
        .unwrap();
    assert_eq!(val, b"original");
    assert_eq!(
        engine.get(&BAG.to_string(), &"k".to_string()).unwrap(),
        b"original"
    );
}

#[test]
fn atomic_update_if_different_updates_when_value_differs() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set(&BAG.to_string(), &"k".to_string(), b"old")
        .unwrap();
    engine
        .update_if_different(&BAG.to_string(), &"k".to_string(), b"new")
        .unwrap();
    assert_eq!(
        engine.get(&BAG.to_string(), &"k".to_string()).unwrap(),
        b"new"
    );
}

#[test]
fn atomic_update_if_different_no_write_when_same() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set(&BAG.to_string(), &"k".to_string(), b"same")
        .unwrap();
    engine
        .update_if_different(&BAG.to_string(), &"k".to_string(), b"same")
        .unwrap();
    assert_eq!(
        engine.get(&BAG.to_string(), &"k".to_string()).unwrap(),
        b"same"
    );
}

#[test]
fn atomic_get_and_delete_returns_value_and_removes_key() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set(&BAG.to_string(), &"k".to_string(), b"v")
        .unwrap();
    let val = engine
        .get_and_delete(&BAG.to_string(), &"k".to_string())
        .unwrap();
    assert_eq!(val, b"v");
    assert!(!engine.exists(&BAG.to_string(), &"k".to_string()).unwrap());
}

#[test]
fn atomic_get_and_delete_on_missing_key_fails() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let result = engine.get_and_delete(&BAG.to_string(), &"ghost".to_string());
    assert!(result.is_err());
}
