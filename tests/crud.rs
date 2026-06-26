mod common;
use common::*;

use barkv::EngineError;


// ── Basic CRUD ────────────────────────────────────────────────────────────────

#[test]
fn crud_set_and_get() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"key".to_string(), b"hello world").unwrap();
    let val = engine.get(&BAG.to_string(), &"key".to_string()).unwrap();
    assert_eq!(val, b"hello world");
}

#[test]
fn crud_get_missing_key_fails() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let result = engine.get(&BAG.to_string(), &"ghost".to_string());
    assert!(matches!(result, Err(EngineError::NoSuchEntryKeyError(_))));
}

#[test]
fn crud_get_from_missing_bag_fails() {
    let (_dir, engine) = new_engine();
    let result = engine.get(&"no-bag".to_string(), &"k".to_string());
    assert!(matches!(result, Err(EngineError::NoSuchBagKeyError(_))));
}

#[test]
fn crud_set_to_missing_bag_fails() {
    let (_dir, engine) = new_engine();
    let result = engine.set(&"no-bag".to_string(), &"k".to_string(), b"v");
    assert!(matches!(result, Err(EngineError::NoSuchBagKeyError(_))));
}

#[test]
fn crud_overwrite_returns_latest_value() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v1").unwrap();
    engine.set(&BAG.to_string(), &"k".to_string(), b"v2").unwrap();
    engine.set(&BAG.to_string(), &"k".to_string(), b"v3").unwrap();
    let val = engine.get(&BAG.to_string(), &"k".to_string()).unwrap();
    assert_eq!(val, b"v3");
}

#[test]
fn crud_delete_removes_key() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v").unwrap();
    engine.delete(&BAG.to_string(), &"k".to_string()).unwrap();
    assert!(!engine.exists(&BAG.to_string(), &"k".to_string()).unwrap());
    assert!(matches!(
        engine.get(&BAG.to_string(), &"k".to_string()),
        Err(EngineError::NoSuchEntryKeyError(_))
    ));
}

#[test]
fn crud_delete_missing_key_fails() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let result = engine.delete(&BAG.to_string(), &"ghost".to_string());
    assert!(matches!(result, Err(EngineError::NoSuchEntryKeyError(_))));
}

#[test]
fn crud_double_delete_fails() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v").unwrap();
    engine.delete(&BAG.to_string(), &"k".to_string()).unwrap();
    let result = engine.delete(&BAG.to_string(), &"k".to_string());
    assert!(matches!(result, Err(EngineError::NoSuchEntryKeyError(_))));
}

#[test]
fn crud_set_after_delete_works() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v1").unwrap();
    engine.delete(&BAG.to_string(), &"k".to_string()).unwrap();
    engine.set(&BAG.to_string(), &"k".to_string(), b"v2").unwrap();
    let val = engine.get(&BAG.to_string(), &"k".to_string()).unwrap();
    assert_eq!(val, b"v2");
}

#[test]
fn crud_exists_false_for_missing_key() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    assert!(!engine.exists(&BAG.to_string(), &"ghost".to_string()).unwrap());
}

#[test]
fn crud_exists_false_for_missing_bag() {
    let (_dir, engine) = new_engine();
    assert!(!engine.exists(&"no-bag".to_string(), &"k".to_string()).unwrap_or(false));
}

#[test]
fn crud_exists_true_after_set() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v").unwrap();
    assert!(engine.exists(&BAG.to_string(), &"k".to_string()).unwrap());
}

#[test]
fn crud_list_keys() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k1".to_string(), b"v").unwrap();
    engine.set(&BAG.to_string(), &"k2".to_string(), b"v").unwrap();
    engine.set(&BAG.to_string(), &"k3".to_string(), b"v").unwrap();
    let keys = engine.list_keys(&BAG.to_string()).unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&"k1".to_string()));
    assert!(keys.contains(&"k2".to_string()));
    assert!(keys.contains(&"k3".to_string()));
}

#[test]
fn crud_list_keys_excludes_deleted() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k1".to_string(), b"v").unwrap();
    engine.set(&BAG.to_string(), &"k2".to_string(), b"v").unwrap();
    engine.delete(&BAG.to_string(), &"k1".to_string()).unwrap();
    let keys = engine.list_keys(&BAG.to_string()).unwrap();
    assert_eq!(keys.len(), 1);
    assert!(keys.contains(&"k2".to_string()));
}

#[test]
fn crud_multiple_bags_are_isolated() {
    let (_dir, engine) = new_engine();
    engine.create_bag(&"a".to_string()).unwrap();
    engine.create_bag(&"b".to_string()).unwrap();
    engine.set(&"a".to_string(), &"k".to_string(), b"from-a").unwrap();
    engine.set(&"b".to_string(), &"k".to_string(), b"from-b").unwrap();

    assert_eq!(engine.get(&"a".to_string(), &"k".to_string()).unwrap(), b"from-a");
    assert_eq!(engine.get(&"b".to_string(), &"k".to_string()).unwrap(), b"from-b");

    engine.delete(&"a".to_string(), &"k".to_string()).unwrap();
    assert!(!engine.exists(&"a".to_string(), &"k".to_string()).unwrap());
    assert!(engine.exists(&"b".to_string(), &"k".to_string()).unwrap());
}


// ── Edge case values ──────────────────────────────────────────────────────────

#[test]
fn edge_empty_value() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"").unwrap();
    let val = engine.get(&BAG.to_string(), &"k".to_string()).unwrap();
    assert_eq!(val, b"");
}

#[test]
fn edge_large_value() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let large: Vec<u8> = (0..8192u16).map(|i| (i % 256) as u8).collect();
    engine.set(&BAG.to_string(), &"big".to_string(), &large).unwrap();
    let val = engine.get(&BAG.to_string(), &"big".to_string()).unwrap();
    assert_eq!(val, large);
}

#[test]
fn edge_all_byte_values_roundtrip() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let all_bytes: Vec<u8> = (0u8..=255).collect();
    engine.set(&BAG.to_string(), &"bin".to_string(), &all_bytes).unwrap();
    let val = engine.get(&BAG.to_string(), &"bin".to_string()).unwrap();
    assert_eq!(val, all_bytes);
}

#[test]
fn edge_unicode_key() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let key = "こんにちは-🦀-Ünïcödé".to_string();
    engine.set(&BAG.to_string(), &key, b"value").unwrap();
    let val = engine.get(&BAG.to_string(), &key).unwrap();
    assert_eq!(val, b"value");
    assert!(engine.exists(&BAG.to_string(), &key).unwrap());
}

#[test]
fn edge_many_distinct_keys() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    for i in 0..50 {
        engine
            .set(&BAG.to_string(), &format!("key-{i}"), &[i as u8])
            .unwrap();
    }
    for i in 0..50 {
        let val = engine.get(&BAG.to_string(), &format!("key-{i}")).unwrap();
        assert_eq!(val, [i as u8]);
    }
}
