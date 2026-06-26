mod common;
use common::*;

use barkv::model::KVPair;


#[test]
fn batch_set_many_and_get_many() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let pairs = vec![
        KVPair { key: "k1".to_string(), value: b"v1".to_vec() },
        KVPair { key: "k2".to_string(), value: b"v2".to_vec() },
        KVPair { key: "k3".to_string(), value: b"v3".to_vec() },
    ];
    engine.set_many(&BAG.to_string(), pairs).unwrap();
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), 3);

    let k1 = "k1".to_string();
    let k2 = "k2".to_string();
    let k3 = "k3".to_string();
    let results = engine.get_many(&BAG.to_string(), &[&k1, &k2, &k3]).unwrap();
    assert_eq!(results.len(), 3);
    let found: Vec<_> = results.iter().map(|kv| kv.key.as_str()).collect();
    assert!(found.contains(&"k1"));
    assert!(found.contains(&"k2"));
    assert!(found.contains(&"k3"));
}

#[test]
fn batch_get_many_ignores_missing_keys() {
    // Design: missing keys in batch ops are silently skipped, not errors.
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k1".to_string(), b"v1").unwrap();

    let k1 = "k1".to_string();
    let ghost = "ghost".to_string();
    let results = engine.get_many(&BAG.to_string(), &[&k1, &ghost]).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].key, "k1");
}

#[test]
fn batch_delete_many_ignores_missing_keys() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k1".to_string(), b"v1").unwrap();
    engine.set(&BAG.to_string(), &"k2".to_string(), b"v2").unwrap();

    let k1 = "k1".to_string();
    let ghost = "ghost".to_string();
    engine.delete_many(&BAG.to_string(), &[&k1, &ghost]).unwrap();
    assert!(!engine.exists(&BAG.to_string(), &"k1".to_string()).unwrap());
    assert!(engine.exists(&BAG.to_string(), &"k2".to_string()).unwrap());
}

#[test]
fn batch_set_many_empty_is_ok() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set_many(&BAG.to_string(), vec![]).unwrap();
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), 0);
}

#[test]
fn batch_get_many_empty_returns_empty() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let results = engine.get_many(&BAG.to_string(), &[]).unwrap();
    assert!(results.is_empty());
}

#[test]
fn batch_delete_many_all_missing_is_ok() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let ghost = "ghost".to_string();
    engine.delete_many(&BAG.to_string(), &[&ghost]).unwrap();
}

#[test]
fn batch_set_many_persists_across_reopen() {
    let (dir, engine) = new_engine_with_bag(BAG);
    engine
        .set_many(
            &BAG.to_string(),
            vec![
                KVPair { key: "a".to_string(), value: b"1".to_vec() },
                KVPair { key: "b".to_string(), value: b"2".to_vec() },
            ],
        )
        .unwrap();
    drop(engine);

    let engine = reopen(&dir);
    assert_eq!(engine.get(&BAG.to_string(), &"a".to_string()).unwrap(), b"1");
    assert_eq!(engine.get(&BAG.to_string(), &"b".to_string()).unwrap(), b"2");
}
