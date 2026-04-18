
use tempfile::TempDir;
use std::time::SystemTime;

use bar_kv::model::KVPair;
use bar_kv::engine::BarKVEngine;
use bar_kv::engine::EngineError;

use std::time::UNIX_EPOCH;


// ── Helpers ──────────────────────────────────────────────────────────────────

fn new_engine() -> (TempDir, BarKVEngine) {
    let dir = TempDir::new().unwrap();
    let engine = BarKVEngine::create(dir.path().to_str().unwrap()).unwrap();
    (dir, engine)
}

fn new_engine_with_bag(bag: &str) -> (TempDir, BarKVEngine) {
    let (dir, engine) = new_engine();
    engine.create_bag(&bag.to_string()).unwrap();
    (dir, engine)
}

fn reopen(dir: &TempDir) -> BarKVEngine {
    BarKVEngine::open(dir.path().to_str().unwrap()).unwrap()
}

/// Write enough unique entries to push the active file past MIN_LOCK_SIZE
/// (10 000 bytes), triggering at least `rotations` file rotations.
///
/// Each entry is ~(25 header + key_bytes + value_bytes).
/// With an 8-byte key and 100-byte value that is 133 bytes/entry.
/// ⌈10 000 / 133⌉ ≈ 76 entries per file, so `76 * rotations + margin`.
fn fill_to_rotations(engine: &BarKVEngine, bag: &str, rotations: usize) {
    let value = vec![0xABu8; 100];
    let count = 80 * rotations;
    for i in 0..count {
        engine
            .set(&bag.to_string(), &format!("key-{i:05}"), &value)
            .unwrap();
    }
}

const BAG: &str = "test-bag";

fn now_millis() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(time) => time.as_millis(),
        Err(_e) => 0,
    }
}


// ── Lifecycle ─────────────────────────────────────────────────────────────────

#[test]
fn lifecycle_create_then_open() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    drop(BarKVEngine::create(path).unwrap());
    let engine = BarKVEngine::open(path).unwrap();
    assert!(engine.list_bags().unwrap().is_empty());
}

#[test]
fn lifecycle_open_or_create_on_empty_dir() {
    let dir = TempDir::new().unwrap();
    let engine = BarKVEngine::open_or_create(dir.path().to_str().unwrap()).unwrap();
    assert!(engine.list_bags().unwrap().is_empty());
}

#[test]
fn lifecycle_open_or_create_opens_existing_store() {
    let (dir, engine) = new_engine();
    engine.create_bag(&BAG.to_string()).unwrap();
    drop(engine);
    let engine = BarKVEngine::open_or_create(dir.path().to_str().unwrap()).unwrap();
    assert!(engine.list_bags().unwrap().contains(&BAG.to_string()));
}

#[test]
fn lifecycle_create_on_non_empty_dir_fails() {
    let (dir, _engine) = new_engine();
    let result = BarKVEngine::create(dir.path().to_str().unwrap());
    assert!(matches!(result, Err(EngineError::RootPathNotEmpty)));
}

#[test]
fn lifecycle_open_on_dir_without_store_file_fails() {
    let dir = TempDir::new().unwrap();
    let result = BarKVEngine::open(dir.path().to_str().unwrap());
    assert!(matches!(result, Err(EngineError::RootFileNotFound)));
}

#[test]
fn lifecycle_open_on_invalid_path_fails() {
    let result = BarKVEngine::open("/this/path/does/not/exist/at/all");
    assert!(result.is_err());
}


// ── Bag operations ───────────────────────────────────────────────────────────

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
    engine.set(&BAG.to_string(), &"k".to_string(), b"v").unwrap();
    engine.drop_bag(&BAG.to_string()).unwrap();
    assert!(engine.get(&BAG.to_string(), &"k".to_string()).is_err());
    assert!(engine.set(&BAG.to_string(), &"k".to_string(), b"v").is_err());
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
    engine.set(&BAG.to_string(), &"k1".to_string(), b"v1").unwrap();
    engine.set(&BAG.to_string(), &"k2".to_string(), b"v2").unwrap();
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), 2);
    engine.delete(&BAG.to_string(), &"k1".to_string()).unwrap();
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), 1);
}

#[test]
fn bag_overwrite_does_not_inflate_len() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v1").unwrap();
    engine.set(&BAG.to_string(), &"k".to_string(), b"v2").unwrap();
    engine.set(&BAG.to_string(), &"k".to_string(), b"v3").unwrap();
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


// ── Persistence ───────────────────────────────────────────────────────────────

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


// ── TTL ───────────────────────────────────────────────────────────────────────

#[test]
fn ttl_entry_readable_before_expiry() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set_with_expiry(&BAG.to_string(), &"k".to_string(), b"val", 60_000)
        .unwrap();
    let val = engine.get(&BAG.to_string(), &"k".to_string()).unwrap();
    assert_eq!(val, b"val");
}

#[test]
fn ttl_entry_expired_at_zero_ttl() {
    // TTL=0 means expiry = now, which is <= now on the next read.
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set_with_expiry(&BAG.to_string(), &"k".to_string(), b"val", 0)
        .unwrap();
    let result = engine.get(&BAG.to_string(), &"k".to_string());
    assert!(result.is_err(), "Expected error for expired entry, got Ok");
}

#[test]
fn ttl_on_non_expiring_entry_errors() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v").unwrap();
    let result = engine.ttl(&BAG.to_string(), &"k".to_string());
    assert!(result.is_err());
}

#[test]
fn ttl_on_expiring_entry_returns_future_timestamp() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set_with_expiry(&BAG.to_string(), &"k".to_string(), b"v", 60_000)
        .unwrap();
    let expiry = engine.ttl(&BAG.to_string(), &"k".to_string()).unwrap();
    assert!(expiry > now_millis(), "Expiry should be in the future");
}

#[test]
fn ttl_persist_removes_expiry() {
    // persist() should re-write the entry without the TTL flag.
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set_with_expiry(&BAG.to_string(), &"k".to_string(), b"val", 60_000)
        .unwrap();
    engine.persist(&BAG.to_string(), &"k".to_string()).unwrap();

    // ttl() should now fail — no expiry on entry
    assert!(engine.ttl(&BAG.to_string(), &"k".to_string()).is_err());
    // value should still be readable
    assert_eq!(engine.get(&BAG.to_string(), &"k".to_string()).unwrap(), b"val");
}

#[test]
fn ttl_regular_set_removes_expiry() {
    // Design spec: "Regular 'set' will remove expiry."
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set_with_expiry(&BAG.to_string(), &"k".to_string(), b"v1", 60_000)
        .unwrap();
    engine.set(&BAG.to_string(), &"k".to_string(), b"v2").unwrap();

    let val = engine.get(&BAG.to_string(), &"k".to_string()).unwrap();
    assert_eq!(val, b"v2");
    // The new entry was written without an expiry, so ttl() should error.
    assert!(engine.ttl(&BAG.to_string(), &"k".to_string()).is_err());
}

#[test]
fn ttl_survives_reopen() {
    let (dir, engine) = new_engine_with_bag(BAG);
    engine
        .set_with_expiry(&BAG.to_string(), &"k".to_string(), b"v", 60_000)
        .unwrap();
    drop(engine);

    let engine = reopen(&dir);
    let val = engine.get(&BAG.to_string(), &"k".to_string()).unwrap();
    assert_eq!(val, b"v");
    let expiry = engine.ttl(&BAG.to_string(), &"k".to_string()).unwrap();
    assert!(expiry > now_millis());
}

#[test]
fn ttl_expired_entry_removed_by_partial_compaction() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine
        .set_with_expiry(&BAG.to_string(), &"dead".to_string(), b"v", 0)
        .unwrap();
    engine
        .set(&BAG.to_string(), &"live".to_string(), b"alive")
        .unwrap();
    for result in engine.compact_active() {
        result.1.unwrap();
    }
    // Expired key should be gone
    assert!(!engine.exists(&BAG.to_string(), &"dead".to_string()).unwrap());
    // Live key must survive
    assert_eq!(
        engine.get(&BAG.to_string(), &"live".to_string()).unwrap(),
        b"alive"
    );
}

#[test]
fn ttl_persist_then_reopen() {
    // After persist() the entry should survive reopen without an expiry.
    let (dir, engine) = new_engine_with_bag(BAG);
    engine
        .set_with_expiry(&BAG.to_string(), &"k".to_string(), b"persistent", 60_000)
        .unwrap();
    engine.persist(&BAG.to_string(), &"k".to_string()).unwrap();
    drop(engine);

    let engine = reopen(&dir);
    assert_eq!(
        engine.get(&BAG.to_string(), &"k".to_string()).unwrap(),
        b"persistent"
    );
    assert!(engine.ttl(&BAG.to_string(), &"k".to_string()).is_err());
}


// ── Batch operations ──────────────────────────────────────────────────────────

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


// ── Atomic-like operations ────────────────────────────────────────────────────

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
    engine.set(&BAG.to_string(), &"k".to_string(), b"original").unwrap();
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
    engine.set(&BAG.to_string(), &"k".to_string(), b"old").unwrap();
    engine
        .update_if_different(&BAG.to_string(), &"k".to_string(), b"new")
        .unwrap();
    assert_eq!(engine.get(&BAG.to_string(), &"k".to_string()).unwrap(), b"new");
}

#[test]
fn atomic_update_if_different_no_write_when_same() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"same").unwrap();
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
    engine.set(&BAG.to_string(), &"k".to_string(), b"v").unwrap();
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


// ── Partial compaction ────────────────────────────────────────────────────────

#[test]
fn compact_active_preserves_all_live_keys() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    for i in 0..20u8 {
        engine
            .set(&BAG.to_string(), &format!("k{i}"), &[i])
            .unwrap();
    }
    for (_, res) in engine.compact_active() {
        res.unwrap();
    }
    for i in 0..20u8 {
        assert_eq!(engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(), [i]);
    }
}

#[test]
fn compact_active_deduplicates_overwritten_keys() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    for _ in 0..5 {
        engine.set(&BAG.to_string(), &"k".to_string(), b"noise").unwrap();
    }
    engine.set(&BAG.to_string(), &"k".to_string(), b"final").unwrap();
    for (_, res) in engine.compact_active() {
        res.unwrap();
    }
    assert_eq!(engine.get(&BAG.to_string(), &"k".to_string()).unwrap(), b"final");
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), 1);
}

#[test]
fn compact_active_keeps_deleted_key_gone() {
    // Partial compaction keeps tombstones as safeguards; the key must remain absent.
    let (_dir, engine) = new_engine_with_bag(BAG);
    engine.set(&BAG.to_string(), &"k".to_string(), b"v").unwrap();
    engine.delete(&BAG.to_string(), &"k".to_string()).unwrap();
    for (_, res) in engine.compact_active() {
        res.unwrap();
    }
    assert!(!engine.exists(&BAG.to_string(), &"k".to_string()).unwrap());
}

#[test]
fn compact_active_result_survives_reopen() {
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..10u8 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &[i]).unwrap();
    }
    // Overwrite first half
    for i in 0..5u8 {
        engine
            .set(&BAG.to_string(), &format!("k{i}"), &[i + 100])
            .unwrap();
    }
    for (_, res) in engine.compact_active() {
        res.unwrap();
    }
    drop(engine);

    let engine = reopen(&dir);
    for i in 0..5u8 {
        assert_eq!(
            engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(),
            [i + 100]
        );
    }
    for i in 5..10u8 {
        assert_eq!(
            engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(),
            [i]
        );
    }
}

#[test]
fn compact_active_empty_bag_is_ok() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    for (_, res) in engine.compact_active() {
        res.unwrap();
    }
}


// ── Full compaction ───────────────────────────────────────────────────────────

#[test]
fn full_compaction_preserves_all_live_keys() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    for i in 0..20u8 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &[i]).unwrap();
    }
    for (_, res) in engine.full_compaction() {
        res.unwrap();
    }
    for i in 0..20u8 {
        assert_eq!(engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(), [i]);
    }
}

#[test]
fn full_compaction_removes_deleted_entries() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    for i in 0..10u8 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &[i]).unwrap();
    }
    for i in 0..5u8 {
        engine.delete(&BAG.to_string(), &format!("k{i}")).unwrap();
    }
    for (_, res) in engine.full_compaction() {
        res.unwrap();
    }
    for i in 0..5u8 {
        assert!(!engine.exists(&BAG.to_string(), &format!("k{i}")).unwrap());
    }
    for i in 5..10u8 {
        assert_eq!(
            engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(),
            [i]
        );
    }
}

#[test]
fn full_compaction_result_survives_reopen() {
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..20u8 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &[i]).unwrap();
    }
    for i in 0..10u8 {
        engine.delete(&BAG.to_string(), &format!("k{i}")).unwrap();
    }
    for (_, res) in engine.full_compaction() {
        res.unwrap();
    }
    drop(engine);

    let engine = reopen(&dir);
    for i in 0..10u8 {
        assert!(!engine.exists(&BAG.to_string(), &format!("k{i}")).unwrap());
    }
    for i in 10..20u8 {
        assert_eq!(
            engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(),
            [i]
        );
    }
}


// ── File rotation (triggers when active file exceeds MIN_LOCK_SIZE = 10 000 B) ─

#[test]
fn rotation_all_keys_readable_after_one_rotation() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    fill_to_rotations(&engine, BAG, 1);
    for i in 0..80 {
        assert!(
            engine.exists(&BAG.to_string(), &format!("key-{i:05}")).unwrap(),
            "key-{i:05} missing after rotation"
        );
    }
}

#[test]
fn rotation_data_survives_reopen_after_one_rotation() {
    let (dir, engine) = new_engine_with_bag(BAG);
    fill_to_rotations(&engine, BAG, 1);
    drop(engine);

    let engine = reopen(&dir);
    for i in 0..80 {
        let val = engine.get(&BAG.to_string(), &format!("key-{i:05}")).unwrap();
        assert_eq!(val, vec![0xABu8; 100]);
    }
}

#[test]
fn rotation_data_survives_reopen_after_two_rotations() {
    let (dir, engine) = new_engine_with_bag(BAG);
    fill_to_rotations(&engine, BAG, 2);
    drop(engine);

    let engine = reopen(&dir);
    for i in 0..160 {
        let val = engine.get(&BAG.to_string(), &format!("key-{i:05}")).unwrap();
        assert_eq!(val, vec![0xABu8; 100]);
    }
}

#[test]
fn rotation_full_compaction_then_reopen() {
    // Write enough for two rotations, delete half, full compact, reopen, verify.
    let (dir, engine) = new_engine_with_bag(BAG);
    let value = vec![0xCDu8; 100];
    // Write 160 entries → 2 rotations
    for i in 0..160 {
        engine
            .set(&BAG.to_string(), &format!("key-{i:05}"), &value)
            .unwrap();
    }
    // Delete first 80
    for i in 0..80 {
        engine
            .delete(&BAG.to_string(), &format!("key-{i:05}"))
            .unwrap();
    }
    for (_, res) in engine.full_compaction() {
        res.unwrap();
    }
    drop(engine);

    let engine = reopen(&dir);
    for i in 0..80 {
        assert!(
            !engine.exists(&BAG.to_string(), &format!("key-{i:05}")).unwrap(),
            "Deleted key-{i:05} still present after compaction+reopen"
        );
    }
    for i in 80..160 {
        let val = engine.get(&BAG.to_string(), &format!("key-{i:05}")).unwrap();
        assert_eq!(val, value);
    }
}

#[test]
fn rotation_overwrite_across_files_returns_latest() {
    // Write enough to trigger a rotation, then overwrite every key.
    // The latest value must be returned regardless of which file it lives in.
    let (_dir, engine) = new_engine_with_bag(BAG);
    fill_to_rotations(&engine, BAG, 1);
    // Overwrite all keys with a different value
    for i in 0..80 {
        engine
            .set(
                &BAG.to_string(),
                &format!("key-{i:05}"),
                &[i as u8],
            )
            .unwrap();
    }
    for i in 0..80 {
        let val = engine.get(&BAG.to_string(), &format!("key-{i:05}")).unwrap();
        assert_eq!(val, [i as u8]);
    }
}

#[test]
fn rotation_validate_after_full_compaction() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    fill_to_rotations(&engine, BAG, 2);
    for (_, res) in engine.full_compaction() {
        res.unwrap();
    }
    let failures = engine.validate();
    assert!(failures.is_empty(), "Validation failures: {failures:?}");
}


// ── Validation ────────────────────────────────────────────────────────────────

#[test]
fn validate_fresh_store_is_clean() {
    let (_dir, engine) = new_engine();
    assert!(engine.validate().is_empty());
}

#[test]
fn validate_after_crud_operations_is_clean() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    for i in 0..20u8 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &[i]).unwrap();
    }
    for i in 0..10u8 {
        engine.delete(&BAG.to_string(), &format!("k{i}")).unwrap();
    }
    let failures = engine.validate();
    assert!(failures.is_empty(), "Validation failures: {failures:?}");
}

#[test]
fn validate_after_reopen_is_clean() {
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..10u8 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &[i]).unwrap();
    }
    drop(engine);
    let engine = reopen(&dir);
    let failures = engine.validate();
    assert!(failures.is_empty(), "Validation failures: {failures:?}");
}

#[test]
fn validate_multi_bag_store_is_clean() {
    let (_dir, engine) = new_engine();
    for b in ["bag-1", "bag-2", "bag-3"] {
        engine.create_bag(&b.to_string()).unwrap();
        engine.set(&b.to_string(), &"k".to_string(), b"v").unwrap();
    }
    let failures = engine.validate();
    assert!(failures.is_empty(), "Validation failures: {failures:?}");
}
