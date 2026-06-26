mod common;
use common::*;


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
