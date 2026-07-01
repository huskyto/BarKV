mod common;
use common::*;

#[test]
fn validate_fresh_store_is_clean() {
    let (_dir, engine) = new_engine();
    assert!(engine.validate().is_empty());
}

#[test]
fn validate_after_crud_operations_is_clean() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    for i in 0..20u8 {
        engine
            .set(&BAG.to_string(), &format!("k{i}"), &[i])
            .unwrap();
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
        engine
            .set(&BAG.to_string(), &format!("k{i}"), &[i])
            .unwrap();
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
