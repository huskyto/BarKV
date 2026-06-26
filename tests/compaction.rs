mod common;
use common::*;


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
