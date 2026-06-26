mod common;
use common::*;

use std::fs;
use std::path::PathBuf;

use barkv::BarKV;

// On-disk file paths, derived from the store root.
fn bag_file(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join(format!("{BAG}-0.bkv"))
}

fn root_store_file(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("barkv.store")
}


// ── Truncation / torn-write recovery ──────────────────────────────────────────
// DESIGN.md "Error Recovery": "On truncated files, the store will regenerate all
// the valid entries present in the log."

#[test]
fn recovery_torn_trailing_write_recovers_committed_entries() {
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..5 {
        engine
            .set(&BAG.to_string(), &format!("k{i}"), format!("v{i}").as_bytes())
            .unwrap();
    }
    drop(engine);

    // Simulate a crash mid-append: a few stray bytes of a partial entry at the tail.
    let path = bag_file(&dir);
    let mut data = fs::read(&path).unwrap();
    data.extend_from_slice(&[0xFFu8; 7]);
    fs::write(&path, &data).unwrap();

    let engine = BarKV::open(dir.path().to_str().unwrap())
        .expect("store should recover by replaying the valid log and dropping the partial tail");
    for i in 0..5 {
        assert_eq!(
            engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(),
            format!("v{i}").into_bytes()
        );
    }
}

#[test]
fn recovery_truncated_tail_recovers_earlier_entries() {
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..5 {
        engine
            .set(&BAG.to_string(), &format!("k{i}"), format!("v{i}").as_bytes())
            .unwrap();
    }
    drop(engine);

    // Cut 5 bytes off the end. Each entry is >= 25 bytes, so only the last entry
    // is left incomplete; k0..k3 remain fully written.
    let path = bag_file(&dir);
    let data = fs::read(&path).unwrap();
    fs::write(&path, &data[..data.len() - 5]).unwrap();

    let engine = BarKV::open(dir.path().to_str().unwrap())
        .expect("store should reopen, keeping every fully-written entry");
    for i in 0..4 {
        assert_eq!(
            engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(),
            format!("v{i}").into_bytes()
        );
    }
    assert!(
        !engine.exists(&BAG.to_string(), &"k4".to_string()).unwrap(),
        "the partially-truncated trailing entry must not be recovered"
    );
}


// ── Corruption detection ──────────────────────────────────────────────────────
// DESIGN.md "Error Recovery": "Will detect corruption via CRC check, but will not
// fix data." Detection is surfaced through validate(); we do not assert that open()
// itself recovers from corruption.

#[test]
fn recovery_crc_corruption_detected_by_validate() {
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..5 {
        engine
            .set(&BAG.to_string(), &format!("k{i}"), format!("v{i}").as_bytes())
            .unwrap();
    }

    // Flip the last byte of the bag file (inside the last entry's value) so its
    // stored CRC no longer matches. Engine stays open; validate() re-reads disk.
    let path = bag_file(&dir);
    let mut data = fs::read(&path).unwrap();
    let last = data.len() - 1;
    data[last] ^= 0xFF;
    fs::write(&path, &data).unwrap();

    let failures = engine.validate();
    assert!(
        !failures.is_empty(),
        "validate() should detect the CRC mismatch, got no failures"
    );
}

#[test]
fn recovery_corrupt_root_store_detected_by_validate() {
    let (dir, engine) = new_engine();
    engine.create_bag(&BAG.to_string()).unwrap();

    // Corrupt a byte in the root store body (past the 12-byte header).
    let path = root_store_file(&dir);
    let mut data = fs::read(&path).unwrap();
    let i = data.len() - 1;
    data[i] ^= 0xFF;
    fs::write(&path, &data).unwrap();

    let failures = engine.validate();
    assert!(
        !failures.is_empty(),
        "validate() should detect root-store CRC corruption, got no failures"
    );
}
