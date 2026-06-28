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

fn boundary_after(n: usize, value: &[u8]) -> u64 {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = BarKV::create(dir.path().to_str().unwrap()).unwrap();
    engine.create_bag(&BAG.to_string()).unwrap();
    for i in 0..n {
        engine.set(&BAG.to_string(), &format!("k{i}"), value).unwrap();
    }
    drop(engine);
    fs::metadata(dir.path().join(format!("{BAG}-0.bkv"))).unwrap().len()
}

#[test]
fn recovery_truncated_value_region_truncates_file_to_boundary() {
    let value = vec![b'x'; 100];
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..5 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &value).unwrap();
    }
    drop(engine);

    // 5 bytes off a 100-byte value: header of the last entry stays intact.
    let path = bag_file(&dir);
    let data = fs::read(&path).unwrap();
    fs::write(&path, &data[..data.len() - 5]).unwrap();

    let engine = reopen(&dir);
    for i in 0..4 {
        assert_eq!(
            engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(),
            value
        );
    }
    drop(engine);

    let after = fs::metadata(&path).unwrap().len();
    assert_eq!(
        after,
        boundary_after(4, &value),
        "file must be truncated to the end of the last fully-written entry"
    );
}

#[test]
fn recovery_truncated_header_region_truncates_file_to_boundary() {
    let value = b"v".to_vec();
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..5 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &value).unwrap();
    }
    drop(engine);

    // Tiny values mean cutting 5 bytes lands inside the last entry's header.
    let path = bag_file(&dir);
    let data = fs::read(&path).unwrap();
    fs::write(&path, &data[..data.len() - 5]).unwrap();

    let engine = reopen(&dir);
    drop(engine);

    let after = fs::metadata(&path).unwrap().len();
    assert_eq!(
        after,
        boundary_after(4, &value),
        "file must be truncated to the end of the last fully-written entry"
    );
}

#[test]
fn recovery_write_after_recovery_persists() {
    let value = vec![b'x'; 100];
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..5 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &value).unwrap();
    }
    drop(engine);

    let path = bag_file(&dir);
    let data = fs::read(&path).unwrap();
    fs::write(&path, &data[..data.len() - 5]).unwrap(); // value-region cut

    // Recover, then write a brand-new entry.
    let engine = reopen(&dir);
    engine.set(&BAG.to_string(), &"fresh".to_string(), b"hello").unwrap();
    drop(engine);

    // Everything committed before the crash, plus the post-recovery write, survives.
    let engine = reopen(&dir);
    for i in 0..4 {
        assert_eq!(
            engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(),
            value
        );
    }
    assert_eq!(
        engine.get(&BAG.to_string(), &"fresh".to_string()).unwrap(),
        b"hello".to_vec(),
        "a write made after recovery must survive the next reopen"
    );
}

#[test]
fn recovery_store_is_clean_after_reopen() {
    let value = vec![b'x'; 100];
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..5 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &value).unwrap();
    }
    drop(engine);

    let path = bag_file(&dir);
    let data = fs::read(&path).unwrap();
    fs::write(&path, &data[..data.len() - 5]).unwrap(); // value-region cut

    let engine = reopen(&dir);
    let failures = engine.validate();
    assert!(
        failures.is_empty(),
        "store should be clean after recovering the partial tail, got: {failures:?}"
    );
}

#[test]
fn recovery_cut_at_entry_boundary_keeps_all_entries() {
    let value = vec![b'x'; 100];
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..5 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &value).unwrap();
    }
    drop(engine);

    // Truncate to the exact end of entry k3 — k4 disappears cleanly.
    let path = bag_file(&dir);
    let boundary = boundary_after(4, &value);
    let data = fs::read(&path).unwrap();
    fs::write(&path, &data[..boundary as usize]).unwrap();

    let engine = reopen(&dir);
    for i in 0..4 {
        assert_eq!(
            engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(),
            value
        );
    }
    assert!(
        !engine.exists(&BAG.to_string(), &"k4".to_string()).unwrap(),
        "k4 was cut off at the boundary and must not come back"
    );
    let failures = engine.validate();
    assert!(failures.is_empty(), "clean boundary cut must validate clean, got: {failures:?}");
}

#[test]
fn recovery_truncated_active_keeps_sealed_files_intact() {
    let (dir, engine) = new_engine_with_bag(BAG);
    fill_to_rotations(&engine, BAG, 1); // forces at least one rotation -> sealed file(s)
    drop(engine);

    // Identify the active (highest-id) .bkv file; snapshot every other store file.
    let mut bkv: Vec<PathBuf> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "bkv"))
        .collect();
    bkv.sort();
    let active = bkv.last().unwrap().clone();
    assert!(bkv.len() >= 2, "fill_to_rotations should have produced a sealed file");

    let untouched: Vec<(PathBuf, Vec<u8>)> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p != &active && p.is_file())
        .map(|p| {
            let bytes = fs::read(&p).unwrap();
            (p, bytes)
        })
        .collect();

    // Tear the tail of the active file (100-byte values -> value-region cut).
    let data = fs::read(&active).unwrap();
    fs::write(&active, &data[..data.len() - 5]).unwrap();

    let engine = reopen(&dir);
    // An early key, which lives in a sealed file, is still readable.
    assert_eq!(
        engine.get(&BAG.to_string(), &"key-00000".to_string()).unwrap(),
        vec![0xABu8; 100]
    );
    drop(engine);

    for (path, before) in untouched {
        let after = fs::read(&path).unwrap();
        assert_eq!(
            before, after,
            "sealed/non-active file {path:?} must be byte-identical after recovery"
        );
    }
}

#[test]
fn recovery_midfile_corruption_detected_by_validate() {
    let value = vec![b'x'; 100];
    let (dir, engine) = new_engine_with_bag(BAG);
    for i in 0..5 {
        engine.set(&BAG.to_string(), &format!("k{i}"), &value).unwrap();
    }

    // Flip a byte well inside the file (in entry k1's region), not at the tail.
    let path = bag_file(&dir);
    let mut data = fs::read(&path).unwrap();
    let mid = boundary_after(1, &value) as usize + 40; // inside k1
    data[mid] ^= 0xFF;
    fs::write(&path, &data).unwrap();

    let failures = engine.validate();
    assert!(
        !failures.is_empty(),
        "validate() should detect mid-file CRC corruption, got no failures"
    );
}

#[test]
fn recovery_midfile_corruption_skips_entry_and_keeps_rest() {
    // Distinct value lengths per entry: a wrong skip size then lands off any entry
    // boundary, so replay can only stay aligned if it advances by the *corrupt
    // entry's own* size, read from the right offset.
    let values: Vec<Vec<u8>> = (0..5).map(|i| vec![b'x'; 50 + i * 20]).collect();
    let (dir, engine) = new_engine_with_bag(BAG);
    for (i, value) in values.iter().enumerate() {
        engine.set(&BAG.to_string(), &format!("k{i}"), value).unwrap();
    }
    drop(engine);

    // Corrupt a byte inside k1's value (header/length intact, so its size is still
    // readable and the entry is skippable rather than truncating the tail).
    let path = bag_file(&dir);
    let mut data = fs::read(&path).unwrap();
    let k1_start = boundary_after(1, &values[0]) as usize; // end of k0 == start of k1
    data[k1_start + 30] ^= 0xFF; // 25-byte header + 2-byte key + into the value
    fs::write(&path, &data).unwrap();

    // open() must succeed despite the mid-file corruption.
    let engine = reopen(&dir);

    // The corrupt entry is dropped...
    assert!(
        !engine.exists(&BAG.to_string(), &"k1".to_string()).unwrap(),
        "the corrupt entry must not be replayed"
    );
    // ...but every entry before and after it is intact, with its own value.
    for i in [0, 2, 3, 4] {
        assert_eq!(
            engine.get(&BAG.to_string(), &format!("k{i}")).unwrap(),
            values[i],
            "entries on either side of the corrupt one must survive intact"
        );
    }
}
