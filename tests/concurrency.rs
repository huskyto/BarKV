mod common;
use common::*;

use barkv::EngineError;


#[test]
fn concurrent_writes_distinct_keys_same_bag_all_persist() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let threads = 8;
    let per_thread = 50;

    let engine = &engine;
    std::thread::scope(|s| {
        for t in 0..threads {
            s.spawn(move || {
                for i in 0..per_thread {
                    engine
                        .set(&BAG.to_string(), &format!("t{t}-k{i}"), format!("t{t}-v{i}").as_bytes())
                        .unwrap();
                }
            });
        }
    });

    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), threads * per_thread);
    for t in 0..threads {
        for i in 0..per_thread {
            assert_eq!(
                engine.get(&BAG.to_string(), &format!("t{t}-k{i}")).unwrap(),
                format!("t{t}-v{i}").into_bytes()
            );
        }
    }
    let failures = engine.validate();
    assert!(failures.is_empty(), "validation failures: {failures:?}");
}

#[test]
fn concurrent_writes_distinct_bags() {
    let (_dir, engine) = new_engine();
    let bags = 4;
    let per_bag = 50;
    for b in 0..bags {
        engine.create_bag(&format!("bag{b}")).unwrap();
    }

    let engine = &engine;
    std::thread::scope(|s| {
        for b in 0..bags {
            s.spawn(move || {
                for i in 0..per_bag {
                    engine
                        .set(&format!("bag{b}"), &format!("k{i}"), format!("v{i}").as_bytes())
                        .unwrap();
                }
            });
        }
    });

    for b in 0..bags {
        assert_eq!(engine.len_bag(&format!("bag{b}")).unwrap(), per_bag);
    }
    assert!(engine.validate().is_empty(), "validation failures: {:?}", engine.validate());
}

#[test]
fn concurrent_get_or_set_same_key_single_winner() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    let threads = 8;

    let engine = &engine;
    let results: Vec<Vec<u8>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                s.spawn(move || {
                    engine
                        .get_or_set(&BAG.to_string(), &"k".to_string(), format!("val-{t}").as_bytes())
                        .unwrap()
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // get_or_set is atomic: the first writer wins, and every caller — including
    // those that found the key already present — must observe that one value.
    let winner = engine.get(&BAG.to_string(), &"k".to_string()).unwrap();
    for r in &results {
        assert_eq!(r, &winner, "get_or_set returned a value other than the single winner");
    }
}

#[test]
fn concurrent_create_same_bag_single_success() {
    let (_dir, engine) = new_engine();
    let threads = 8;

    let engine = &engine;
    let results: Vec<Result<(), EngineError>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|_| s.spawn(move || engine.create_bag(&"dup".to_string())))
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let oks = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(oks, 1, "exactly one create_bag should succeed");
    for r in results.iter().filter(|r| r.is_err()) {
        assert!(matches!(r, Err(EngineError::BagAlreadyExistsError(_))));
    }
}

#[test]
fn concurrent_set_and_get_same_key_no_torn_read() {
    let (_dir, engine) = new_engine_with_bag(BAG);
    // Distinct, equal-length values: a torn read (wrong offset/length, or a value
    // read while a write is mid-flight) shows up as bytes outside this set.
    let values: Vec<Vec<u8>> = vec![b"AAAA".to_vec(), b"BBBB".to_vec(), b"CCCC".to_vec()];
    engine.set(&BAG.to_string(), &"k".to_string(), &values[0]).unwrap();

    let engine = &engine;
    let values = &values;
    // Fixed iteration counts (not writer-timing) so every reader is guaranteed to
    // do real work that overlaps the writer.
    std::thread::scope(|s| {
        s.spawn(move || {
            for n in 0..5000 {
                engine
                    .set(&BAG.to_string(), &"k".to_string(), &values[n % values.len()])
                    .unwrap();
            }
        });
        for _ in 0..3 {
            s.spawn(move || {
                for _ in 0..5000 {
                    let v = engine.get(&BAG.to_string(), &"k".to_string()).unwrap();
                    assert!(values.contains(&v), "torn/unexpected read: {v:?}");
                }
            });
        }
    });
}

#[test]
fn concurrent_writes_with_rotation_stay_consistent() {
    // The riskiest concurrent path: 100-byte values push the active file past
    // MIN_LOCK_SIZE (10_000 B) repeatedly, so many writers race the file-rotation
    // (lock_active) machinery. 8 * 40 * ~133 B ≈ 42 KB → several rotations.
    let (dir, engine) = new_engine_with_bag(BAG);
    let threads = 8;
    let per_thread = 40;
    let value = vec![0xABu8; 100];

    {
        let engine = &engine;
        let value = &value;
        std::thread::scope(|s| {
            for t in 0..threads {
                s.spawn(move || {
                    for i in 0..per_thread {
                        engine
                            .set(&BAG.to_string(), &format!("t{t}-k{i:03}"), value)
                            .unwrap();
                    }
                });
            }
        });
    }

    // No key lost or duplicated across rotations.
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), threads * per_thread);
    for t in 0..threads {
        for i in 0..per_thread {
            assert_eq!(
                engine.get(&BAG.to_string(), &format!("t{t}-k{i:03}")).unwrap(),
                value
            );
        }
    }
    let failures = engine.validate();
    assert!(failures.is_empty(), "validation failures after concurrent rotation: {failures:?}");

    // And the rotated, multi-file store rebuilds correctly.
    drop(engine);
    let engine = reopen(&dir);
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), threads * per_thread);
    for t in 0..threads {
        for i in 0..per_thread {
            assert_eq!(
                engine.get(&BAG.to_string(), &format!("t{t}-k{i:03}")).unwrap(),
                value
            );
        }
    }
}

#[test]
fn concurrent_writes_survive_reopen() {
    let (dir, engine) = new_engine_with_bag(BAG);
    let threads = 4;
    let per_thread = 40;

    {
        let engine = &engine;
        std::thread::scope(|s| {
            for t in 0..threads {
                s.spawn(move || {
                    for i in 0..per_thread {
                        engine
                            .set(&BAG.to_string(), &format!("t{t}-k{i}"), format!("t{t}-v{i}").as_bytes())
                            .unwrap();
                    }
                });
            }
        });
    }
    drop(engine);

    let engine = reopen(&dir);
    assert_eq!(engine.len_bag(&BAG.to_string()).unwrap(), threads * per_thread);
    for t in 0..threads {
        for i in 0..per_thread {
            assert_eq!(
                engine.get(&BAG.to_string(), &format!("t{t}-k{i}")).unwrap(),
                format!("t{t}-v{i}").into_bytes()
            );
        }
    }
}
