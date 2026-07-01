# BarKV

[![CI](https://github.com/huskyto/BarKV/actions/workflows/ci.yml/badge.svg)](https://github.com/huskyto/BarKV/actions/workflows/ci.yml)

A log-structured key-value store written in Rust.

Keys are grouped into named *bags* and written to append-only files. Writes go to the end of the active file; an in-memory index maps each key to the offset of its latest record, so reads are a single seek. Old records are reclaimed later by compaction rather than overwritten in place.

The store handle is cheap to clone and safe to share across threads.

## Status

Early but functional. The architecture and public API are complete and tested. The main thing missing for serious use is durability: there is no `fsync` in the write path yet, so an acknowledged write can still be lost on power loss or a kernel crash.

In short: fine to build against and experiment with, not yet something to trust with data you can't lose.

## Installation

Not published to crates.io yet. Depend on it straight from Git:

```sh
cargo add --git https://github.com/huskyto/BarKV.git barkv
```

or in `Cargo.toml`:

```toml
[dependencies]
barkv = { git = "https://github.com/huskyto/BarKV.git" }
```

## Example

```rust
use barkv::BarKV;

let store = BarKV::open_or_create("./data")?;
let bag = "users".to_string();

store.create_bag(&bag)?;
store.set(&bag, &"alice".to_string(), b"hello")?;

assert_eq!(store.get(&bag, &"alice".to_string())?, b"hello");

store.close()?;
```

## Concepts

**Bags** group related keys. A bag must be created before use, lives in its own set of files, and can be dropped as a unit. Keys are unique within a bag.

**Compaction** is the housekeeping pass that drops superseded, deleted, and expired records and rewrites each bag's live data into a fresh file. Execution is manual. Active files seal once they grow past a configurable limit and are not rewritten (read-only) again after a full compaction.

**Full Compaction** widens that pass to a bag's entire history instead of just its active file. Because it can see every record, it removes all redundancy, including the safeguard tombstones partial compaction has to leave behind for keys that might live in earlier files.

**TTL.** Entries can be given a time-to-live. An expired entry reads as if it were deleted and is removed at the next compaction. `persist` rewrites an entry without its expiry.

**Integrity.** Every record carries a CRC. `validate()` walks the whole store and reports any corruption it finds without modifying anything. On startup a truncated trailing record (the typical result of a crash mid-write) is detected and rolled back to the last good offset.

## API overview

| Area | Methods |
|------|---------|
| Lifecycle | `open`, `create`, `open_or_create`, `close` |
| Values | `get`, `set`, `delete`, `exists` |
| Bags | `create_bag`, `drop_bag`, `list_bags`, `len_bag`, `list_keys` |
| Batch | `get_many`, `set_many`, `delete_many` |
| Atomic | `get_or_set`, `get_and_delete`, `update_if_different` |
| TTL | `set_with_expiry`, `ttl`, `persist` |
| Maintenance | `compact_active`, `full_compaction`, `validate` |

For more information, see the rustdoc.

## Limitations

- **Compaction is manual.** Dead and superseded records hold disk space until a compaction pass is run.

## Design

`DESIGN.md` is the reference for the on-disk format: the store archive file, the per-bag file chains, seal helper files, the compaction and sealing rules, and the concurrency model.

It encodes the design decisions for BarKV.

## Building

```sh
cargo build
cargo test
```

Requires a Rust toolchain with edition 2024 support (1.85+).

## License

Licensed under the [MIT License](LICENSE).
