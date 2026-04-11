
# BarKV

## What is BarKV

BarKV aims to be a solid implementation of a Key-Value Pair Store. Its design goal is to be easy to use and safe.

BarKV takes inspiration from Bitcask, as a log-structured key-value store.

## Design Decisions

### Bags/Namespaces

BarKV uses the concept of Bags to group sets of key-values.

Besides adding structure, Bags can be manipulated as a whole.

Before being able to use a bag, it needs to be created.

Each bag exists in its own group of files.

On deletion, the in-memory entry of the bag is removed, and the on-disk files are deleted.

### Deletion

On deletion of an entry, the in-memory entry is removed, and a tombstone entry is added to the on-disk representation.

During compaction, on-disk entries with a tombstone will be permanently deleted.

Tombstone entries have an empty value, and have the **deleted** flag set.

### Error Recovery

On truncated files, the store will regenerate all the valid entries present in the log.

Will detect corruption via CRC check, but will not fix data.

### TTL

Entries can have an optional TTL.

Entries with expired TTL will return an error as if they had been deleted if the user tries to query them, and they will be removed during compaction.

The On Disk Entry encodes the timestamp at which the entry will expire in the 'expiry' field. The TTL flag has to be turned on.

Entries with TTL can be persisted. This will create a new On Disk Entry without the TTL information.

### Store Archive File

Root of the store.
It contains references to the root file of each bag.

### Compaction

The compaction run is a housekeeping task that will remove redundant and dead entries from the store.

- Overwritten entries will be removed.
- Deleted entry chains will be completely removed.
- Expired TTL entries will be removed.

The compacted data will be written to a new file, which will then replace the currently active store file.

Only active store files will be compacted. Previous files are considered archived and sealed, and compaction will run on them before they are sealed; once sealed they will not be touched again.

### File Sealing

Once an active file grows too large (soft limit can be configured. Default is 10 MB), it will be **sealed** as follows:

1. No new entries will be added to the file.
2. The file will be compacted.
3. A helper file with a **.seal** extension will be created. Description on next section.
4. A new **active** file will be created.

### Seal Helper File

The helper file is created when a file is **sealed**, and will have the name of the original file with a **.seal** extension.

It includes:

- CRC for integrity check.
- Key-Offset-Size entries for faster In Memory Index rebuild.
- Path of the next file.

### In Memory Index Rebuild

On store startup:

1. Read Store Archive File.
2. Parse Bags information.
3. Create each bag.
4. Read the chain of store files for each bag. (use Seal Helper files for archived store files)
5. Apply logs in order to rebuild In Memory Map.

### Concurrency Model

// TODO

### Atomicity

// TODO

## Data Model

### Store Archive File

Header size: 12 bytes.

- magic_bytes: [u8; 5]
- version: [u8; 3]
- crc: [u8; 4]
- bag_roots: [BagRoot]

**crc**: Covers bag_roots.

#### BagRoot

- key_size: u16,
- path_size: u16,
- name: [u8],
- path: [u8]

### In Memory Archive

Very simple high level container.

- bags: Map<EntryKey, Bag>

### In Memory Bag

- key: EntryKey
- entries: Map<EntryKey, InMemoryEntry>
- root_path: Path
- active_path: Path
- file_handle: File

### In Memory Entry

- key: EntryKey
- bag: EntryKey
- file: Path
- offset: u64
- entry_size: u64

**entry_size**: cached size of the full On Disk entry to allow single read retrieval.

### On Disk Entry

Header size: 25 bytes.
Optional header size: 8 bytes;

- crc: [u8; 4]
- timestamp: u64
- flags: u8
- key_size: u32
- val_size: u64
- expiry: u64  (Optional)
- key: [u8]
- value: [u8]

|Entry|Size|Note|
|-----|----|----|
|crc  |   4|-   |
|timestamp|8|-  |
|flags|   1|-   |
|key_size|4|Big Endian|
|val_size|8|Big Endian|
|expiry|8|Optional|
|key  |-   |Serialized|
|value|-   |Binary|
||||
|Total(fixed)|25|-|

|Postion|Flag|Note|
|-------|----|----|
|0-5|-|Reserved|
|6|TTL|-|
|7|Deleted|-|

### Store Files

- crc: [u8; 4]
- flags: u8
- entries: [OnDiskEntry]

#### Flags

|Postion|Flag|Note|
|-------|----|----|
|0-5|-|Reserved|
|6|Sealed|-|
|7|Deleted|Not currently used|

### Seal Helper Files

- crc: [u8; 4]
- nf_size: u16
- next_file: [u8]
- entries: [ShortEntry]

**next_file**: Path to next file.

#### ShortEntry

- offset: u64
- entry_size: u64
- key_size: u32
- key: [u8]

## Operations

### Values

- get(bag, key)
- set(bag, key, value)
- delete(bag, key)
- exists(bag, key)
- list_keys(bag)
- list_entries(bag)

### Bags

- create_bag(bag)
- drop_bag(bag)
- count_bag(bag)
- list_bags()


### Lifecycle

- open(path)
- close()
- sync()
- compact()

### Atomic

- get_or_set(bag, key, value)
- update_if_different(bag, key, value)
- get_and_delete(bag, key)

### Batch

- get_many(bag, keys)
- set_many(bag, kv_pairs)
- delete_many(bag, keys)

### TTL

- set_with_expiry(bag, key, value, ttl)
- ttl(bag, key)
- persist(bag, key)

### State

- stats()
- validate()

## Roadmap

### TCP Server

Easy hosting of BarKV via an executable with included TCP server.

#### Multithreaded

Should be able to handle multiple operation requests at once without running into trouble.

#### Distributed

Will allow for distributed instances with a negotiated source of truth.

#### Data Recovery

Use Reed-Solomon codes to repair corrupted entries, instead of only detecting them.

### Scripting/Batching

Basic scripting to allow for multiple operations to be applied with a single library call.

### Bash commands

Run api operations via bash calls.

### Types

Use flags to store types values, instead of all being [u8].

### Full compaction

Will check all the store, including sealed files and compact everything into a new set of files.

## Non-Goals

- Transactions.
- Secondary Indexes.
- Store-level replication.
