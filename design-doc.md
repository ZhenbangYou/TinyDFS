# TinyDFS Design Documentation
This is a tiny distributed file system.

## Design Goal
### Use Case
1. Sequential reads/writes
    - Random accesses are also allowed, but with suboptimal performance.
2. Middle to large files.

The above is sufficient to support MapReduce.

### Failure Mode
1. Machine crash.
2. Network failure.

### Threat Mode
We consider everyone to be benign.

### Goals
1. Sequential read/write throughput.
2. Simplicity of both APIs and implementation.

### Non-Goals
1. Latency of any operation.
2. Throughput of operations other than read/write, e.g., create, delete.
3. Read/write coordination and transactional support.
    - This should be done by a coordination service like Zookeeper.

## APIs
Generally, the APIs are designed based on [HDFS APIs](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html), and modified according to [Rust file APIs](https://doc.rust-lang.org/std/fs/struct.File.html).
### First Step: Connect to the Distributed File System
```dfs, err := client.ConnectDistributedFileSystem("localhost:8000")```

### Step 2: Manipulate Files
#### Type A: File-Level Manipulation
1. Create a file:
```err := dfs.Create("file.txt")```
2. Check existence:
```exists := dfs.Exists("file.txt")```
3. Delete a file:
```err := dfs.Delete("file.txt")```
4. Get the size of a file:
```size, err := dfs.GetSize("file.txt")```
5. Truncate a file
```err := dfs.Truncate("file.txt", 100)```

#### Type B: Read a File
1. Create a read handle:
```readHandle := dfs.OpenForRead("file.txt")```
2. (Optionally) seek:
```readHandle.Seek(100)```
3. Read:
```data, err := readHandle.Read(100)```

#### Type C: Write a File
1. Create a write handle:
```writeHandle := dfs.OpenForWrite("file.txt")```
2. (Optionally) seek:
```writeHandle.Seek(100)```
3. Read:
```err := writeHandle.Write(data)```
4. Close the write handle:
```writeHandle.Close()```

Note that a failed client write doesn't mean none of the data is written; instead, it's possible that part of the data is written (the unit is block, which is an implementation detail hidden away from users), so users should retry after failures to avoid leaving the file inconsistent.

### Final Step: Close Connection
```defer dfs.Close()```

## High-Level Architecture
Generally, the architecture is a mixture of GFS, HDFS, as well as our own designs.

This project consists of three major parts:
1. a centralized master (in directory `namenode`).
2. workers (in directory `datanode`).
3. client-side driver (in directory `client`).

### Centralized Master
We chose this approach due to simplicity. To avoid single-point bottleneck, we made lots of effort to reduce the amount of work of the master, especially avoiding disk writes as much as possible.

A centralized master makes it much easier to achieve linearizability.

#### Minimizing Disk Writes at Master
The crux is how much information at master needs persistence. In GFS, this is **none**, while in HDFS, the entire namespace (i.e., directory structure) is persistent. We tried to get close to the GFS approach, but finally found we couldn't achieve that without sacrificing consistency (we achieved **linearizability**).

The master only persists the namespace and the version number of each block, but it doesn't persist which worker stores a certain block.

### Workers
They are the workhorses -- the master only stores metadata, while all data is stored in workers.

### Client-side driver
It provides simple APIs for users. It's not just a thin wrapper, e.g., it
1. hides away blocks,
2. reports timeout after a reasonable interval rather than block forever,
3. maintains file offsets,
4. hides away leases.

## Design
### How a File Is Stored
Each file is partitioned into a sequence of fixed-size blocks (128MB in HDFS). Each block is replicated multiple times (we chose 3, the same default number as GFS and HDFS). Each replica of a block is stored as a UNIX file.

### Create & Exists
These operation only involve the master. For `Create`, the master persists it in a local Redis DB.

### Multi-Version
Every time updating a block in workers, the original block (stored as a UNIX file) will be kept (and deleted when it's known to be safe). The multi-version design simplfies fault tolerance, but here comes an issue -- stale blocks may stay around after a failure; this can be resolved by a periodical block report from workers to the master.

### Persistent States
Master: all in Redis:
1. A hashset of all file names.
2. For each file, a hashmap from block index to its version.

Worker: blocks stored as UNIX files with name `{FileName}_{BlockIndex}_{VersionNumber}`.

### Read
1. User issue `Read` on a read handle.
2. Driver breaks the read request into multiple blocks.
3. Driver asks the master for metadata of these blocks, including endpoints of workers that store this block, and the version number of this block.
4. For each block, driver chooses a worker to fetch the block randomly (to achieve read load balance), and issue `ReadBlock` to them *in parallel*.
5. On success, driver adjusts the file offset of the read handle.

### Write

#### General Procedure
1. User issue `Write` on a write handle.
2. Driver breaks the write request into multiple blocks.
3. Driver asks the master for metadata of these blocks, including endpoints of workers that store this block, and the version number of this block. Driver also provide a non-zero number as a *lease token* for this file.
4. If there's currently no valid lease of this file or the valid lease is the same as that provided by the driver, the master accepts this lease.
5. If this write appends new blocks to the file, the master chooses workers to hold these new blocks based on the number of blocks they hold (to achieve load balance), but the master doesn't inform those workers about this.
6. On successfully obtaining the lease, driver periodically renews this lease until the write handle is closed.
7. Driver writes each block *in parallel*. For each block, all workers holding it forms a **Chain Replication (CR)**.
8. How the CR Works:
    - Driver writes the block to the first node, and the latter propagates the block to its successor, so on and so forth. The last node in the chain reports to the master. On getting an ACK from the master, the last node ACKs its predecessor, so on and so forth. The first node ACKs the driver.

    - On receiving a request from a worker with a valid lease and high version, the master updates the version and persists it in Redis.

    - On getting an ACK, a node can safely delete the block of previous version.

    - If any node in the chain doesn't get an ACK, it keeps the new block anyway; this is because it's possible that the master accepted the new version, but the ACK lost somewhere down the chain.

#### New Blocks
Version number is an unsigned integer. `0` is consider as an invalid version number. When the master finds a write appends new blocks, it picks workers and send them to the driver, and tells the driver the version number of these new blocks are `0`.

When the master receives a request from a worker with a block of version `1`, it will know this is a new block.

#### How to Prevent Multiple Writers
Lease.

When the last node in the chain reports to the master, it should attach the lease token (originated from the driver and passed through the chain).

When a write handle is closed, the driver contact the master to revoke the lease.

#### How to Remove Stale Blocks due to Failures
Block reports.

Each worker reports all of its blocks (including file name, block index, and version number) at the following time:
1. the worker starts up;
2. the worker's previous heartbeat to the master fails, and the current heartbeat succeeds;
3. periodically (every hour in HDFS).

The master replies with stale blocks in the block report request.

### Delete
The master
1. checks its in-memory states to find all blocks of the file to be deleted,
2. deletes all of them from workers,
3. updates its in-memory states and persistent states in Redis.

Even if a worker fails to delete a block, that will only lead to stale blocks, but these stale blocks won't be accessed since the correct state is maintained persistently in the master.

### Truncate
For each truncate, we need to delete some (zero or more) blocks and truncate (zero or one) block. Deleting blocks is similar to what we discussed in the `Delete` subsection above, while truncate blocks is similar to write a block (thus its version will be bumped up). For fault tolerance, we should truncate the block first (when it fails, we can report this truncate operation fails). When the previous step succeeds, we can delete blocks.

## Implementation
### How to Persist Data
We use **Redis** with `appendonly=yes` and `appendfsync=always`, so that it persists every modification instantaneously with log appending (sequential disk write).

## Future Work
### Support for Snapshots
Since this project is implemented in a multi-version way, supporting snapshots will be straightforward, except it may be a bit hard to determine when an old version can be safely garbage collected.

### Pipelined Writes
We use `net/rpc` in Go for RPC. It's easy to learn and use but it doesn't support streaming as [gRPC](https://grpc.io/docs/languages/go/basics/#server-side-streaming-rpc).
This optimization is critical to write latency.

### Better Random Write Performance
When a block of new version is created and the block is only partly updated, the unmodified part will be copied from the previous version, leading to write amplification.
This doesn't matter for sequential writes like those in MapReduce, but this is definitely prohibitive for workloads like relational databases.

ON the fast path, we can write the modified part to the previous block. This is not hard, but we haven't done this.

### Security
We assume all participants to be honest and cooperative, which is not robust enough in production.

## What We Didn't Do Well
### Unit Test
We gradually caught many bugs during development. We could have caught them faster with better unit tests.