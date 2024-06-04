# TinyDFS Design Documentation
This is a tiny distributed file system.

## Design Goal
### Use Case
1. Sequential reads/writes
    - Random accesses are also allowed, but with suboptimal performance.
2. Middle to large files.

The above is sufficient to support MapReduce.

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

#### Minimizing Disk Writes at Master
The crux is how much information at master needs persistence. In GFS, this is **none**, while in HDFS, the entire namespace (i.e., directory structure) is persistent. We tried to get close to the GFS approach, but finally found we couldn't achieve that without sacrificing consistency (we achieved **linearizability**).

### Workers

### Client-side driver

## Design
### How a File Is Stored
Each file is partitioned into a sequence of fixed-size blocks (128MB in HDFS). Each block is replicated multiple times (we chose 3, the same default number as GFS and HDFS). Each replica of a block is stored as a file in Linux file system.

### Create & Exists
These operation only involve the master. For `Create`, the master persists it in a local Redis DB.

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

#### How the CR Works

#### New Blocks

#### How to Remove Stale Blocks due to Failures


### Delete


### Truncate

## Implementation

## Future Work
### Support for Snapshots
### Pipelined Writes
### Better Random Write Performance
### Security

## What We Didn't Do Well