# TinyDFS
This is a tiny distributed file system optimized for bandwidth

## Format of Configuration Files

### NameNode Endpoint
Exactly one line.
It can be `IP:port` (e.g., `162.105.131.160:443`) or `DomainName:port`(e.g., `www.stanford.edu:443`).

### DataNode Endpoints
One line per DataNode, using the same format as above.

## Scripts
```bash scripts/build.sh```
Build namenode and datanode.

```bash scripts/setup.sh```
Run namenode and multiple datanodes.

```bash scripts/test.sh```
An example script to setup and test the system.

```bash scripts/terminate.sh```
Terminate all namenode and datanodes.

## Examples
```go run examples/read/main.go --server="localhost:8000" --path="test" --offset=10 --length=10```


## Redis Setup

### Installation
Ubuntu:
```sudo apt install redis```.

Make sure the version is >= 7 (`redis-cli -v`).

### Configuration
Open config file (`sudo vim /etc/redis/redis.conf`), and modify the following:
1. `appendonly yes`
2. `appendfsync always`
3. `supervised auto`
