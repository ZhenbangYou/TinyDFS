# TinyDFS
This is a tiny distributed file system optimized for bandwidth

## Format of Configuration Files

### NameNode Endpoint
Exactly one line.
It can be `IP:port` (e.g., `162.105.131.160:443`) or `DomainName:port`(e.g., `:www.stanford.edu:443`).

### DataNode Endpoints
One line per DataNode, using the same format as above.

## Scripts
```bash scripts/build.sh```
Build namenode and datanode.
```bash scripts/setup.sh```
Run namenode and multiple datanodes.
```bash scripts/terminate.sh```
Terminate all namenode and datanodes.