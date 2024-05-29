package common

type FileAttributes struct {
	Size uint
}

type BlockMetadata struct {
	FileName   string
	BlockIndex uint
	Version    uint
	Size       uint
}

type BlockReport struct {
	Endpoint      string
	BlockMetadata []BlockMetadata
}

type Heartbeat struct {
	Endpoint string
}

// Request to read a file with the specified name and block range [BeginBlock, EndBlock)
type GetBlockLocationsRequest struct {
	FileName   string
	BeginBlock uint
	EndBlock   uint
	LeaseToken uint64 // Client endpoint applying for a lease should supply a non-zero token
}

type GetBlockLocationsResponse struct {
	BlockInfoList []BlockInfo
}

type BlockInfo struct {
	Version           uint
	DataNodeEndpoints []string
}

// Request to read a block with the specified name and byte range [BeginOffset, EndOffset)
type ReadBlockRequest struct {
	FileName    string
	BlockIndex  uint
	Version     uint
	BeginOffset uint
	Length      uint
}

type ReadBlockResponse struct {
	Data []byte
}

type WriteBlockRequest struct {
	FileName    string
	BlockIndex  uint
	Version     uint
	BeginOffset uint

	Data []byte

	// All datanodes holding a block form a Chain Replication
	ReplicaEndpoints []string
	IndexInChain     uint

	LeaseToken uint64
}

// requests a datanode to create the replica of an existing block
type CreateReplicationRequest struct {
	FileName   string
	BlockIndex uint
	Version    uint

	// All datanodes to replicate the block to
	ReplicaEndpoints []string
}

// the datanode receives the request and stores the block replica
type ReplicateBlockRequest struct {
	FileName   string
	BlockIndex uint
	Version    uint

	Data []byte

	// All datanodes to replicate the block to
	ReplicaEndpoints []string
	IndexInChain     uint
}

type BlockVersionBump struct {
	FileName         string
	BlockIndex       uint
	Version          uint
	Size             uint
	ReplicaEndpoints []string
	LeaseToken       uint64
}

type LeaseRenewalRequest struct {
	FileName   string
	LeaseToken uint64
}
