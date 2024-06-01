package common

type BlockIdentifier struct {
	FileName   string
	BlockIndex uint
	Version    uint
}

type BlockMetadata struct {
	BlockID BlockIdentifier
	Size    uint
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
	BlockID     BlockIdentifier
	BeginOffset uint
	Length      uint
}

type ReadBlockResponse struct {
	Data []byte
}

type WriteBlockRequest struct {
	BlockID     BlockIdentifier
	BeginOffset uint

	Data []byte

	// All datanodes holding a block form a Chain Replication
	ReplicaEndpoints []string
	IndexInChain     uint

	LeaseToken uint64
}

type BlockVersionBump struct {
	BlockID          BlockIdentifier
	Size             uint
	ReplicaEndpoints []string
	LeaseToken       uint64
}

type Lease struct {
	FileName   string
	LeaseToken uint64
}
