package common

type FileAttributes struct {
	Size uint
}

type BlockMetadata struct {
	Version uint
}

type BlockReport struct {
	Endpoint      string
	BlockMetadata map[string]BlockMetadata
}

type Heartbeat struct {
	Endpoint string
}
