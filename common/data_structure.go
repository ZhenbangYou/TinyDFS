package common

type FileMetadata struct {
	Size uint
}

type FileHandle struct {
	Path string
	MetaData FileMetadata
}

type OpenArgs struct {
	Path     string
	ReadOnly bool
}