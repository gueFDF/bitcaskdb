package index

import "bitcaskdb/wal"

type Indexer interface {
	Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition
	Get(key []byte) *wal.ChunkPosition
	Delete(key []byte) (*wal.ChunkPosition, bool)
	Size() int
}

type IndexerType = byte

const (
	SkipList IndexerType = iota
)

func NewIndexer(indexType IndexerType) Indexer {
	switch indexType {
	case SkipList:
		return newSkl()
	default:
		panic("unexpected index type")
	}
}
