package bitcaskdb

//此文件用来实现 索引(key - pos) 的持久化 和 索引重建

import (
	"bitcaskdb/wal"
	"encoding/binary"
	"io"
	"math"
)

const (
	kvFileNameSuffix = ".KV" //存放key和pos的文件位置
)

/*
+-----------+-------------+-------------+-----------+
| SegmentId | BlockNumber | ChunkOffset | ChunkSize |
+-----------+-------------+-------------+-----------+
	5       +     5       +      10     +     5         =25
*/
// 编码
func encodeKvRecord(key []byte, pos *wal.ChunkPosition) []byte {
	buf := make([]byte, 25)
	var idx = 0

	// SegmentId
	idx += binary.PutUvarint(buf[idx:], uint64(pos.SegmentId))
	// BlockNumber
	idx += binary.PutUvarint(buf[idx:], uint64(pos.BlockNumber))
	// ChunkOffset
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkOffset))
	// ChunkSize
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkSize))

	//因为最后一条是char*,所以不用编码key的长度
	// key
	result := make([]byte, idx+len(key))
	copy(result, buf[:idx])
	copy(result[idx:], key)
	return result
}

// 解码
func decodeKvRecord(buf []byte) ([]byte, *wal.ChunkPosition) {
	var idx = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[idx:])
	idx += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[idx:])
	idx += n
	// Key
	key := buf[idx:]

	return key, &wal.ChunkPosition{
		SegmentId:   wal.SegmentID(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}

// 从kvfile中加载key - position
func (db *DB) loadIndexFormKvFile() error {
	kvFile, err := wal.Open(wal.Options{
		DirPath:         db.options.DirPath,
		SegmentSize:     math.MaxInt64,
		SegmentFileExt:  kvFileNameSuffix,
		CacheBlockCount: 10,
	})

	if err != nil {
		return err
	}

	defer func() {
		_ = kvFile.Close()
	}()

	reader := kvFile.NewReader()
	for {
		chunk, _, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		key, position := decodeKvRecord(chunk)
		db.index.Put(key, position)
	}

	return nil
}
