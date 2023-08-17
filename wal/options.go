package wal

import "os"

type Options struct {
	DirPath         string // path to the file
	SegmentSize     int64  // max size of the file
	SegmentFileExt  string // extension of the file
	CacheBlockCount uint32 // 缓存支持的块数
	Sync            bool   // 是否同步(是否持久化)
	BytesPerSync    uint32 // 用 fsync 之前要写入的字节数
}

const (
	B  = 1
	KB = 1024 * B
	MB = KB * 1024
	GB = 1024 * MB
)

// 默认配置
var DefaultOptions = Options{
	DirPath:         os.TempDir(),
	SegmentSize:     GB,
	SegmentFileExt:  ".SEG",
	CacheBlockCount: 10, // default 10
	Sync:            false,
	BytesPerSync:    0,
}
