package bitcaskdb

import "os"

type Options struct {
	DirPath         string // path to the file
	SegmentSize     int64  // max size of the file
	CacheBlockCount uint32 // 缓存支持的块数
	Sync            bool   //是否每次写入进行sync
	BytesPerSync    uint32 //强制刷新的上限
	WatchQueueSize  uint64 //event队列的大小
}

// 批处理配置
type BatchOptions struct {
	Sync     bool
	ReadOnly bool
}

// 迭代器配置
type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:         tempDBDir(),
	SegmentSize:     1 * GB,
	CacheBlockCount: 0,
	Sync:            false,
	BytesPerSync:    0,
	WatchQueueSize:  0,
}

var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

func tempDBDir() string {
	dir, _ := os.MkdirTemp("", "rosedb-temp")
	return dir
}
