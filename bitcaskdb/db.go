package bitcaskdb

import (
	"bitcaskdb/utils"
	"bitcaskdb/wal"
	"errors"
	"os"
	"sync"
)

const (
	dataFileNameSuffix = ".SEG"

	mergeFinNameSuffix = ".MERGEFIN"
)

type DB struct {
	dataFiles    *wal.WAL // data files are a sets of segment files in WAL.
	kvFile       *wal.WAL // kv file is used to store the key and the position for fast startup.
	index        Indexer
	options      Options
	fileLock     *utils.FLock
	mu           sync.RWMutex
	closed       bool
	mergeRunning uint32 // indicate if the database is merging
	batchPool    sync.Pool
	watchCh      chan *Event // user consume channel for watch events
	watcher      *Watcher
}

func Open(options Options) (*DB, error) {
	var err error
	//检查配置
	if err = checkOptions(options); err != nil {
		return nil, err
	}

	// 如果目录不存在,创建目录
	if _, err = os.Stat(options.DirPath); err != nil {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 创建文件锁,防止一个目录被多个数据库使用
	filelock, err := utils.NewFLock(options.DirPath)
	if err != nil {
		return nil, err
	}
	filelock.Lock()
	if err != nil {
		return nil, err
	}

	//TODO  load merge files if exists
	db := &DB{
		index:    NewIndexer(SkipList),
		options:  options,
		fileLock: filelock,
		//	batchPool: sync.Pool{New: makeBatch},
	}

	//打开数据库文件(wal)
	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return nil, err
	}
	return nil, nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.SegmentSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}

// 打开Wal文件
func (db *DB) openWalFiles() (*wal.WAL, error) {
	walFiles, err := wal.Open(wal.Options{
		DirPath:         db.options.DirPath,
		SegmentSize:     db.options.SegmentSize,
		SegmentFileExt:  dataFileNameSuffix,
		CacheBlockCount: db.options.CacheBlockCount,
		Sync:            db.options.Sync,
		BytesPerSync:    db.options.BytesPerSync,
	})

	if err != nil {
		return nil, err
	}

	return walFiles, nil
}
