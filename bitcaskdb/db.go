package bitcaskdb

import (
	"bitcaskdb/utils"
	"bitcaskdb/wal"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	dataFileNameSuffix = ".SEG"
)

type DB struct {
	dataFiles    *wal.WAL // data files are a sets of segment files in WAL.
	kvFile       *wal.WAL // kv file is used to store the key and the position for fast startup.
	index        Indexer
	options      Options
	fileLock     *utils.FLock
	mu           sync.RWMutex
	closed       bool
	mergeRunning uint32 // 是否正在合并
	batchPool    sync.Pool
	watchCh      chan *Event // user consume channel for watch events
	watcher      *Watcher
}

// 存放这个数据库的大小
type Stat struct {
	// Total number of keys
	KeysNum int
	// Total disk size of database directory
	DiskSize int64
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
	err = filelock.Lock()
	if err != nil {
		return nil, err
	}

	//如果merge文件存在,加载merge文件
	if err = loadMergeFiles(options.DirPath); err != nil {
		return nil, err
	}

	db := &DB{
		index:     NewIndexer(SkipList),
		options:   options,
		fileLock:  filelock,
		batchPool: sync.Pool{New: makeBatch},
	}

	//打开数据库文件(wal)
	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return nil, err
	}

	//加载index
	if err = db.loadIndex(); err != nil {
		return nil, err
	}

	if options.WatchQueueSize > 0 {
		db.watchCh = make(chan *Event, 100)
		db.watcher = NewWatcher(options.WatchQueueSize)
		go db.watcher.sendEvent(db.watchCh)
	}

	return db, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.closeFiles(); err != nil {
		return err
	}

	if err := db.fileLock.Unlock(); err != nil {
		return err
	}

	db.fileLock.Release()
	// close watch channel
	if db.options.WatchQueueSize > 0 {
		close(db.watchCh)
	}

	db.closed = true
	return nil
}

// closeFiles close all data files and hint file
func (db *DB) closeFiles() error {
	// close wal
	if err := db.dataFiles.Close(); err != nil {
		return err
	}
	// close hint file if exists
	if db.kvFile != nil {
		if err := db.kvFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.dataFiles.Sync()
}

func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()
	diskSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("rosedb: get database directory size error: %v", err))
	}
	return &Stat{
		KeysNum:  db.index.Size(),
		DiskSize: diskSize,
	}
}

func (db *DB) Put(key []byte, value []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()

	batch.init(false, false, db).withPendingWrites()
	if err := batch.Put(key, value); err != nil {
		return err
	}
	return batch.Commit()
}

func (db *DB) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()

	batch.init(false, false, db).withPendingWrites()
	if err := batch.PutWithTTL(key, value, ttl); err != nil {
		return err
	}
	return batch.Commit()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Get(key)
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

func (db *DB) loadIndexFromWAL() error {
	mergeFinSegmentId, err := getMergeFinSegmentId(db.options.DirPath)
	if err != nil {
		return err
	}

	indexRecords := make(map[uint64][]*IndexRecord)

	now := time.Now().UnixNano()

	reader := db.dataFiles.NewReader()

	for {
		//如果小于mergeFinSegmentId则跳过,因为那部分已经被合并了
		if reader.CurrentSegmentId() <= mergeFinSegmentId {
			reader.SkipCurrentSegment()
			continue
		}
		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		record := decodeLogRecord(chunk)
		if record.Type == LogRecordBatchFinished {
			batchId, err := utils.ParseBytes(record.Key)
			if err != nil {
				return err
			}
			for _, idxRecord := range indexRecords[uint64(batchId)] {
				if idxRecord.recordType == LogRecordNormal {
					db.index.Put(idxRecord.key, idxRecord.position)
				}
				if idxRecord.recordType == LogRecordDeleted {
					db.index.Delete(idxRecord.key)
				}
			}
			delete(indexRecords, uint64(batchId))
		} else if record.Type == LogRecordNormal && record.BatchId == mergeFinishedBatchID {
			db.index.Put(record.Key, position)
		} else {
			if record.IsExpired(now) {
				continue
			}

			//放入临时切片
			indexRecords[record.BatchId] = append(indexRecords[record.BatchId],
				&IndexRecord{
					key:        record.Key,
					recordType: record.Type,
					position:   position,
				})
		}
	}

	return nil
}

func (db *DB) loadIndex() error {
	// 获取merge过的data的index
	if err := db.loadIndexFormKvFile(); err != nil {
		return err
	}
	// 获取还未merge的data的index
	if err := db.loadIndexFromWAL(); err != nil {
		return err
	}
	return nil
}
