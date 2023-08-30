package bitcaskdb

import (
	"bitcaskdb/wal"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

const (
	mergeFinNameSuffix   = ".MERGEFIN"
	mergeDirSuffixName   = "-merge"
	mergeFinishedBatchID = 0
)

func (db *DB) Merge(reopenAfterDone bool) error {
	if err := db.doMerge(); err != nil {
		return err
	}

	if !reopenAfterDone {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	//先关闭当前的数据库文件
	_ = db.closeFiles()

	//替换原来的文件
	err := loadMergeFiles(db.options.DirPath)
	if err != nil {
		return err
	}

	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return err
	}

	db.index = NewIndexer(SkipList)

	if err = db.loadIndex(); err != nil {
		return err
	}

	return nil
}

func (db *DB) doMerge() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDBClosed
	}

	//无需数据时 无需merge
	if db.dataFiles.IsEmpty() {
		db.mu.Unlock()
		return nil
	}

	//保证只有一个在merge
	if atomic.LoadUint32(&db.mergeRunning) == 1 {
		db.mu.Unlock()
		return ErrMergeRunning
	}

	atomic.StoreUint32(&db.mergeRunning, 1)

	defer atomic.StoreUint32(&db.mergeRunning, 0)

	prevActiveSegId := db.dataFiles.ActiveSegmentID()

	//创建一个新的可写的active file
	if err := db.dataFiles.OpenNewActiveSegmentFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	db.mu.Unlock()

	//打开一个用来merge的数据库
	mergeDB, err := db.openMergeDB()
	if err != nil {
		return err
	}

	defer mergeDB.Close()

	now := time.Now().UnixNano()

	reader := db.dataFiles.NewReaderWithMax(prevActiveSegId)

	for {
		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		record := decodeLogRecord(chunk)

		if record.Type == LogRecordNormal && (record.Expire == 0 || record.Expire > now) {
			db.mu.RLock()
			indexPos := db.index.Get(record.Key)
			db.mu.RUnlock()
			//只有内存与内存中的pos相同,才能说明这是有效数据,不然则是旧数据
			if indexPos != nil && positionEquals(indexPos, position) {
				//合并后的所有数据都有效,所以BatchID=0
				record.BatchId = mergeFinishedBatchID
				//写入数据
				newPosition, err := mergeDB.dataFiles.Write(encodeLogRecord(record))
				if err != nil {
					return nil
				}

				//写入kv(重启时快速重建索引)
				_, err = mergeDB.kvFile.Write(encodeKvRecord(record.Key, newPosition))
				if err != nil {
					return err
				}

			}
		} else {
			continue
		}
	}

	mergeFinFile, err := mergeDB.openMergeFinishedFile()
	if err != nil {
		return err
	}

	_, err = mergeFinFile.Write(encodeMergeFinRecord(prevActiveSegId))
	if err != nil {
		return err
	}

	if err := mergeFinFile.Close(); err != nil {
		return err
	}

	return nil
}

// 打开一个用来merge的db
func (db *DB) openMergeDB() (*DB, error) {
	mergePath := mergeDirPath(db.options.DirPath)

	//如果目录已经存在,删除merge目录
	if err := os.RemoveAll(mergePath); err != nil {
		return nil, err
	}

	//复制配置项
	options := db.options

	options.Sync = false
	options.BytesPerSync = 0
	options.DirPath = mergePath
	mergeDB, err := Open(options)
	if err != nil {
		return nil, err
	}

	kvfile, err := wal.Open(wal.Options{
		DirPath:         options.DirPath,
		SegmentSize:     math.MaxInt64,
		SegmentFileExt:  kvFileNameSuffix,
		Sync:            false,
		BytesPerSync:    0,
		CacheBlockCount: 0,
	})
	if err != nil {
		return nil, err
	}

	mergeDB.kvFile = kvfile

	return mergeDB, nil

}

// 打开一个合并完成的文件(用来存放最后一个segmentid)
func (db *DB) openMergeFinishedFile() (*wal.WAL, error) {
	return wal.Open(wal.Options{
		DirPath:         db.options.DirPath,
		SegmentSize:     GB,
		SegmentFileExt:  mergeFinNameSuffix,
		Sync:            false,
		BytesPerSync:    0,
		CacheBlockCount: 0,
	})
}

func loadMergeFiles(dirPath string) error {
	mergeDirPath := mergeDirPath(dirPath)
	if _, err := os.Stat(mergeDirPath); err != nil {
		return nil
	}

	//Load结束,merge也就没有意义了,直接删除
	defer func() {
		_ = os.RemoveAll(mergeDirPath)
	}()

	// rename + move
	copyFile := func(suffix string, fileId uint32, force bool) {
		srcFile := wal.SegmentFileName(mergeDirPath, suffix, fileId)
		stat, err := os.Stat(srcFile)
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			panic(fmt.Sprintf("loadMergeFiles: failed to get src file stat %v", err))
		}
		if !force && stat.Size() == 0 {
			return
		}
		destFile := wal.SegmentFileName(dirPath, suffix, fileId)
		_ = os.Rename(srcFile, destFile)
	}

	mergeFinSegmentId, err := getMergeFinSegmentId(mergeDirPath)
	if err != nil {
		return err
	}

	for fileId := uint32(1); fileId <= mergeFinSegmentId; fileId++ {
		destFile := wal.SegmentFileName(dirPath, dataFileNameSuffix, fileId)

		//删除原文件
		if _, err = os.Stat(destFile); err == nil {
			if err = os.Remove(destFile); err != nil {
				return err
			}
		}

		copyFile(dataFileNameSuffix, fileId, false)
	}

	copyFile(mergeFinNameSuffix, 1, true)
	copyFile(kvFileNameSuffix, 1, true)

	return nil

}

func getMergeFinSegmentId(mergePath string) (wal.SegmentID, error) {
	mergeFinFile, err := os.Open(wal.SegmentFileName(mergePath, mergeFinNameSuffix, 1))
	if err != nil {
		//合并还未完成
		return 0, nil
	}

	defer func() {
		_ = mergeFinFile.Close()
	}()

	//TODO : 不依赖WAL
	//四个字节存放SegmentId
	mergeFinBuf := make([]byte, 0)
	if _, err := mergeFinFile.ReadAt(mergeFinBuf, 7); err != nil {
		return 0, err
	}

	mergeFinSegmentId := binary.LittleEndian.Uint32(mergeFinBuf)
	return mergeFinSegmentId, nil
}

/*
from  /a/b/c.txt
to    /a/b/c.txt-merge
*/
func mergeDirPath(dirPath string) string {
	dir := filepath.Dir(filepath.Clean(dirPath))
	base := filepath.Base(dirPath)
	return filepath.Join(dir, base+mergeDirSuffixName)
}

// 比较两个pos是否相等
func positionEquals(a, b *wal.ChunkPosition) bool {
	return a.SegmentId == b.SegmentId &&
		a.BlockNumber == b.BlockNumber &&
		a.ChunkOffset == b.ChunkOffset
}

func encodeMergeFinRecord(segmentId wal.SegmentID) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, segmentId)
	return buf
}
