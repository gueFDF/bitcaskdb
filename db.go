package bitcaskdb

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Bitcaskdb struct {
	indexes map[string]int64 //内存中的索引信息
	dbFile  *DBFile          //数据文件
	dirPath string           //数据目录
	mu      sync.Mutex
}

func Open(dirPath string) (*Bitcaskdb, error) {

	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 加载数据文件
	dbFile, err := NewDBfile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &Bitcaskdb{
		dbFile:  dbFile,
		indexes: make(map[string]int64),
		dirPath: dirPath,
	}

	db.loadIndexesFromFile()
	return db, nil
}

// put entries
func (db *Bitcaskdb) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.dbFile.Offset
	entry := NewEntry(key, value, PUT)
	err = db.dbFile.WriteEntry(entry)

	//write to memory
	db.indexes[string(key)] = offset
	return
}

// 是否存在
func (db *Bitcaskdb) exist(key []byte) (int64, error) {
	offset, ok := db.indexes[string(key)]
	if !ok {
		return 0, ErrKeyNotFound
	}

	return offset, nil
}

// Get
func (db *Bitcaskdb) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	offset, err := db.exist(key)
	if err == ErrKeyNotFound {
		return
	}
	var e *Entry
	e, err = db.dbFile.ReadEntry(offset)
	if e != nil {
		val = e.Value
	}
	return
}

func (db *Bitcaskdb) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	_, err = db.exist(key)
	if err == ErrKeyNotFound {
		err = nil
		return
	}

	e := NewEntry(key, nil, DEl)
	err = db.dbFile.WriteEntry(e)
	if err != nil {
		return
	}
	//删除内存中的key
	delete(db.indexes, string(key))
	return
}

// 从文档中加载索引
func (db *Bitcaskdb) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}

	var offset int64
	for {
		e, err := db.dbFile.ReadEntry(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}

		//设置索引状态
		db.indexes[string(e.Key)] = offset

		if e.Mark == DEl {
			delete(db.indexes, string(e.Key))
		}

		offset += e.GetSize()
	}
}

func (db *Bitcaskdb) Merge() error {
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	for {
		e, err := db.dbFile.ReadEntry(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	if len(validEntries) > 0 {
		mergeDBFile, err := NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDBFile.File.Name())
		db.mu.Lock()
		defer db.mu.Unlock()
		// 重新写入有效的 entry
		for _, entry := range validEntries {
			writeOff := mergeDBFile.Offset
			err := mergeDBFile.WriteEntry(entry)
			if err != nil {
				return err
			}

			// 更新索引
			db.indexes[string(entry.Key)] = writeOff
		}
		// 获取文件名
		dbFileName := db.dbFile.File.Name()
		// 关闭文件
		db.dbFile.File.Close()
		// 删除旧的数据文件
		os.Remove(dbFileName)
		mergeDBFile.File.Close()
		// 获取文件名
		mergeDBFileName := mergeDBFile.File.Name()
		// 临时文件变更为新的数据文件
		os.Rename(mergeDBFileName, filepath.Join(db.dirPath, FileName))

		dbFile, err := NewDBfile(db.dirPath)
		if err != nil {
			return err
		}

		db.dbFile = dbFile
	}
	return nil
}

func (db *Bitcaskdb) Close() error {
	if db.dbFile != nil {
		return ErrInvaliDAFile
	}

	return db.dbFile.File.Close()
}
