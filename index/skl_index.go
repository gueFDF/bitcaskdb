package index

import (
	"bitcaskdb/ds/skiplist"
	"bitcaskdb/wal"
	"sync"
)

type Skl struct {
	skl  *skiplist.SkipList
	lock *sync.RWMutex
}

func newSkl() *Skl {
	return &Skl{
		skl:  skiplist.NewSkipList(),
		lock: new(sync.RWMutex),
	}
}

func (skl *Skl) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	skl.lock.Lock()
	defer skl.lock.Unlock()

	skl.skl.Put(key, position)

	return nil
}

func (skl *Skl) Get(key []byte) *wal.ChunkPosition {
	skl.lock.RLock()
	defer skl.lock.RUnlock()
	value := skl.skl.Get(key)
	if value != nil {
		return value.Value().(*wal.ChunkPosition)
	}
	return nil

}

func (skl *Skl) Delete(key []byte) (*wal.ChunkPosition, bool) {
	skl.lock.Lock()
	defer skl.lock.Unlock()

	value := skl.skl.Remove(key)
	if value != nil {
		return value.Value().(*wal.ChunkPosition), true
	}

	return nil, false
}

func (skl *Skl) Size() int {
	skl.lock.RLock()
	len := skl.skl.Len
	skl.lock.RUnlock()
	return len
}
