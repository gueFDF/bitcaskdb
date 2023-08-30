package bitcaskdb

import (
	"bitcaskdb/utils"
	"bitcaskdb/wal"
	"fmt"
	"sync"
	"time"
)

type Batch struct {
	db            *DB
	pendingWrites map[string]*LogRecord // save the data to be written
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool // 是否已经提交
	rollbacked    bool // 是否已经回滚
	batchId       *utils.Snowflake
}

// 为用户提供使用,做批处理
func (db *DB) NewBatch(options BatchOptions) *Batch {
	batch := &Batch{
		db:         db,
		options:    options,
		committed:  false,
		rollbacked: false,
	}

	if !options.ReadOnly {
		batch.pendingWrites = make(map[string]*LogRecord)
		node, err := utils.NewSonwflacke(1)

		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchId = node
	}
	batch.lock() //保证原子
	return batch
}

// batch的创建函数(内部使用)
func makefunc() interface{} {
	node, err := utils.NewSonwflacke(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}

	return &Batch{
		options: DefaultBatchOptions,
		batchId: node,
	}
}

// 初始化
func (b *Batch) init(rdonly, sync bool, db *DB) *Batch {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.db = db
	b.lock()
	return b
}

// 追加写
func (b *Batch) withPendingWrites() *Batch {
	b.pendingWrites = make(map[string]*LogRecord)
	return b
}

// 重置(为了重复使用)
func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = nil
	b.committed = false
	b.rollbacked = false
}

func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()

	b.pendingWrites[string(key)] = &LogRecord{
		Key:    key,
		Value:  value,
		Type:   LogRecordNormal,
		Expire: 0,
	}
	b.mu.Unlock()

	return nil
}

// 带超时时间
func (b *Batch) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// write to pendingWrites
	b.pendingWrites[string(key)] = &LogRecord{
		Key:    key,
		Value:  value,
		Type:   LogRecordNormal,
		Expire: time.Now().Add(ttl).UnixNano(),
	}
	b.mu.Unlock()

	return nil
}

func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, ErrDBClosed
	}

	now := time.Now().UnixNano()

	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			if record.Type != LogRecordDeleted || record.IsExpired(now) {
				b.mu.RUnlock()
				return nil, ErrKeyNotFound
			}
			b.mu.RUnlock()
			return record.Value, nil
		}
		b.mu.RUnlock()
	}
	// 从data file中获取

	chunkPosition := b.db.index.Get(key)
	if chunkPosition == nil {
		return nil, ErrKeyNotFound
	}

	chunk, err := b.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}

	record := decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		panic("Deleted data cannot exist in the index")
	}
	if record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}

// 删除
func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	if position := b.db.index.Get(key); position != nil {
		// write to pendingWrites if the key exists
		b.pendingWrites[string(key)] = &LogRecord{
			Key:  key,
			Type: LogRecordDeleted,
		}
	} else {
		//如果磁盘中不存在,说明该data还未commit ,直接在内存中删除即可
		delete(b.pendingWrites, string(key))
	}
	b.mu.Unlock()

	return nil
}

// 检查key-value是否在内存中
func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed {
		return false, ErrDBClosed
	}

	now := time.Now().UnixNano()

	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			b.mu.RUnlock()
			//已经被删除或者过期
			return record.Type != LogRecordDeleted && !record.IsExpired(now), nil
		}
		b.mu.RUnlock()
	}

	//如果index不存在,说明不存在
	position := b.db.index.Get(key)
	if position == nil {
		return false, nil
	}

	//读取datafile,检查是否过期或者被删除
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return false, err
	}

	record := decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return false, nil
	}
	return true, nil
}

// 设置超时时间
func (b *Batch) SetExpire(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	//还未提交
	if record := b.pendingWrites[string(key)]; record != nil {
		record.Expire = time.Now().Add(ttl).UnixNano()
	} else {
		position := b.db.index.Get(key)
		if position == nil {
			return ErrKeyNotFound
		}
		chunk, err := b.db.dataFiles.Read(position)
		if err != nil {
			return err
		}

		now := time.Now()
		record := decodeLogRecord(chunk)

		if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			b.db.index.Delete(key)
			return ErrKeyNotFound
		}

		record.Expire = now.Add(ttl).UnixNano()
		b.pendingWrites[string(key)] = record
	}
	return nil
}

// 获取某条消息的超时时间(-1表示该条消息无超时时常)
func (b *Batch) TTL(key []byte) (time.Duration, error) {
	if len(key) == 0 {
		return -1, ErrKeyIsEmpty
	}
	if b.db.closed {
		return -1, ErrDBClosed
	}
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.pendingWrites != nil {
		if record := b.pendingWrites[string(key)]; record != nil {
			if record.Expire == 0 {
				return -1, nil
			}
			if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
				return -1, ErrKeyNotFound
			}
			return time.Duration(record.Expire - now.UnixNano()), nil
		}
	}

	//在datafile中寻找
	position := b.db.index.Get(key)
	if position == nil {
		return -1, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return -1, err
	}
	record := decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		return -1, ErrKeyNotFound
	}
	//已经超时
	if record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(key)
		return -1, ErrKeyNotFound
	}
	if record.Expire > 0 {
		return time.Duration(record.Expire - now.UnixNano()), nil
	}
	return -1, nil
}

// 提交
func (b *Batch) Commit() error {
	defer b.unlock() //提交完成后,解锁DB
	if b.db.closed {
		return ErrDBClosed
	}

	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	//已经提交
	if b.committed {
		return ErrBatchCommitted
	}

	//已经回滚
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	batchId := b.batchId.Getid()
	positions := make(map[string]*wal.ChunkPosition)
	now := time.Now().UnixNano()

	for _, record := range b.pendingWrites {
		record.BatchId = uint64(batchId)
		encRecord := encodeLogRecord(record)
		pos, err := b.db.dataFiles.Write(encRecord)
		if err != nil {
			return err
		}
		positions[string(record.Key)] = pos
	}

	// 追加一条,batchend的日志
	endRecord := encodeLogRecord(&LogRecord{
		Key:  batchId.ToBytes(),
		Type: LogRecordBatchFinished,
	})
	if _, err := b.db.dataFiles.Write(endRecord); err != nil {
		return err
	}

	//如果db.options.Sync==true ,此处就没必要sync
	if b.options.Sync && !b.db.options.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}

	// write index
	for key, record := range b.pendingWrites {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			b.db.index.Delete(record.Key)
		} else {
			b.db.index.Put(record.Key, positions[key])
		}

		if b.db.options.WatchQueueSize > 0 {
			e := &Event{Key: record.Key, Value: record.Value, BatchId: record.BatchId}
			if record.Type == LogRecordDeleted {
				e.Action = WatchActionDelete
			} else {
				e.Action = WatchActionPut
			}
			b.db.watcher.putEvent(e)
		}
	}

	b.committed = true
	return nil
}

func (b *Batch) Rollback() error {
	defer b.unlock() //回滚完成后,解锁DB

	if b.db.closed {
		return ErrDBClosed
	}

	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	if !b.options.ReadOnly {
		// clear pendingWrites
		b.pendingWrites = nil
	}

	b.rollbacked = true
	return nil
}
