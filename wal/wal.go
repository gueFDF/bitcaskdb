package wal

import (
	"bitcaskdb/lru"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
)

const (
	initialSegmentFileID = 1
)

var (
	ErrValueTooLArge = errors.New("the data size can't larger than segment size")
)

type WAL struct {
	activeSegment *segment
	olderSegments map[SegmentID]*segment //用来存放所有的块索引
	options       Options
	mu            sync.RWMutex
	blockCache    *lru.Cache
	bytesWrite    uint32
}

type Reader struct {
	segmentReaders []*segmentReader
	currentReader  int
}

func Open(options Options) (*WAL, error) {

	// must ".***"
	if !strings.HasPrefix(options.SegmentFileExt, ".") {
		return nil, fmt.Errorf("segment file extension must start with '.'")
	}

	// if options.CacheBlockCount > 0 {
	// 	return nil, fmt.Errorf("CacheBlockCount must be greater or equal to zero ")
	// }

	wal := &WAL{
		options:       options,
		olderSegments: make(map[uint32]*segment),
	}

	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	//使用缓存
	if options.CacheBlockCount > 0 {
		var lruSize = options.CacheBlockCount * (8 + blockSize)

		cache := lru.New(int64(lruSize), nil)
		wal.blockCache = cache
	}

	var segmentIds []int

	//	获取所有segment的direntry
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		var id int
		//获取segmentsId
		_, err := fmt.Sscanf(entry.Name(), "%d"+options.SegmentFileExt, &id)
		if err != nil {
			continue
		}
		segmentIds = append(segmentIds, id)
	}

	if len(segmentIds) == 0 {
		segment, err := openSegmentFile(options.DirPath, options.SegmentFileExt,
			initialSegmentFileID, wal.blockCache)

		if err != nil {
			return nil, err

		}
		wal.activeSegment = segment
	} else {
		sort.Ints(segmentIds)
		for i, segId := range segmentIds {
			segment, err := openSegmentFile(options.DirPath, options.SegmentFileExt,
				uint32(segId), wal.blockCache)
			if err != nil {
				return nil, err
			}

			if i == len(segmentIds)-1 {
				wal.activeSegment = segment
			} else {
				wal.olderSegments[segment.id] = segment
			}
		}
	}

	return wal, nil

}

// 创建新的segment
func (wal *WAL) OpenNewActiveSegmentFile() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}

	segment, err := openSegmentFile(wal.options.DirPath, wal.options.SegmentFileExt,
		wal.activeSegment.id+1, wal.blockCache)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment

	return nil
}

func (wal *WAL) ActiveSegmentID() SegmentID {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return wal.activeSegment.id
}

func (wal *WAL) IsEmpty() bool {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return len(wal.olderSegments) == 0 && wal.activeSegment.Size() == 0
}


//创建只能读到segId的reader
func (wal *WAL) NewReaderWithMax(segId SegmentID) *Reader {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var segmentReaders []*segmentReader

	for _, segment := range wal.olderSegments {
		if segId == 0 || segment.id <= segId {
			reader := segment.NewReader()
			segmentReaders = append(segmentReaders, reader)
		}
	}

	if segId == 0 || wal.activeSegment.id <= segId {
		reader := wal.activeSegment.NewReader()
		segmentReaders = append(segmentReaders, reader)
	}

	sort.Slice(segmentReaders, func(i, j int) bool {
		return segmentReaders[i].segment.id < segmentReaders[j].segment.id
	})

	return &Reader{
		segmentReaders: segmentReaders,
		currentReader:  0,
	}
}

//指定位置创建Reader
func (wal *WAL) NewReaderWithStart(startPos *ChunkPosition) (*Reader, error) {
	if startPos == nil {
		return nil, errors.New("start position is nil ")
	}
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	reader := wal.NewReader()

	for {
		// 移动到startpos
		if reader.CurrentSegmentId() < startPos.SegmentId {
			reader.SkipCurrentSegment()
			continue
		}

		currentPos := reader.CurrentChunkPosition()
		if currentPos.BlockNumber >= startPos.BlockNumber &&
			currentPos.ChunkOffset >= startPos.ChunkOffset {
			break
		}

		if _, _, err := reader.Next(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return reader, nil
}

func (wal *WAL) NewReader() *Reader {
	return wal.NewReaderWithMax(0)
}

func (r *Reader) Next() ([]byte, *ChunkPosition, error) {
	//超出范围了
	if r.currentReader >= len(r.segmentReaders) {
		return nil, nil, io.EOF
	}

	data, position, err := r.segmentReaders[r.currentReader].Next()
	if err == io.EOF {
		r.currentReader++
		return r.Next()
	}
	return data, position, err
}

func (r *Reader) SkipCurrentSegment() {
	r.currentReader++
}

func (r *Reader) CurrentSegmentId() SegmentID {
	return r.segmentReaders[r.currentReader].segment.id
}

func (r *Reader) CurrentChunkPosition() *ChunkPosition {
	reader := r.segmentReaders[r.currentReader]
	return &ChunkPosition{
		SegmentId:   reader.segment.id,
		BlockNumber: reader.blockNumber,
		ChunkOffset: reader.chuncOffset,
	}
}

func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if int64(len(data)+chunkHeaderSize) > wal.options.SegmentSize {
		return nil, ErrValueTooLArge
	}

	//当前segment装不下
	if wal.isFull(int64(len(data))) {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWrite = 0
		segment, err := openSegmentFile(wal.options.DirPath, wal.options.SegmentFileExt,
			wal.activeSegment.id+1, wal.blockCache)
		if err != nil {
			return nil, err
		}

		//更新
		wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
		wal.activeSegment = segment
	}

	position, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	wal.bytesWrite += position.ChunkSize

	// 判断是否sync
	var needSync = wal.options.Sync
	if !needSync && wal.options.BytesPerSync > 0 {
		needSync = wal.bytesWrite >= wal.options.BytesPerSync
	}
	if needSync {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWrite = 0
	}

	return position, nil
}

func (wal *WAL) Read(pos *ChunkPosition) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var segment *segment
	if pos.SegmentId == wal.activeSegment.id {
		segment = wal.activeSegment
	} else {
		segment = wal.olderSegments[pos.SegmentId]
	}
	if segment == nil {
		return nil, fmt.Errorf("segment file %d%s not found", pos.SegmentId, wal.options.SegmentFileExt)
	}

	return segment.Read(pos.BlockNumber, pos.ChunkOffset)
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// purge the block cache.
	if wal.blockCache != nil {
		//	wal.blockCache.Purge()
		wal.blockCache = nil
	}

	// close all segment files.
	for _, segment := range wal.olderSegments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	wal.olderSegments = nil

	// close the active segment file.
	return wal.activeSegment.Close()
}
func (wal *WAL) Delete() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.blockCache != nil {
		// TODO 清除缓存块
		wal.blockCache = nil
	}

	for _, segment := range wal.olderSegments {
		if err := segment.Remove(); err != nil {
			return err
		}
	}

	wal.olderSegments = nil

	// delete the active segment file.
	wal.activeSegment.currentBlockNumber = 0
	wal.activeSegment.currentBlockSize = 0
	return wal.activeSegment.Remove()
}

func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}

func (wal *WAL) isFull(dataSize int64) bool {
	return wal.activeSegment.Size()+dataSize+chunkHeaderSize > wal.options.SegmentSize
}

func DestroyWAL(wal *WAL) {
	if wal != nil {
		_ = wal.Close()
		_ = os.RemoveAll(wal.options.DirPath)
	}
}
