package wal

import (
	"bitcaskdb/lru"
	"bitcaskdb/mlog"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/valyala/bytebufferpool"
)

type ChunkType = byte
type SegmentID = uint32

const (
	ChunkTypeFull   ChunkType = iota // 一整条data放在一个chunk
	ChunkTypeFrist                   //该chunk放data的开头
	ChunkTypeMiddle                  //该chunk放data的中间部分
	ChunkTypeLast                    //该chunk放data的结尾部分
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

const (
	chunkHeaderSize = 7 // checksum(4)+len(2)+type(1)

	blockSize = 32 * KB

	fileModePerm = 0644 //文件权限
)

type segment struct {
	id                 SegmentID
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	closed             bool
	cache              *lru.Cache
	header             []byte
	blockPool          sync.Pool
}

// iterator
type segmentReader struct {
	segment     *segment
	blockNumber uint32
	chuncOffset int64
}

type blockAndHeader struct {
	block  []byte
	header []byte
}

// 一个块在一个segment file 中的位置
type ChunkPosition struct {
	SegmentId   SegmentID
	BlockNumber uint32
	ChunkOffset int64
	ChunkSize   uint32
}

// 生成segment file name
func SegmentFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}

// 生成segmentfile
func openSegmentFile(dirPath, extName string, id uint32, cache *lru.Cache) (*segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, extName, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)

	if err != nil {
		return nil, err
	}

	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		mlog.Error("seek to the end of segment file %d%s failed: %v", id, extName, err)
	}

	return &segment{
		id:                 id,
		fd:                 fd,
		cache:              cache,
		header:             make([]byte, chunkHeaderSize),
		blockPool:          sync.Pool{New: newBlockAndHeader},
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
	}, nil

}

func newBlockAndHeader() interface{} {
	return &blockAndHeader{
		block:  make([]byte, blockSize),
		header: make([]byte, chunkHeaderSize),
	}
}

func (seg *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:     seg,
		blockNumber: 0,
		chuncOffset: 0,
	}
}

// Sync
func (seg *segment) Sync() error {
	if seg.closed {
		return nil
	}
	return seg.fd.Sync()
}

// Remove
func (seg *segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.fd.Close()
	}
	return os.Remove(seg.fd.Name())
}

// Close
func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}
	seg.closed = true

	return seg.fd.Close()
}

// Size
func (seg *segment) Size() int64 {
	return int64(seg.currentBlockNumber*blockSize + seg.currentBlockSize)
}

func (seg *segment) Write(data []byte) (*ChunkPosition, error) {
	if seg.closed {
		return nil, ErrClosed
	}

	//剩余空间不足以放下header
	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		if seg.currentBlockSize < blockSize { //0填充
			padding := make([]byte, blockSize-seg.currentBlockSize)
			if _, err := seg.fd.Write(padding); err != nil {
				return nil, err
			}
		}
		seg.currentBlockNumber += 1
		seg.currentBlockSize = 0
	}

	position := &ChunkPosition{
		SegmentId:   seg.id,
		BlockNumber: seg.currentBlockNumber,
		ChunkOffset: int64(seg.currentBlockSize),
	}
	dataSize := uint32(len(data))

	if seg.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		err := seg.WriteInternal(data, ChunkTypeFull)
		if err != nil {
			return nil, err
		}
		position.ChunkSize = dataSize + chunkHeaderSize
		return position, nil
	}

	var leftDataSize = dataSize
	var blockCount uint32 = 0
	for leftDataSize > 0 {
		chunkSize := blockSize - seg.currentBlockSize - chunkHeaderSize
		if chunkSize > leftDataSize {
			chunkSize = leftDataSize
		}

		chunk := make([]byte, chunkSize)

		var end = dataSize - leftDataSize + chunkSize
		if end > dataSize {
			end = dataSize
		}
		copy(chunk[:], data[dataSize-leftDataSize:end])

		var err error

		if leftDataSize == dataSize {
			err = seg.WriteInternal(chunk, ChunkTypeFrist)
		} else if leftDataSize == chunkSize {
			err = seg.WriteInternal(chunk, ChunkTypeLast)
		} else {
			err = seg.WriteInternal(chunk, ChunkTypeMiddle)
		}

		if err != nil {
			return nil, err
		}

		leftDataSize -= chunkSize
		blockCount += 1
	}

	position.ChunkSize = blockCount*chunkHeaderSize + dataSize
	return position, nil
}

func (seg *segment) WriteInternal(data []byte, chuncType ChunkType) error {

	//两字节的len
	dataSize := uint32(len(data))
	binary.LittleEndian.PutUint16(seg.header[4:6], uint16(dataSize))

	//一字节的类型
	seg.header[6] = chuncType

	//四字节的crc校验和
	sum := crc32.ChecksumIEEE(seg.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(seg.header[:4], sum)

	//使用字节池进行优化
	buf := bytebufferpool.Get()

	defer func() {
		bytebufferpool.Put(buf)
	}()

	buf.B = append(buf.B, seg.header...)
	buf.B = append(buf.B, data...)

	if _, err := seg.fd.Write(buf.Bytes()); err != nil {
		return err
	}
	if seg.currentBlockSize > blockSize {
		log.Fatalln("wrong! can not exceed the block size")
	}

	seg.currentBlockSize += dataSize + chunkHeaderSize

	if seg.currentBlockSize == blockSize {
		seg.currentBlockNumber += 1
		seg.currentBlockSize = 0
	}

	return nil
}

func (seg *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	value, _, err := seg.readInternal(blockNumber, chunkOffset)
	return value, err
}

func (seg *segment) readInternal(blockNumber uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if seg.closed {
		return nil, nil, ErrClosed
	}

	var (
		result    []byte
		bh        = seg.blockPool.Get().(*blockAndHeader)
		segSize   = seg.Size()
		nextChunk = &ChunkPosition{SegmentId: seg.id}
	)

	defer func() {
		seg.blockPool.Put(bh)
	}()

	for {
		size := int64(blockSize)
		offset := int64(blockNumber * blockSize)

		//不足一个块
		if size+offset > segSize {
			size = segSize - offset
		}

		//偏移量不符合(大于一个块)
		if chunkOffset >= size {
			return nil, nil, io.EOF
		}

		var ok bool
		var cacheBlock []byte

		if seg.cache != nil {
			cacheBlock, ok = seg.cache.Get(seg.getCacheKey(blockNumber))
		}
		if ok {
			copy(bh.block, cacheBlock)
		} else {
			//缓存未命中
			_, err := seg.fd.ReadAt(bh.block[0:size], offset)
			if err != nil {
				return nil, nil, err
			}

			//进行缓存
			if seg.cache != nil && size == blockSize && len(cacheBlock) == 0 {
				cacheBlock := make([]byte, blockSize)
				copy(cacheBlock, bh.block)
				seg.cache.Add(seg.getCacheKey(blockNumber), cacheBlock)
			}
		}

		//header
		copy(bh.header, bh.block[chunkOffset:chunkOffset+chunkHeaderSize])

		// length
		length := binary.LittleEndian.Uint16(bh.header[4:6])

		//copy data
		start := chunkOffset + chunkHeaderSize
		result = append(result, bh.block[start:start+int64(length)]...)

		// checksum
		checksumEnd := chunkOffset + chunkHeaderSize + int64(length)
		checksum := crc32.ChecksumIEEE(bh.block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(bh.header[:4])
		//数据损坏
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		//type
		chunkType := bh.header[6]

		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEnd

			if checksumEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber += 1
				nextChunk.ChunkOffset = 0
			}
			break
		}

		//该chunk还未读取完成
		blockNumber += 1
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

func (seg *segment) getCacheKey(blockNumber uint32) uint64 {
	return uint64(seg.id)<<32 | uint64(blockNumber)
}

func (segReader *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	if segReader.segment.closed {
		return nil, nil, ErrClosed
	}

	chunkPosition := &ChunkPosition{
		SegmentId:   segReader.segment.id,
		BlockNumber: segReader.blockNumber,
		ChunkOffset: segReader.chuncOffset,
	}

	value, nextChunk, err := segReader.segment.readInternal(
		segReader.blockNumber,
		segReader.chuncOffset,
	)

	if err != nil {
		return nil, nil, err
	}

	chunkPosition.ChunkSize =
		nextChunk.BlockNumber*blockSize + uint32(nextChunk.ChunkOffset) -
			(segReader.blockNumber*blockSize + uint32(segReader.chuncOffset))

	segReader.blockNumber = nextChunk.BlockNumber
	segReader.chuncOffset = nextChunk.ChunkOffset

	return value, chunkPosition, nil
}

func (cp *ChunkPosition) Encode() []byte {
	maxLen := binary.MaxVarintLen32*3 + binary.MaxVarintLen64
	buf := make([]byte, maxLen)

	var index = 0

	// SegmentId
	index += binary.PutUvarint(buf[index:], uint64(cp.SegmentId))
	// BlockNumber
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockNumber))
	// ChunkOffset
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	// ChunkSize
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))

	return buf[:index]
}

func DecodeChunkPosition(buf []byte) *ChunkPosition {
	if len(buf) == 0 {
		return nil
	}

	var index = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[index:])
	index += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n

	return &ChunkPosition{
		SegmentId:   uint32(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
