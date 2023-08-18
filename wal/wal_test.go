package wal

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testWriteAndIterate(t *testing.T, wal *WAL, size int, valueSize int) {
	val := strings.Repeat("wal", valueSize)
	positions := make([]*ChunkPosition, size)
	for i := 0; i < size; i++ {
		pos, err := wal.Write([]byte(val))
		assert.Nil(t, err)
		positions[i] = pos
	}

	var count int
	// iterates all the data
	reader := wal.NewReader()
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break
		}
		assert.Equal(t, val, string(data))

		assert.Equal(t, positions[count].SegmentId, pos.SegmentId)
		assert.Equal(t, positions[count].BlockNumber, pos.BlockNumber)
		assert.Equal(t, positions[count].ChunkOffset, pos.ChunkOffset)

		count++
	}
	assert.Equal(t, size, count)
}

func TestWAL_RW1(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write1")
	opts := Options{
		DirPath:         dir,
		SegmentFileExt:  ".SEG",
		SegmentSize:     32 * 1024 * 1024,
		CacheBlockCount: 10,
	}

	wal, err := Open(opts)
	assert.Nil(t, err)
	defer DestroyWAL(wal)

	//write
	pos1, err := wal.Write([]byte("hello1"))
	assert.Nil(t, err)
	assert.NotNil(t, pos1)
	pos2, err := wal.Write([]byte("hello2"))
	assert.Nil(t, err)
	assert.NotNil(t, pos2)
	pos3, err := wal.Write([]byte("hello3"))
	assert.Nil(t, err)
	assert.NotNil(t, pos3)
	pos4, err := wal.Write([]byte("hello4"))
	assert.Nil(t, err)
	assert.NotNil(t, pos4)
	pos5, err := wal.Write([]byte("hello5"))
	assert.Nil(t, err)
	assert.NotNil(t, pos5)

	//read
	val, err := wal.Read(pos1)
	assert.Nil(t, err)
	assert.Equal(t, "hello1", string(val))
	val, err = wal.Read(pos2)
	assert.Nil(t, err)
	assert.Equal(t, "hello2", string(val))
	val, err = wal.Read(pos3)
	assert.Nil(t, err)
	assert.Equal(t, "hello3", string(val))
	val, err = wal.Read(pos4)
	assert.Nil(t, err)
	assert.Equal(t, "hello4", string(val))
	val, err = wal.Read(pos5)
	assert.Nil(t, err)
	assert.Equal(t, "hello5", string(val))

}

func TestWAL_RWlarge(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-rwlarge")
	opts := Options{
		DirPath:         dir,
		SegmentFileExt:  ".SEG",
		SegmentSize:     32 * 1024 * 1024,
		CacheBlockCount: 10,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer DestroyWAL(wal)
	testWriteAndIterate(t, wal, 100000, 512)
}

func TestWAL_RWlarge1(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-rwlarge1")
	opts := Options{
		DirPath:         dir,
		SegmentFileExt:  ".SEG",
		SegmentSize:     32 * 1024 * 1024,
		CacheBlockCount: 10,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer DestroyWAL(wal)
	testWriteAndIterate(t, wal, 20000, 32*1024*3+10)
}

func TestDelete(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-delete")
	opts := Options{
		DirPath:         dir,
		SegmentFileExt:  ".SEG",
		SegmentSize:     32 * 1024 * 1024,
		CacheBlockCount: 10,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer DestroyWAL(wal)
	assert.True(t, wal.IsEmpty())
	testWriteAndIterate(t, wal, 2000, 512)
	assert.False(t, wal.IsEmpty())

	err = wal.Delete()
	assert.Nil(t, err)
	assert.True(t, wal.IsEmpty())
}

func TestReaderAndNext(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-wal-reader-with-start")
	opts := Options{
		DirPath:         dir,
		SegmentFileExt:  ".SEG",
		SegmentSize:     32 * 1024 * 1024,
		CacheBlockCount: 10,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer DestroyWAL(wal)

	_, err = wal.NewReaderWithStart(nil)
	assert.NotNil(t, err)

	reader1, err := wal.NewReaderWithStart(&ChunkPosition{SegmentId: 0, BlockNumber: 0, ChunkOffset: 100})
	assert.Nil(t, err)
	_, _, err = reader1.Next()
	assert.Equal(t, err, io.EOF)
	testWriteAndIterate(t, wal, 20000, 512)
	reader2, err := wal.NewReaderWithStart(&ChunkPosition{SegmentId: 0, BlockNumber: 0, ChunkOffset: 0})
	assert.Nil(t, err)
	_, pos2, err := reader2.Next()
	assert.Nil(t, err)
	assert.Equal(t, pos2.BlockNumber, uint32(0))
	assert.Equal(t, pos2.ChunkOffset, int64(0))
	_, pos3, err := reader2.Next()
	assert.Nil(t, err)
	assert.Equal(t, pos3.ChunkOffset, int64(512*3+chunkHeaderSize))
	_, pos4, err := reader2.Next()
	assert.Nil(t, err)
	assert.Equal(t, pos4.ChunkOffset, int64(512*3+chunkHeaderSize+pos3.ChunkOffset))

}
