package benchmark

import (
	"bitcaskdb/wal"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkWAL_Write(b *testing.B) {
	dir, _ := os.MkdirTemp("", "wal-benchmark-test")
	opts := wal.Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	walFile, err := wal.Open(opts)
	assert.Nil(b, err)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err = walFile.Write([]byte("hello world"))
		assert.Nil(b, err)
	}
	b.StopTimer()

	wal.DestroyWAL(walFile)
}

func BenchmarkReadNoCache(b *testing.B) {
	dir, _ := os.MkdirTemp("", "wal-benchmark-test")
	opts := wal.Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	walFile, err := wal.Open(opts)
	assert.Nil(b, err)

	var positions []*wal.ChunkPosition
	for i := 0; i < 1000000; i++ {
		pos, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
		positions = append(positions, pos)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := walFile.Read(positions[rand.Intn(len(positions))])
		assert.Nil(b, err)
	}
	b.StopTimer()
	wal.DestroyWAL(walFile)
}

func BenchmarkReadCache(b *testing.B) {
	dir, _ := os.MkdirTemp("", "wal-benchmark-test")
	opts := wal.Options{
		DirPath:         dir,
		SegmentFileExt:  ".SEG",
		SegmentSize:     32 * 1024 * 1024,
		CacheBlockCount: 1000,
	}
	walFile, err := wal.Open(opts)
	assert.Nil(b, err)

	var positions []*wal.ChunkPosition
	for i := 0; i < 1000000; i++ {
		pos, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
		positions = append(positions, pos)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := walFile.Read(positions[rand.Intn(len(positions))])
		assert.Nil(b, err)
	}
	b.StopTimer()
	wal.DestroyWAL(walFile)
}

func BenchmarkReadNoCache1(b *testing.B) {
	dir, _ := os.MkdirTemp("", "wal-benchmark-test")
	opts := wal.Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	walFile, err := wal.Open(opts)
	assert.Nil(b, err)

	var positions []*wal.ChunkPosition
	for i := 0; i < 10000; i++ {
		pos, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
		positions = append(positions, pos)
	}

	b.ResetTimer()
	b.ReportAllocs()

	var count int = 1
	for i := 0; i < b.N; i++ {
		_, err := walFile.Read(positions[rand.Intn(count)])
		assert.Nil(b, err)
		count++
		if count == len(positions) {
			count = 1
		}
	}
	b.StopTimer()
	wal.DestroyWAL(walFile)
}


func BenchmarkReadCache1(b *testing.B) {
	dir, _ := os.MkdirTemp("", "wal-benchmark-test")
	opts := wal.Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
		CacheBlockCount:10,
	}
	walFile, err := wal.Open(opts)
	assert.Nil(b, err)

	var positions []*wal.ChunkPosition
	for i := 0; i < 10000; i++ {
		pos, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
		positions = append(positions, pos)
	}

	b.ResetTimer()
	b.ReportAllocs()

	var count int = 1
	for i := 0; i < b.N; i++ {
		_, err := walFile.Read(positions[rand.Intn(count)])
		assert.Nil(b, err)
		count++
		if count == len(positions) {
			count = 1
		}
	}
	b.StopTimer()
	wal.DestroyWAL(walFile)
}