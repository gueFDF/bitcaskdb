package benchmark

import (
	"bitcaskdb/bitcaskdb"
	"bitcaskdb/utils"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var db *bitcaskdb.DB

func openDB() func() {
	options := bitcaskdb.DefaultOptions
	options.DirPath = "/tmp/bitcaskdb"

	var err error
	db, err = bitcaskdb.Open(options)
	if err != nil {
		panic(err)
	}

	return func() {
		_ = db.Close()
		_ = os.RemoveAll(options.DirPath)
	}
}

func BenchmarkPutGet(b *testing.B) {
	closer := openDB()
	defer closer()

	b.Run("put", benchmarkPut)
	b.Run("get", bencharkGet)
}

func benchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func bencharkGet(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != bitcaskdb.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}
