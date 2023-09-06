package main

import "bitcaskdb/bitcaskdb"

func main() {
	options := bitcaskdb.DefaultOptions
	options.DirPath = "/tmp/bitcaskdb"

	var err error
	db, err := bitcaskdb.Open(options)
	if err != nil {
		panic(err)
	}

	db.Put([]byte("key1"), []byte("value1"))
	db.Put([]byte("key2"), []byte("value2"))
	db.Put([]byte("key3"), []byte("value3"))

	db.Get([]byte("key1"))
	db.Get([]byte("key2"))
	db.Get([]byte("key3"))
}
