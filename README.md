## 📚 What is bitcaskdb ?

**BitCaskdb**是基于高效bitcask模型的高性能键值(KV)存储引擎。它提供了快速可靠的数据检索和存储功能。通过利用bitcask模型的简单性和有效性，**BitCaskdb**确保了高效的读写操作，从而提高了整体性能。它提供了一种简化的方法来存储和访问键值对，使其成为需要快速响应数据访问的场景的绝佳选择。**BitCaskdb**对速度和简单性的关注使其成为在平衡存储成本的同时优先考虑性能的应用程序的有价值的替代方案。

**BitCaskdb**存储数据的文件使用预写日志（Write Ahead Log），这些日志文件是具有 block 缓存的只追加写入（append-only）文件。




## 🏁 优势
* 读写延迟低,高吞吐
* 支持多种内存索引,跳表,红黑树,ARTree可供选择
* 纯内存索引,查找速度快
* 崩溃快速恢复,CRC校验，以确保数据一致
* 支持批处理操作,通过雪花算法生成UID区分不同批处理
* 支持过期消息
* LRU缓存,实现高效的WAL
## 🚀 性能测试

#### BTree 索引




## 基本操作
```go
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
