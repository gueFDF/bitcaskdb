package lru

import (
	"reflect"
	"testing"
)

type String string

func (d String) Len() int {
	return len(d)
}

func TestGet(t *testing.T) {
	lru := New(int64(0), nil)
	lru.Add(1, []byte("1234"))
	if v, ok := lru.Get(1); !ok || string(v) != "1234" {
		t.Fatalf("cache hit key1=1234 failed")
	}
	if _, ok := lru.Get(2); ok {
		t.Fatalf("cache miss key2 failed")
	}
}

func TestRemoveoldest(t *testing.T) {
	var k1, k2, k3 uint64 = 1, 2, 3
	v1, v2, v3 := "value1", "value2", "v3"
	cap := len(v1+v2) + 16
	lru := New(int64(cap), nil)
	lru.Add(k1, []byte(v1))
	lru.Add(k2, []byte(v2))
	lru.Add(k3, []byte(v3))

	if _, ok := lru.Get(1); ok || lru.Len() != 2 {
		t.Fatalf("Removeoldest key1 failed")
	}
}

func TestOnEvicted(t *testing.T) {
	keys := make([]uint64, 0)
	callback := func(key uint64, value []byte) {
		keys = append(keys, key)
	}
	lru := New(int64(10), callback)
	lru.Add(1, []byte("123456"))
	lru.Add(2, []byte("k2"))
	lru.Add(3, []byte("k3"))
	lru.Add(4, []byte("k4"))

	expect := []uint64{1, 2, 3}

	if !reflect.DeepEqual(expect, keys) {
		t.Fatalf("Call OnEvicted failed, expect keys equals to %v %v", expect, keys)
	}
}

func TestAdd(t *testing.T) {
	lru := New(int64(0), nil)
	lru.Add(1, []byte("1"))
	lru.Add(1, []byte("111"))

	if lru.nbytes != int64(len("111"))+8 {
		t.Fatal("expected 6 but got", lru.nbytes)
	}
}
