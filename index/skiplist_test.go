package index

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Employee struct {
	id   uint32
	name string
	age  uint8
}

func TestNewSkipList(t *testing.T) {
	list := NewSkipList()
	if list == nil {
		t.Error("new skl err")
	}
}

func TestSkipList_Exist(t *testing.T) {
	list := NewSkipList()
	exist := list.Exist([]byte("11"))
	t.Log(exist)
}

func TestSkipList_FindPrefix(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	list.Put([]byte("ec"), val)
	list.Put([]byte("dc"), val)
	list.Put([]byte("ac"), val)
	list.Put([]byte("ae"), val)
	list.Put([]byte("bc"), val)
	list.Put([]byte("22"), val)

	_ = list.FindPrefix([]byte("a"))
}

func TestSkipList_Put(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	list.Put([]byte("ec"), val)
	list.Put([]byte("dc"), val)
	list.Put([]byte("ac"), val)
	list.Put([]byte("ae"), val)
}

func TestSkipList_Get(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	list.Put([]byte("ec"), val)
	list.Put([]byte("dc"), 123)
	list.Put([]byte("ac"), val)

	list.Put([]byte("111"), Employee{3330912, "mary", 24})

	t.Logf("%v \n", list.Get([]byte("ec")))
	t.Logf("%v \n", list.Get([]byte("ac")))
	t.Logf("%v \n", list.Get([]byte("111")))
}

func TestSkipList_Remove(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	list.Put([]byte("ec"), val)
	list.Put([]byte("dc"), 123)
	list.Put([]byte("ac"), val)

	list.Remove([]byte("dc"))
	list.Remove([]byte("ec"))
	list.Remove([]byte("ac"))
}

func TestSkipList_Foreach(t *testing.T) {
	list := NewSkipList()
	val1 := []byte("test_val1")
	val2 := []byte("test_val2")
	val3 := []byte("test_val3")
	val4 := []byte("test_val4")

	list.Put([]byte("ec"), val1)
	list.Put([]byte("dc"), val2)
	list.Put([]byte("ac"), val3)
	list.Put([]byte("ae"), val4)

	keys := func(e *Element) bool {
		t.Logf("%s ", e.key)
		return false
	}

	list.Foreach(keys)

	vals := func(e *Element) bool {
		t.Logf("%s ", e.value)
		return true
	}

	list.Foreach(vals)
}

func TestSkipList_Foreach2(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	list.Put([]byte("ec"), val)
	list.Put([]byte("dc"), val)
	list.Put([]byte("ac"), val)
	list.Put([]byte("ae"), val)

	list.Foreach(func(e *Element) bool {
		e.value = []byte("test_val_002")
		return true
	})

	for p := list.Front(); p != nil; p = p.Next() {
		fmt.Printf("%s %s \n", string(p.Key()), string(p.Value().([]byte)))
	}
}

func TestElement_Key(t *testing.T) {
	e := Element{key: []byte("a"), value: "a"}
	t.Log(e.key)
}

func TestElement_Value(t *testing.T) {
	e := Element{key: []byte("a"), value: "a"}
	t.Log(e.value)
}



func TestSkipList_PrefixScan(t *testing.T) {
	list := NewSkipList()
	list.Put([]byte("dccdef"), 1)
	list.Put([]byte("eccdef"), 2)
	list.Put([]byte("fccdef"), 3)
	list.Put([]byte("acccbf"), 132)
	list.Put([]byte("acceew"), 44)
	list.Put([]byte("acadef"), 124)
	list.Put([]byte("accdef"), 232)

	e1 := list.FindPrefix([]byte("eee"))
	t.Logf("%+v", e1)

	e2 := list.FindPrefix([]byte("acc"))
	t.Logf("%+v", e2)

	e3 := list.FindPrefix([]byte("accc"))
	t.Logf("%+v", e3)

	e4 := list.FindPrefix([]byte("dcc"))
	t.Logf("%+v", e4)

	e5 := list.FindPrefix([]byte("ecc"))
	t.Logf("%+v", e5)

	e6 := list.FindPrefix([]byte("fcc"))
	t.Logf("%+v", e6)
}

func TestSkipList_all(t *testing.T) {
	list := NewSkipList()
	list.Put([]byte("16"), 16)
	list.Put([]byte("5"), 5)
	list.Put([]byte("14"), 14)
	list.Put([]byte("13"), 13)
	list.Put([]byte("0"), 0)
	list.Put([]byte("3"), 3)
	list.Put([]byte("12"), 12)
	list.Put([]byte("3"), 3)
	list.Put([]byte("12"), 12)

	ele := list.Remove([]byte("3"))
	assert.Equal(t, ele.value.(int), 3)

	ele = list.Get([]byte("6"))
	assert.Nil(t, ele)

	list.Put([]byte("7"), 7)
	ele = list.Remove([]byte("0"))
	assert.Equal(t, ele.value.(int), 0)

	ele = list.Remove([]byte("1"))
	assert.Nil(t, ele)

}
