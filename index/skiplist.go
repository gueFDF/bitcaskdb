package index

import (
	"bytes"
	"math/rand"
	"time"
)

const (
	maxLevel int = 18 //设置最大为18层，可以视情况调节
)

type handleEle func(e *Element) bool

type Element struct {
	Node
	key   []byte
	value interface{}
}

type Node struct {
	next []*Element
}

type SkipList struct {
	Node
	maxLevel      int
	Len           int
	prevNodeCache []*Node
	rand          *rand.Rand
}

func NewSkipList() *SkipList {
	return &SkipList{
		Node:          Node{next: make([]*Element, maxLevel)},
		prevNodeCache: make([]*Node, maxLevel),
		maxLevel:      maxLevel,
		rand:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (t *SkipList) randLevel() int {
	i := 1
	for ; i <= t.maxLevel; i++ {
		randomFloat := t.rand.Float64()
		if randomFloat < 0.5 {
			return i
		}
	}
	return i
}

// 用来插入或删除使用
// 和get差不多,只是会存储每一层查找的右边界
func (t *SkipList) backNodes(key []byte) []*Node {
	var prev = &t.Node
	var next *Element

	//用来存储寻找过程中,每一层的最终节点
	prevs := t.prevNodeCache

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}

		//记录寻找的过程
		prevs[i] = prev
	}
	return prevs
}

func (e *Element) Key() []byte {
	return e.key
}

func (e *Element) Value() interface{} {
	return e.value
}

func (e *Element) SetValue(val interface{}) {
	e.value = val
}

func (e *Element) Next() *Element {
	return e.next[0]
}

func (t *SkipList) Front() *Element {
	return t.next[0]
}

// 获取某一个节点
func (t *SkipList) Get(key []byte) *Element {
	var prev = &t.Node //当前节点
	var next *Element  //存放下一个节指针

	//从最高级遍历
	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]
		// 向右移动,寻找比key大的
		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}

	}

	//到达最底层
	if next != nil && bytes.Compare(next.key, key) <= 0 {
		return next
	}

	return nil
}

// 判断key是否存在
func (t *SkipList) Exist(key []byte) bool {
	return t.Get(key) != nil
}

func (t *SkipList) Remove(key []byte) *Element {
	prev := t.backNodes(key)

	//如果存在
	if element := prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		//删除这个节点的所有索引
		for k, v := range element.next {
			prev[k].next[k] = v
		}
		t.Len--
		return element
	}

	//要删除的key不存在
	return nil

}

func (t *SkipList) Put(key []byte, value interface{}) *Element {
	var element *Element

	//寻找key,记录寻找过程每层经过的最后一个节点
	prev := t.backNodes(key)

	//如果第一层存在该key直接修改value
	if element := prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		element.value = value
		return element
	}

	//否则,新建立一个元素用于插入
	element = &Element{
		Node: Node{
			next: make([]*Element, t.randLevel()),
		},
		key:   key,
		value: value,
	}

	//建立索引
	for i := range element.next {
		element.next[i] = prev[i].next[i]
		prev[i].next[i] = element
	}
	t.Len++
	return element
}

func (t *SkipList) Foreach(fun handleEle) {
	for p := t.Front(); p != nil; p = p.Next() {
		if ok := fun(p); !ok {
			break
		}
	}
}

// 按key的前缀查找
func (t *SkipList) FindPrefix(prefix []byte) *Element {
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(prefix, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}


	if next == nil {
		next = t.Front()
	}
	return next
}
