package bitcaskdb

import (
	"sync"
	"time"
)

type WatchActionType = byte

const (
	WatchActionPut WatchActionType = iota
	WatchActionDelete
)

type Event struct {
	Action  WatchActionType
	Key     []byte
	Value   []byte
	BatchId uint64
}
type eventQueue struct {
	Events   []*Event
	Capacity uint64
	Front    uint64 // read point
	Back     uint64 // write point
}

func (eq *eventQueue) push(e *Event) {
	eq.Events[eq.Back] = e
	eq.Back = (eq.Back + 1) % eq.Capacity
}

func (eq *eventQueue) pop() *Event {
	e := eq.Events[eq.Front]
	eq.frontTakeAStep()
	return e
}

func (eq *eventQueue) isFull() bool {
	return (eq.Back+1)%eq.Capacity == eq.Front
}

func (eq *eventQueue) isEmpty() bool {
	return eq.Back == eq.Front
}

func (eq *eventQueue) frontTakeAStep() {
	eq.Front = (eq.Front + 1) % eq.Capacity
}

type Watcher struct {
	queue eventQueue
	mu    sync.RWMutex
}

func NewWatcher(capacity uint64) *Watcher {
	return &Watcher{
		queue: eventQueue{
			Events:   make([]*Event, capacity),
			Capacity: capacity,
		},
	}
}

func (w *Watcher) putEvent(e *Event) {
	w.mu.Lock()
	w.queue.push(e)
	if w.queue.isFull() {
		w.queue.frontTakeAStep()
	}
	w.mu.Unlock()
}

func (w *Watcher) getEvent() *Event {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.queue.isEmpty() {
		return nil
	}
	return w.queue.pop()
}

func (w *Watcher) sendEvent(c chan *Event) {
	for {
		event := w.getEvent()
		if event == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		c <- event  // j
	}
}
