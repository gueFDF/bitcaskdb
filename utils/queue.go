package utils

type Event struct {
	Data interface{}
}

type EventQueue struct {
	Events   []*Event
	Capacity uint64
	ReadPos  uint64 // read point
	WritePos uint64 // write point
}


func (eq *EventQueue) Push(e *Event) {
	eq.Events[eq.WritePos] = e
	eq.WritePos = (eq.WritePos + 1) % eq.Capacity
}

func (eq *EventQueue) Pop() *Event {
	e := eq.Events[eq.ReadPos]
	eq.FrontTakeAStep()
	return e
}

func (eq *EventQueue) IsFull() bool {
	return (eq.WritePos+1)%eq.Capacity == eq.ReadPos
}

func (eq *EventQueue) IsEmpty() bool {
	return eq.WritePos == eq.ReadPos
}

func (eq *EventQueue) FrontTakeAStep() {
	eq.ReadPos = (eq.ReadPos + 1) % eq.Capacity
}
