package queue

import (
	"testing"
)

func Test_Round_Robin_Simple_Queue_Race(t *testing.T) {
	t.Parallel()
	q := NewQueue(1)
	Queue_Race(t, q, 1, 4)

}

func Test_Round_Robin_Simple_Queue_Random_Timeout_Race(t *testing.T) {
	t.Parallel()
	q := NewQueue(2)
	Queue_Random_Timeout_Race(t, q, 2, 4)

}

func Test_Round_Robin_Simple_Timeout(t *testing.T) {
	t.Parallel()
	q := NewQueue(1)
	Timeout(t, q)

}

func Test_Round_Robin_Simple_Timeout1(t *testing.T) {
	t.Parallel()
	q := NewQueue(1)
	Timeout1(t, q)
}

func Test_Round_Robin_Simple_TimeoutSeveral(t *testing.T) {
	t.Parallel()
	q := NewQueue(1)
	TimeoutSeveral(t, q)
}

func Test_Round_Robin_Simple_PushNewQuery(t *testing.T) {
	t.Parallel()
	q := NewQueue(1)
	PushNewQuery(t, q)
}
