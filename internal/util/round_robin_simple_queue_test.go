package util

import (
	"testing"
)

func Test_Round_Robin_Simple_Queue_Race(t *testing.T) {
	t.Parallel()
	q := NewQ1(2)
	Queue_Race(t, q, 8)

}

func Test_Round_Robin_Simple_Queue_Random_Timeout_Race(t *testing.T) {
	t.Parallel()
	q := NewQ1(2)
	Queue_Random_Timeout_Race(t, q, 8)

}

func Test_Round_Robin_Simple_Timeout(t *testing.T) {
	t.Parallel()
	q := NewQ1(1)
	Timeout(t, q)

}

func Test_Round_Robin_Simple_Timeout1(t *testing.T) {
	t.Parallel()
	q := NewQ1(1)
	Timeout1(t, q)
}
