package stats

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
	"go4.org/mem"
)

type parserComb[a any] func(input []byte) (res a, tail []byte, err error)

type comb[a, b any] struct {
	a a
	b b
}

type either[a, b any] struct {
	left   a
	isLeft bool
	right  b
}

const debug_mode = false

var errParse = fmt.Errorf("parse error")

func errMsg(err error, input []byte) error {
	if debug_mode {
		return errors.WithStack(fmt.Errorf("%w, %s", err, string(input)))
	}
	return err
}

func combine[a, b any](l parserComb[a], r parserComb[b]) parserComb[comb[a, b]] {
	return func(input []byte) (res comb[a, b], tail []byte, err error) {
		resA, tail, err := l(input)
		if err != nil {
			return res, input, err
		}
		resB, tail, err := r(tail)
		if err != nil {
			return res, input, err
		}
		return comb[a, b]{resA, resB}, tail, nil

	}
}

func map_[a, b any](l parserComb[a], f func(a) b) parserComb[b] {
	return func(input []byte) (res b, tail []byte, err error) {
		resA, tail, err := l(input)
		if err != nil {
			return res, input, err
		}
		return f(resA), tail, err
	}
}

//func flatMap[a, b any](l parserComb[a], f func(a) parserComb[b]) parserComb[b] {
//	return func(input []byte) (res b, tail []byte, err error) {
//		resA, tail, err := l(input)
//		if err != nil {
//			return res, input, err
//		}
//		return f(resA)(tail)
//	}
//}

func right[a, b any](l parserComb[a], r parserComb[b]) parserComb[b] {
	return func(input []byte) (res b, tail []byte, err error) {
		_, tail, err = l(input)
		if err != nil {
			return res, input, err
		}
		return r(tail)
	}
}

func left[a, b any](l parserComb[a], r parserComb[b]) parserComb[a] {
	return func(input []byte) (res a, tail []byte, err error) {
		res, tail, err = l(input)
		if err != nil {
			return res, input, err
		}
		_, tail, err = r(tail)
		if err != nil {
			return res, input, err
		}
		return res, tail, nil
	}
}

func char(c byte) parserComb[byte] {
	return func(input []byte) (res byte, tail []byte, err error) {
		if len(input) == 0 {
			return res, input, errMsg(errParse, input)
		}
		if input[0] == c {
			return c, input[1:], nil
		}
		return 0, input, errMsg(errParse, input)
	}
}

func anyOf[a any](l parserComb[a], r parserComb[a]) parserComb[a] {
	return func(input []byte) (res a, tail []byte, err error) {
		res, tail, err = l(input)
		if err == nil {
			return res, tail, nil
		}
		res, tail, err = r(input)
		if err == nil {
			return res, tail, nil
		}
		return res, input, errMsg(errParse, input)
	}
}

func or[a, b any](l parserComb[a], r parserComb[b]) parserComb[either[a, b]] {
	return func(input []byte) (res either[a, b], tail []byte, err error) {
		resA, tail, err := l(input)
		if err == nil {
			return either[a, b]{isLeft: true, left: resA}, tail, nil
		}
		resB, tail, err := r(input)
		if err == nil {
			return either[a, b]{right: resB}, tail, nil
		}
		return res, input, errMsg(errParse, input)
	}
}

func prefix(predicate []byte) parserComb[[]byte] {
	return func(input []byte) (res []byte, tail []byte, err error) {
		if len(predicate) > len(input) {
			return nil, input, fmt.Errorf("invalid prefix")
		}
		if bytes.HasPrefix(input, predicate) {
			return input[:len(predicate)], input[len(predicate):], nil
		}
		return nil, input, errMsg(errParse, input)
	}
}

func satisfyIX(pred func(byte) bool) parserComb[int] {
	return func(input []byte) (res int, tail []byte, err error) {
		var i = 0
		for ; i < len(input); i++ {
			if !pred(input[i]) {
				break
			}
		}
		return i, input, nil
	}
}

func satisfy(pred func(byte) bool) parserComb[[]byte] {
	return func(input []byte) (res []byte, tail []byte, err error) {
		i, _, _ := satisfyIX(pred)(input)
		return input[:i], input[i:], nil
	}
}

// helpers:

func number() parserComb[int64] {
	return func(input []byte) (res int64, tail []byte, err error) {
		i := 0
		for ; i < len(input) && input[i] >= '0' && input[i] <= '9'; i++ {
		}

		if i == 0 {
			return 0, input, errMsg(errParse, input)
		}
		n, err := mem.ParseInt(mem.B(input[:i]), 10, 64)
		if err != nil {
			return 0, input, err
		}
		return n, input[i:], nil
	}
}

func toTheEndOfLine() parserComb[[]byte] {
	return func(input []byte) (res []byte, tail []byte, err error) {
		i := bytes.IndexByte(input, '\n')
		if i == -1 {
			return input, nil, nil
		}
		return input[:i+1], input[i+1:], nil

	}
}
