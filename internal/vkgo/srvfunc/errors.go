// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package srvfunc

import (
	"os"

	goerrors "github.com/go-errors/errors"
)

// Gorecover служит оберткой над recover() и go-errors для симпатичного вывода ошибки или вызова callback в случае ошибки
func Gorecover(cb func(stack string)) {
	if err := recover(); err == nil {
	} else if stack := goerrors.Wrap(err, 3).ErrorStack(); cb == nil {
		os.Stderr.WriteString(stack)
		os.Stderr.WriteString("\n")
		os.Exit(1)
	} else {
		cb(stack)
	}
}

// EWrap оборачивает ошибку в *goerrors.Error
// Если ошибка уже обернута, не трогает ее.
// Если передан nil, то nil и возвращает.
//
// Deprecated: используйте пакет github.com/pkg/errors.
func EWrap(err error) error {
	if err == nil {
		return nil
	}
	return goerrors.Wrap(err, 1)
}

// EIs сокращение для goerrors.Is
// Исключительно чтобы не использовать явный импорт goerrors, если данный пакет уже импортирован
//
// Deprecated: используйте пакет github.com/pkg/errors.
func EIs(err error, original error) bool {
	return goerrors.Is(err, original)
}
