// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package logz

import (
	"io"
	"os"
	"sync/atomic"
)

type Writer struct {
	// writer io.Writer.
	writer atomic.Value
}

func newWriter() *Writer {
	return &Writer{}
}

func (w *Writer) SetOutput(writer io.Writer) {
	w.writer.Store(writer)
}

func (w *Writer) Write(p []byte) (n int, err error) {
	writer, ok := w.writer.Load().(io.Writer)
	if !ok {
		return 0, nil
	}

	return writer.Write(p)
}

func (w *Writer) Close() error {
	return nil
}

func (w *Writer) Sync() error {
	writer, ok := w.writer.Load().(*os.File)
	if !ok {
		return nil
	}

	return writer.Sync()
}
