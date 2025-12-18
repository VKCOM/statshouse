// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package logz

import (
	"io"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/VKCOM/statshouse/internal/vkgo/vkd/logz/logzcore"
)

// A Syncer is a spy for the Sync portion of zapcore.WriteSyncer.
type Syncer struct {
	err    error
	called bool
}

// SetError sets the error that the Sync method will return.
func (s *Syncer) SetError(err error) {
	s.err = err
}

// Sync records that it was called, then returns the user-supplied error (if
// any).
func (s *Syncer) Sync() error {
	s.called = true
	return s.err
}

// Called reports whether the Sync method was called.
func (s *Syncer) Called() bool {
	return s.called
}

// A Discarder sends all writes to io.Discard.
type Discarder struct{ Syncer }

// Write implements io.Writer.
func (d *Discarder) Write(b []byte) (int, error) {
	return io.Discard.Write(b)
}

func BenchmarkCoreLogger(b *testing.B) {
	ec := zap.NewProductionEncoderConfig()
	ec.EncodeDuration = zapcore.NanosDurationEncoder
	ec.EncodeTime = zapcore.EpochNanosTimeEncoder
	enc := zapcore.NewJSONEncoder(ec)

	logger := zap.New(zapcore.NewCore(
		enc,
		&Discarder{},
		logzcore.TraceLevel,
	))

	field := Int("i", 1000)

	b.Run("Info", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			logger.Log(zap.InfoLevel, "test", field)
		}
	})
	b.Run("Trace", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			logger.Log(logzcore.TraceLevel, "test", field)
		}
	})
}

func BenchmarkLoggerWithSink(b *testing.B) {
	l, err := New(Config{
		Level:    "trace",
		App:      "logz-test",
		Encoding: "console",
		Outputs:  []string{"/dev/null"},
	})
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Info", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			l.Info("test %d", Int("i", i*1000))
		}
	})
	b.Run("Trace", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			l.Trace("test %d", Int("i", i*1000))
		}
	})
	b.Run("Infof", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			l.Infof("test %d", i)
		}
	})

	b.Run("Tracef", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			l.Tracef("test %d", i)
		}
	})
}

func BenchmarkLoggerWithSink_WithMetrics(b *testing.B) {
	l, err := New(Config{
		Level:       "trace",
		App:         "logz-test",
		Encoding:    "console",
		Outputs:     []string{"/dev/null"},
		WithMetrics: true,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Info", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			l.Info("test %d", Int("i", i*1000))
		}
	})
	b.Run("Trace", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			l.Trace("test %d", Int("i", i*1000))
		}
	})
	b.Run("Infof", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			l.Infof("test %d", i)
		}
	})

	b.Run("Tracef", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			l.Tracef("test %d", i)
		}
	})
}
