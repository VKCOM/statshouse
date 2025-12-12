// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package logz

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/VKCOM/statshouse/internal/vkgo/commonmetrics"
	"github.com/VKCOM/statshouse/internal/vkgo/vkd/logz/logzcore"
)

// Logger is basically a zap.Logger wrapper that
// 1. make some defaults for vkd apps
// 2. integrate common metrics
// 3. add trace level
type Logger struct {
	Level Level

	app         string
	withMetrics bool
	zapLogger   *zap.Logger
	metrics     commonmetrics.LogMetrics
}

type (
	Level  = zap.AtomicLevel
	Option = zap.Option
)

type Config struct {
	// Level уровень логгирования: trace/debug/info/warn/error/dpanic/panic/fatal.
	Level string `yaml:"level"`
	// App имя приложения.
	App string `yaml:"app"`
	// Version версия приложения.
	Version string `yaml:"version"`
	// Encoding кодировка вывода: json/console.
	Encoding string `yaml:"encoding"`
	// DisableStacktrace отключает стектрейсы. Если true, то стектрейсы добавляются ко всем уровням выше Error.
	DisableStacktrace bool `yaml:"disable_stacktrace"`
	// CallerSkip offset позволяет кастомизировать добавление Caller в соответствующее поле. См. zap.AddCallerSkip.
	CallerSkipOffset int `yaml:"caller_skip_offset"`
	// FullCaller позволяет показывать полный путь в каждом строке до файла.
	FullCaller  bool     `yaml:"full_caller"`
	Outputs     []string `yaml:"outputs"`
	WithMetrics bool     `yaml:"with_metrics"`
}

func ParseLevel(level string) (Level, error) {
	lvl := zap.NewAtomicLevel()
	if strings.ToLower(level) == "trace" {
		lvl.SetLevel(logzcore.TraceLevel)
		return lvl, nil
	}
	err := lvl.UnmarshalText([]byte(level))
	if err != nil {
		return lvl, fmt.Errorf("error unmarshaling logger level type %q: %w", level, err)
	}

	return lvl, nil
}

func NewFromLogger(logger *zap.Logger) *Logger {
	return &Logger{
		zapLogger: logger,
	}
}

// New создает и настраивает инстанс логгера.
func New(config Config) (*Logger, error) {
	zCfg := zap.NewProductionConfig()

	config.Level = strings.ToLower(config.Level)

	lvl, err := ParseLevel(config.Level)
	if err != nil {
		return nil, err
	}
	if lvl.Level() <= zapcore.DebugLevel {
		zCfg.Sampling = nil
	}

	zCfg.Level = lvl
	zCfg.DisableStacktrace = config.DisableStacktrace
	zCfg.Encoding = config.Encoding
	zCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	if len(config.Outputs) > 0 {
		zCfg.OutputPaths = config.Outputs
	}

	if config.Encoding == "console" {
		zCfg.EncoderConfig.EncodeTime = func(time time.Time, encoder zapcore.PrimitiveArrayEncoder) {
			encoder.AppendString(time.Format("2006-01-02 15:04:05.000"))
		}
		zCfg.EncoderConfig.EncodeLevel = logzcore.CapitalLevelEncoder
	} else {
		zCfg.EncoderConfig.EncodeLevel = logzcore.LowerCaseLevelEncoder
	}

	if config.FullCaller {
		zCfg.EncoderConfig.EncodeCaller = zapcore.FullCallerEncoder
	} else {
		zCfg.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	}

	logger, err := zCfg.Build()
	if err != nil {
		return nil, fmt.Errorf("error building logger: %w", err)
	}

	// добавляем скип строк трейса, так как мы оборачиваем логгер.
	if config.CallerSkipOffset == 0 {
		config.CallerSkipOffset = 1
	}
	logger = logger.WithOptions(zap.AddCallerSkip(config.CallerSkipOffset)).With(zap.Int("pid", os.Getpid()))

	res := &Logger{
		Level:       zCfg.Level,
		app:         config.App,
		withMetrics: config.WithMetrics,
		zapLogger:   logger,
	}
	if config.WithMetrics {
		res.metrics = commonmetrics.NewLogMetrics(config.App)
	}
	return res, nil
}

func (l *Logger) WithOptions(opts ...Option) *Logger {
	newLoggerInstance := l.zapLogger.WithOptions(opts...)

	ret := &Logger{
		Level:       l.Level,
		app:         l.app,
		withMetrics: l.withMetrics,
		zapLogger:   newLoggerInstance,
	}

	if l.withMetrics {
		ret.metrics = commonmetrics.NewLogMetrics(l.app)
	}

	return ret
}

func (l *Logger) NewSubsystem(subsystem string) *Logger {
	ret := &Logger{
		Level:       l.Level,
		app:         l.app,
		withMetrics: l.withMetrics,
		zapLogger:   l.zapLogger,
	}

	if l.withMetrics {
		ret.metrics = commonmetrics.NewLogMetrics(subsystem)
	}

	return ret
}

// Sync флашит буффер логгера во Writer, что под капотом.
func (l *Logger) Sync() error {
	return l.zapLogger.Sync()
}

func (l *Logger) With(args ...Field) *Logger {
	newLoggerInstance := l.zapLogger.With(args...)

	ret := &Logger{
		Level:       l.Level,
		app:         l.app,
		withMetrics: l.withMetrics,
		zapLogger:   newLoggerInstance,
	}

	if l.withMetrics {
		ret.metrics = commonmetrics.NewLogMetrics(l.app)
	}

	return ret
}

func (l *Logger) SetOutput(writer io.Writer) {
	logWriter.SetOutput(writer)
}

func (l *Logger) Trace(message string, args ...Field) {
	if l.withMetrics {
		l.metrics.TraceMetric.Count(1)
	}
	l.zapLogger.Log(logzcore.TraceLevel, message, args...)
}

func (l *Logger) Debug(message string, args ...Field) {
	if l.withMetrics {
		l.metrics.DebugMetric.Count(1)
	}
	l.zapLogger.Log(zapcore.DebugLevel, message, args...)
}

func (l *Logger) Info(message string, args ...Field) {
	if l.withMetrics {
		l.metrics.InfoMetric.Count(1)
	}
	l.zapLogger.Log(zapcore.InfoLevel, message, args...)
}

func (l *Logger) Warn(message string, args ...Field) {
	if l.withMetrics {
		l.metrics.WarnMetric.Count(1)
	}
	l.zapLogger.Log(zapcore.WarnLevel, message, args...)
}

func (l *Logger) Error(message string, args ...Field) {
	if l.withMetrics {
		l.metrics.ErrorMetric.Count(1)
	}
	l.zapLogger.Log(zapcore.ErrorLevel, message, args...)
}

func (l *Logger) Panic(message string, args ...Field) {
	if l.withMetrics {
		l.metrics.PanicMetric.Count(1)
	}
	l.zapLogger.Log(zapcore.PanicLevel, message, args...)
}

func (l *Logger) Fatal(message string, args ...Field) {
	if l.withMetrics {
		l.metrics.FatalMetric.Count(1)
	}
	l.zapLogger.Log(zapcore.FatalLevel, message, args...)
}

func (l *Logger) Tracef(message string, args ...interface{}) {
	if l.withMetrics {
		l.metrics.TraceMetric.Count(1)
	}
	l.zapLogger.Sugar().Logf(logzcore.TraceLevel, message, args...)
}

func (l *Logger) Debugf(message string, args ...interface{}) {
	if l.withMetrics {
		l.metrics.DebugMetric.Count(1)
	}
	l.zapLogger.Sugar().Logf(zap.DebugLevel, message, args...)
}

func (l *Logger) Infof(message string, args ...interface{}) {
	if l.withMetrics {
		l.metrics.InfoMetric.Count(1)
	}
	l.zapLogger.Sugar().Logf(zap.InfoLevel, message, args...)
}

func (l *Logger) Warnf(message string, args ...interface{}) {
	if l.withMetrics {
		l.metrics.WarnMetric.Count(1)
	}
	l.zapLogger.Sugar().Logf(zap.WarnLevel, message, args...)
}

func (l *Logger) Errorf(message string, args ...interface{}) {
	if l.withMetrics {
		l.metrics.ErrorMetric.Count(1)
	}
	l.zapLogger.Sugar().Logf(zap.ErrorLevel, message, args...)
}

func (l *Logger) Panicf(message string, args ...interface{}) {
	if l.withMetrics {
		l.metrics.PanicMetric.Count(1)
	}
	l.zapLogger.Sugar().Logf(zap.PanicLevel, message, args...)
}

func (l *Logger) Fatalf(message string, args ...interface{}) {
	if l.withMetrics {
		l.metrics.FatalMetric.Count(1)
	}
	l.zapLogger.Sugar().Logf(zap.FatalLevel, message, args...)
}

// logWriter глобальный врайтер логов в файлы. Sink: logfile.
var logWriter = newWriter()

func init() {
	err := zap.RegisterSink("logfile", func(url *url.URL) (zap.Sink, error) {
		return logWriter, nil
	})
	if err != nil {
		panic(err)
	}
}
