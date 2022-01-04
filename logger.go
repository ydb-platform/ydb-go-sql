package ydb

import (
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type Logger interface {
	// Tracef logs at Trace logger level using fmt formatter
	Tracef(format string, args ...interface{})
	// Debugf logs at Debug logger level using fmt formatter
	Debugf(format string, args ...interface{})
	// Infof logs at Info logger level using fmt formatter
	Infof(format string, args ...interface{})
	// Warnf logs at Warn logger level using fmt formatter
	Warnf(format string, args ...interface{})
	// Errorf logs at Error logger level using fmt formatter
	Errorf(format string, args ...interface{})
	// Fatalf logs at Fatal logger level using fmt formatter
	Fatalf(format string, args ...interface{})

	// WithName provide applying sub-scope of logger messages
	WithName(name string) interface{} // interface must cast to Logger type
}

type Level ydb.Level

const (
	QUIET = Level(ydb.QUIET)
	TRACE = Level(ydb.TRACE)
	DEBUG = Level(ydb.DEBUG)
	INFO  = Level(ydb.INFO)
	WARN  = Level(ydb.WARN)
	ERROR = Level(ydb.ERROR)
	FATAL = Level(ydb.FATAL)
)

type LoggerOption ydb.LoggerOption

func WithNamespace(namespace string) LoggerOption {
	return LoggerOption(ydb.WithNamespace(namespace))
}

func WithMinLevel(minLevel Level) LoggerOption {
	return LoggerOption(ydb.WithMinLevel(ydb.Level(minLevel)))
}

func WithNoColor(b bool) LoggerOption {
	return LoggerOption(ydb.WithNoColor(b))
}

func WithExternalLogger(external Logger) LoggerOption {
	return LoggerOption(ydb.WithExternalLogger(external))
}

func WithOutWriter(out io.Writer) LoggerOption {
	return LoggerOption(ydb.WithOutWriter(out))
}

func WithErrWriter(err io.Writer) LoggerOption {
	return LoggerOption(ydb.WithErrWriter(err))
}
