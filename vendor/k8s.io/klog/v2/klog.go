package klog

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/go-logr/logr"
)

var maxLevel Level = math.MaxInt32
var logger = log.NewNopLogger()
var mu = sync.Mutex{}

type Logger = logr.Logger

// Any function calling klog.FromContext(), klog.Background() or klog.TODO()
// will get a zero-value logger which drops all the logs.
func FromContext(context.Context) Logger { return Logger{} }
func Background() Logger                 { return Logger{} }
func TODO() Logger                       { return Logger{} }

// SetLogger redirects klog logging to the given logger.
// It must be called prior any call to klog.
func SetLogger(l log.Logger) {
	mu.Lock()
	logger = l
	mu.Unlock()
}

// ClampLevel clamps the leveled logging at the specified value.
// It must be called prior any call to klog.
func ClampLevel(l Level) {
	mu.Lock()
	maxLevel = l
	mu.Unlock()
}

type Level int32

type Verbose bool

func V(level Level) Verbose { return level <= maxLevel }

func (v Verbose) Enabled() bool { return bool(v) }

func (v Verbose) Info(args ...interface{}) {
	if v {
		level.Debug(logger).Log("func", "Verbose.Info", "msg", fmt.Sprint(args...))
	}
}

func (v Verbose) Infoln(args ...interface{}) {
	if v {
		level.Debug(logger).Log("func", "Verbose.Infoln", "msg", fmt.Sprint(args...))
	}
}

func (v Verbose) Infof(format string, args ...interface{}) {
	if v {
		level.Debug(logger).Log("func", "Verbose.Infof", "msg", fmt.Sprintf(format, args...))
	}
}

func (v Verbose) InfoS(msg string, keysAndValues ...interface{}) {
	if v {
		level.Debug(logger).Log("func", "Verbose.InfoS", "msg", msg, keysAndValues)
	}
}

func (v Verbose) InfoSDepth(_ int, msg string, keysAndValues ...interface{}) {
	if v {
		level.Debug(logger).Log("func", "Verbose.InfoSDepth", "msg", msg, keysAndValues)
	}
}

func Info(args ...interface{}) {
	level.Debug(logger).Log("func", "Info", "msg", fmt.Sprint(args...))
}

func InfoDepth(_ int, args ...interface{}) {
	level.Debug(logger).Log("func", "InfoDepth", "msg", fmt.Sprint(args...))
}

func Infoln(args ...interface{}) {
	level.Debug(logger).Log("func", "Infoln", "msg", fmt.Sprint(args...))
}

func Infof(format string, args ...interface{}) {
	level.Debug(logger).Log("func", "Infof", "msg", fmt.Sprintf(format, args...))
}

func InfoS(msg string, keysAndValues ...interface{}) {
	level.Debug(logger).Log("func", "InfoS", "msg", msg, keysAndValues)
}

func InfoSDepth(_ int, msg string, keysAndValues ...interface{}) {
	level.Debug(logger).Log("func", "InfoS", "msg", msg, keysAndValues)
}

func Warning(args ...interface{}) {
	level.Warn(logger).Log("func", "Warning", "msg", fmt.Sprint(args...))
}

func WarningDepth(_ int, args ...interface{}) {
	level.Warn(logger).Log("func", "WarningDepth", "msg", fmt.Sprint(args...))
}

func Warningln(args ...interface{}) {
	level.Warn(logger).Log("func", "Warningln", "msg", fmt.Sprint(args...))
}

func Warningf(format string, args ...interface{}) {
	level.Warn(logger).Log("func", "Warningf", "msg", fmt.Sprintf(format, args...))
}

func Error(args ...interface{}) {
	level.Error(logger).Log("func", "Error", "msg", fmt.Sprint(args...))
}

func ErrorDepth(_ int, args ...interface{}) {
	level.Error(logger).Log("func", "ErrorDepth", "msg", fmt.Sprint(args...))
}

func Errorln(args ...interface{}) {
	level.Error(logger).Log("func", "Errorln", "msg", fmt.Sprint(args...))
}

func Errorf(format string, args ...interface{}) {
	level.Error(logger).Log("func", "Errorf", "msg", fmt.Sprintf(format, args...))
}

func ErrorS(err error, msg string, keysAndValues ...interface{}) {
	level.Error(logger).Log("func", "ErrorS", "msg", msg, "err", err, keysAndValues)
}

func Fatal(args ...interface{}) {
	level.Error(logger).Log("func", "Fatal", "msg", fmt.Sprint(args...))
	os.Exit(255)
}

func FatalDepth(_ int, args ...interface{}) {
	level.Error(logger).Log("func", "FatalDepth", "msg", fmt.Sprint(args...))
	os.Exit(255)
}

func Fatalln(args ...interface{}) {
	level.Error(logger).Log("func", "Fatalln", "msg", fmt.Sprint(args...))
	os.Exit(255)
}

func Fatalf(format string, args ...interface{}) {
	level.Error(logger).Log("func", "Fatalf", "msg", fmt.Sprintf(format, args...))
	os.Exit(255)
}

func Exit(args ...interface{}) {
	level.Error(logger).Log("func", "Exit", "msg", fmt.Sprint(args...))
	os.Exit(1)
}

func ExitDepth(_ int, args ...interface{}) {
	level.Error(logger).Log("func", "ExitDepth", "msg", fmt.Sprint(args...))
	os.Exit(1)
}

func Exitln(args ...interface{}) {
	level.Error(logger).Log("func", "Exitln", "msg", fmt.Sprint(args...))
	os.Exit(1)
}

func Exitf(format string, args ...interface{}) {
	level.Error(logger).Log("func", "Exitf", "msg", fmt.Sprintf(format, args...))
	os.Exit(1)
}

// ObjectRef references a kubernetes object
type ObjectRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

// KRef returns ObjectRef from name and namespace
func KRef(namespace, name string) ObjectRef {
	return ObjectRef{
		Name:      name,
		Namespace: namespace,
	}
}

// LoggerWithName() drops the name argument because klog has no ability to
// retrieve the existing key and append the argument to it.
func LoggerWithName(logger Logger, _ string) Logger {
	return logger
}
