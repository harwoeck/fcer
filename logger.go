package fcer

import "fmt"

// Logger defines an logging abstraction that is used inside the fcer package.
// The default implementation used prints to standard out.
type Logger interface {
	Info(format string, a ...interface{})
	Debug(format string, a ...interface{})
}

type logger struct {
}

func (l *logger) Info(format string, a ...interface{}) {
	fmt.Printf("fcer [info]: %s\n", fmt.Sprintf(format, a...))
}
func (l *logger) Debug(format string, a ...interface{}) {
	fmt.Printf("fcer [debug]: %s\n", fmt.Sprintf(format, a...))
}

var (
	// Log is the current logging instance used across the fcer package. By
	// default it points to an internal implementation that prints to stdout.
	// It can be overwritten by setting an instance that implements Logger.
	Log Logger
)

func init() {
	Log = &logger{}
}
