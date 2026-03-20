package util

import "log"

// LogFunc is a printf-style callback for progress and status messages.
// Its signature matches log.Printf so callers can pass log.Printf directly.
type LogFunc func(format string, args ...interface{})

// TerminalLog writes to the standard Go logger (terminal, with timestamps).
var TerminalLog LogFunc = log.Printf

// NopLog discards all log output (useful in tests).
var NopLog LogFunc = func(string, ...interface{}) {}
