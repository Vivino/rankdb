// Package testlogger provides a logging adapter for tests and benchmarks.
package testlogger

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"path/filepath"

	"github.com/Vivino/rankdb/log"
)

type logger interface {
	Log(args ...interface{})
	Helper()
}

func New(t *testing.T) log.Adapter {
	return &adapter{t: t}
}

func NewB(t *testing.B) log.Adapter {
	return &adapter{t: t}
}

// adapter is the go-kit log goa logger adapter.
type adapter struct {
	t    logger
	data []interface{}
}

// Info logs informational messages using go-kit.
func (a *adapter) Info(msg string, data ...interface{}) {
	a.t.Helper()
	data = append(data, a.data...)
	// data = append(data, "logged", caller(2))
	// Override file with \r
	a.t.Log("\r\t"+caller(2)+": INFO:", msg, data2print(data))
	//a.t.Log("INFO:", msg, data2print(data))
}

// Error logs error messages using go-kit.
func (a *adapter) Error(msg string, data ...interface{}) {
	a.t.Helper()
	data = append(data, a.data...)
	//data = append(data, "logged", caller(2))
	// Override file with \r
	a.t.Log("\r\t"+caller(2)+": ERROR:", msg, data2print(data))
}

// New instantiates a new logger from the given context.
func (a *adapter) New(data ...interface{}) log.Adapter {
	a2 := adapter{t: a.t}
	a2.data = make([]interface{}, len(a.data)+len(data))
	copy(a2.data, a.data)
	copy(a2.data[len(a.data):], data)
	return &a2
}

func data2print(keyvals []interface{}) string {
	var res []string
	for i := 0; i < len(keyvals); i += 2 {
		k := keyvals[i]
		var v interface{} = "MISSING"
		if i+1 < len(keyvals) {
			v = keyvals[i+1]
		}
		res = append(res, fmt.Sprintf("(\"%v\":\"%v\")", k, v))
	}
	return "[" + strings.Join(res, ", ") + "]"
}

// caller returns the funtion call line at the specified depth
// as "dir/file.go:n:
func caller(depth int) string {
	_, file, line, ok := runtime.Caller(depth + 1)
	if !ok {
		file = "???"
		line = 0
	}
	return fmt.Sprintf("%s:%d", shortenFile(file), line)
}

// shortenFile returns the file name of an absolute file path.
func shortenFile(file string) string {
	_, f := filepath.Split(file)
	return f
}
