package log

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bytes"
	"fmt"
	stdlog "log"
	"os"
)

// adapter is the stdlib logger adapter.
type adapter struct {
	*stdlog.Logger
	keyvals []interface{}
}

const errMissingLogValue = "MISSING"

// DefaultLogger can be used if no fallback can be used.
func DefaultLogger() Adapter {
	return &adapter{Logger: stdlog.New(os.Stdout, "", stdlog.Ltime)}
}

func (a *adapter) Info(msg string, keyvals ...interface{}) {
	a.logit(msg, keyvals, false)
}

func (a *adapter) Error(msg string, keyvals ...interface{}) {
	a.logit(msg, keyvals, true)
}

func (a *adapter) New(keyvals ...interface{}) Adapter {
	if len(keyvals) == 0 {
		return a
	}
	kvs := append(a.keyvals, keyvals...)
	if len(kvs)%2 != 0 {
		kvs = append(kvs, errMissingLogValue)
	}
	return &adapter{
		Logger: a.Logger,
		// Limiting the capacity of the stored keyvals ensures that a new
		// backing array is created if the slice must grow.
		keyvals: kvs[:len(kvs):len(kvs)],
	}
}

func (a *adapter) logit(msg string, keyvals []interface{}, iserror bool) {
	n := (len(keyvals) + 1) / 2
	if len(keyvals)%2 != 0 {
		keyvals = append(keyvals, errMissingLogValue)
	}
	m := (len(a.keyvals) + 1) / 2
	n += m
	var fm bytes.Buffer
	lvl := "INFO"
	if iserror {
		lvl = "EROR" // Not a typo. It ensures all level strings are 4-chars long.
	}
	fm.WriteString(fmt.Sprintf("[%s] %s", lvl, msg))
	vals := make([]interface{}, n)
	offset := len(a.keyvals)
	for i := 0; i < offset; i += 2 {
		k := a.keyvals[i]
		v := a.keyvals[i+1]
		vals[i/2] = v
		fm.WriteString(fmt.Sprintf(" %s=%%+v", k))
	}
	for i := 0; i < len(keyvals); i += 2 {
		k := keyvals[i]
		v := keyvals[i+1]
		vals[i/2+offset/2] = v
		fm.WriteString(fmt.Sprintf(" %s=%%+v", k))
	}
	a.Logger.Printf(fm.String(), vals...)
}
