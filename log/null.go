package log

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

// adapter is the stdlib logger adapter.
type nullAdapter struct{}

var nullInstance *nullAdapter

// NullLogger can be used to kill logging.
func NullLogger() Adapter {
	return nullInstance
}

func (a *nullAdapter) Info(msg string, keyvals ...interface{}) {}

func (a *nullAdapter) Error(msg string, keyvals ...interface{}) {}

func (a *nullAdapter) New(keyvals ...interface{}) Adapter {
	return a
}

// adapter is the stdlib logger adapter.
type errAdapter struct {
	a Adapter
}

// NullInfo can be used to kill INFO messages.
func NullInfo(a Adapter) Adapter {
	return &errAdapter{a: a}
}

func (a *errAdapter) Info(msg string, keyvals ...interface{}) {}

func (a *errAdapter) Error(msg string, keyvals ...interface{}) {
	a.a.Error(msg, keyvals...)
}

func (a *errAdapter) New(keyvals ...interface{}) Adapter {
	return &errAdapter{a: a.a.New(keyvals...)}
}

// adapter is the stdlib logger adapter.
type redirectAdapter struct {
	a      Adapter
	prefix string
}

// ErrorToInfo will redirect error messages to info.
func ErrorToInfo(a Adapter, prefix string) Adapter {
	return &redirectAdapter{a: a, prefix: prefix}
}

func (a *redirectAdapter) Info(msg string, keyvals ...interface{}) {
	a.a.Info(msg, keyvals...)
}

func (a *redirectAdapter) Error(msg string, keyvals ...interface{}) {
	a.a.Info(a.prefix+msg, keyvals...)
}

func (a *redirectAdapter) New(keyvals ...interface{}) Adapter {
	return &redirectAdapter{a: a.a.New(keyvals...), prefix: a.prefix}
}
