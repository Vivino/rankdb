package log

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

// adapter is the stdlib logger adapter.
type interceptAdapter struct {
	errCB func(msg string, keyvals ...interface{})
	infCB func(msg string, keyvals ...interface{})
	a     Adapter
}

// Intercept can be used to intercept messages.
func Intercept(a Adapter, info, err func(msg string, keyvals ...interface{})) Adapter {
	return &interceptAdapter{a: a, errCB: err, infCB: info}
}

func (a *interceptAdapter) Info(msg string, keyvals ...interface{}) {
	if a.infCB != nil {
		a.infCB(msg, keyvals...)
	}
	a.a.Info(msg, keyvals...)
}

func (a *interceptAdapter) Error(msg string, keyvals ...interface{}) {
	if a.errCB != nil {
		a.errCB(msg, keyvals...)
	}
	a.a.Error(msg, keyvals...)
}

func (a *interceptAdapter) New(keyvals ...interface{}) Adapter {
	return &interceptAdapter{a: a.a.New(keyvals...), errCB: a.errCB, infCB: a.infCB}
}
