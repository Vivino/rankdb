package log

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import "context"

type Adapter interface {
	// Info logs an informational message.
	Info(msg string, keyvals ...interface{})
	// Error logs an error.
	Error(msg string, keyvals ...interface{})
	// New appends to the logger context and returns the updated logger logger.
	New(keyvals ...interface{}) Adapter
}

// key is the type used to store internal values in the context.
// Context provides typed accessor methods to these values.
type key int

const logKey key = 1

// WithLogger sets the request context logger and returns the resulting new context.
func WithLogger(ctx context.Context, logger Adapter) context.Context {
	return context.WithValue(ctx, logKey, logger)
}

// WithValues instantiates a new logger by appending the given key/value pairs to the context
// logger and setting the resulting logger in the context.
func WithValues(ctx context.Context, keyvals ...interface{}) context.Context {
	// If no logger, we do not add value.
	if v := ctx.Value(logKey); v == nil {
		return ctx
	}
	logger := Logger(ctx)
	if logger == nil {
		return ctx
	}
	nl := logger.New(keyvals...)
	return WithLogger(ctx, nl)
}

// WithFn adds the current function name as "func" field and optional fields.
func WithFn(ctx context.Context, keyvals ...interface{}) context.Context {
	// If no logger, we do not add function.
	if v := ctx.Value(logKey); v == nil {
		return ctx
	}

	if f, ok := fnName(1); ok {
		keyvals = append(keyvals, "func", f)
	} else {
		keyvals = append(keyvals, "func_file", caller(1))
	}
	return WithValues(ctx, keyvals...)
}

// Logger extracts the logger from the given context.
func Logger(ctx context.Context) Adapter {
	canceled := false
	select {
	case <-ctx.Done():
		canceled = true
	default:
	}
	var keyvals []interface{}
	if v := ctx.Value(logKey); v != nil {
		if false {
			if f, ok := fnName(1); ok {
				keyvals = []interface{}{"from", f}
			} else {
				keyvals = []interface{}{"from_file", caller(1)}
			}
		}
		if canceled {
			return ErrorToInfo(v.(Adapter).New(keyvals...), "Context cancelled:")
		}
		return v.(Adapter).New(keyvals...)
	}
	a := DefaultLogger().New(keyvals)
	a.Error("no logger in context", caller(1))
	return a
}

// Info extracts the logger from the given context and calls Info on it.
func Info(ctx context.Context, msg string, keyvals ...interface{}) {
	if l := ctx.Value(logKey); l != nil {
		if logger, ok := l.(Adapter); ok {
			logger.Info(msg, keyvals...)
		}
	}
}

// Error extracts the logger from the given context and calls Error on it.
func Error(ctx context.Context, msg string, keyvals ...interface{}) {
	if l := ctx.Value(logKey); l != nil {
		if logger, ok := l.(Adapter); ok {
			if f, ok := fnName(1); ok {
				keyvals = append(keyvals, "from", f)
			}
			keyvals = append(keyvals, "from_file", caller(1))
			logger.Error(msg, keyvals...)
		}
	}
}
