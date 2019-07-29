package loggoa

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"

	"github.com/Vivino/rankdb/log"
	"github.com/goadesign/goa"
)

type logAdapter struct {
	glog goa.LogAdapter
}

// Info logs an informational message.
func (l logAdapter) Info(msg string, keyvals ...interface{}) {
	l.glog.Info(msg, keyvals...)
}

// Error logs an error.
func (l logAdapter) Error(msg string, keyvals ...interface{}) {
	l.glog.Error(msg, keyvals...)
}

// New appends to the logger context and returns the updated logger logger.
func (l logAdapter) New(keyvals ...interface{}) log.Adapter {
	return logAdapter{glog: l.glog.New(keyvals...)}
}

func Wrap(l goa.LogAdapter) log.Adapter {
	return logAdapter{glog: l}
}

// Wrap extracts the goa logger from the context
func WrapCtx(ctx context.Context) context.Context {
	glog := goa.ContextLogger(ctx)
	la := logAdapter{glog: glog}
	return log.WithLogger(ctx, la)
}

// logger is a shallow wrapper for goa logger.
func WithAdapter(ctx context.Context, adapter goa.LogAdapter) context.Context {
	la := logAdapter{glog: adapter}
	return log.WithLogger(ctx, la)
}

type glogAdapter struct {
	log log.Adapter
}

// Info logs an informational message.
func (l glogAdapter) Info(msg string, keyvals ...interface{}) {
	l.log.Info(msg, keyvals...)
}

// Error logs an error.
func (l glogAdapter) Error(msg string, keyvals ...interface{}) {
	l.log.Error(msg, keyvals...)
}

// New appends to the logger context and returns the updated logger logger.
func (l glogAdapter) New(keyvals ...interface{}) goa.LogAdapter {
	return glogAdapter{log: l.log.New(keyvals...)}
}

// Wrap a log adapter and return a goa adapter.
func WrapGoa(l log.Adapter) goa.LogAdapter {
	return glogAdapter{log: l}
}
