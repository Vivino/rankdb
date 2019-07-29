package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"net/http"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/log"
	"github.com/goadesign/goa"
	"github.com/goadesign/goa/middleware"
	shutdown "github.com/klauspost/shutdown2"
)

var (
	errShutdown     = goa.NewErrorClass("server_restarting", http.StatusServiceUnavailable)("Server restarting")
	errCancelled    = goa.NewErrorClass("request_cancelled", 499)("Request Cancelled")
	shutdownStarted = make(chan struct{}, 0)
)

// ShutdownMiddleware rejects request once shutdown starts.
func ShutdownMiddleware(h goa.Handler) goa.Handler {
	return func(ctx context.Context, rw http.ResponseWriter, req *http.Request) error {
		lock := shutdown.Lock(req.Method, req.RequestURI, middleware.ContextRequestID(ctx))
		if lock == nil {
			return errShutdown
		}
		defer lock()
		err := h(ctx, rw, req)
		if err == context.Canceled || ctx.Err() == context.Canceled {
			log.Info(ctx, "Context was cancelled")
			err = nil
			if shutdown.Started() {
				return errShutdown
			}
			return errCancelled
		}
		return err
	}
}

func CancelWithRequest(ctx context.Context, req *http.Request) context.Context {
	if req == nil {
		return ctx
	}
	reqCtx := req.Context()
	if reqCtx == nil {
		return ctx
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-reqCtx.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx
}

var updateBucket rankdb.Bucket

func UpdateRequest(ctx context.Context) (done func(), err error) {
	if updateBucket != nil {
		select {
		case <-ctx.Done():
			return nil, errCancelled
		case <-updateBucket:
			return func() {
				updateBucket <- struct{}{}
			}, nil
		case <-shutdownStarted:
			return nil, errShutdown
		}
	}
	return func() {}, nil
}
