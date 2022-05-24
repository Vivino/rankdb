package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"fmt"
	"net"
	"net/http"
	hpprof "net/http/pprof"
	"os"

	"github.com/Vivino/rankdb/api/app"
	"github.com/Vivino/rankdb/log"
	"github.com/goadesign/goa"
	"github.com/goadesign/goa/middleware"
	"github.com/goadesign/goa/middleware/gzip"
	shutdown "github.com/klauspost/shutdown2"
)

var (
	bgCtx context.Context

	//listening will be closed when the server is starting listening.
	// At tat point the listenAddr will be populated.
	listening  = make(chan struct{})
	listenAddr net.Addr
)

func StartServices(logger goa.LogAdapter, ctx context.Context, err error) {
	shutdown.PreShutdownFn(func() {
		close(shutdownStarted)
	})
	// Create service
	service := goa.New("rankdb")
	service.WithLogger(logger)

	enableJWT = true
	if config.JwtKeyPath == "" {
		log.Info(ctx, "JwtKeyPath not set, JWT is disabled.")
		enableJWT = false
	}

	// Mount middleware
	service.Use(SetLogger())
	service.Use(middleware.RequestID())
	service.Use(middleware.LogRequest(false))
	if nrApp.Enabled() {
		service.Use(NewRelicTx())
	}
	if ddApp.Enabled() {
		service.Use(DatadogTx())
	}
	service.Use(middleware.ErrorHandler(service, true))
	service.Use(middleware.Recover())
	service.Use(ShutdownMiddleware)
	service.Use(gzip.Middleware(1))
	// Mount security middlewares
	jwtMiddleware, err := NewJWTMiddleware()
	exitOnFailure(err)
	app.UseJWTMiddleware(service, jwtMiddleware)

	// Mount controllers
	c := NewHealthController(service)
	app.MountHealthController(service, c)
	c2 := NewElementsController(service)
	app.MountElementsController(service, c2)
	c3 := NewListsController(service)
	app.MountListsController(service, c3)
	c4 := NewMultilistElementsController(service)
	app.MountMultilistController(service, c4)
	c5 := NewStaticController(service)
	app.MountStaticController(service, c5)
	c6, err := NewJWTController(service)
	exitOnFailure(err)
	app.MountJWTController(service, c6)
	c7 := NewBackupController(service)
	app.MountBackupController(service, c7)
	serv := service.Server
	if config.Debug {
		log.Info(ctx, "Enabling pprof on /debug/pprof/")
		serveMux := http.NewServeMux()
		serveMux.HandleFunc("/debug/pprof/", hpprof.Index)
		serveMux.HandleFunc("/debug/pprof/cmdline", hpprof.Cmdline)
		serveMux.HandleFunc("/debug/pprof/profile", hpprof.Profile)
		serveMux.HandleFunc("/debug/pprof/symbol", hpprof.Symbol)
		serveMux.HandleFunc("/debug/pprof/trace", hpprof.Trace)
		serveMux.HandleFunc("/", service.Mux.ServeHTTP)
		serv = &http.Server{
			Handler: serveMux,
		}
	}
	// Start service
	log.Info(ctx, "Starting API server", "address", config.ListenAddress, "debug", config.Debug)
	l, err := initServer(serv)
	exitOnFailure(err)
	log.Info(ctx, "Listening...", "address", l.Addr(), "debug", config.Debug)
	la := l.Addr()
	listenAddr = la
	close(listening)
	if err := serv.Serve(l); err != nil && err != http.ErrServerClosed {
		service.LogError("startup", "err", err)
	}
}

// initServer initializes the HTTP server with values from config.
func initServer(s *http.Server) (net.Listener, error) {
	s.ReadTimeout = config.ReadTimeout.Duration
	s.ReadHeaderTimeout = config.ReadHeaderTimeout.Duration
	s.WriteTimeout = config.WriteTimeout.Duration
	s.IdleTimeout = config.IdleTimeout.Duration
	s.Addr = config.ListenAddress
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return nil, err
	}
	return listener, nil
}

// exitOnFailure prints a fatal error message and exits the process with status 1.
func exitOnFailure(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "[CRIT] %s", err.Error())
	shutdown.Exit(1)
}
