package main

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"syscall"

	"github.com/Vivino/rankdb/api"
	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/loggoa"
	goalogrus "github.com/goadesign/goa/logging/logrus"
	shutdown "github.com/klauspost/shutdown2"
	"github.com/sirupsen/logrus"
)

var (
	configPath           = flag.String("config", "./conf/conf.toml", "Path for config to use.")
	enableDebug          = flag.Bool("restart", false, "Enable rapid restart mode, press ' and <return>.")
	enableMemoryProfiler = flag.Bool("memory-profiler", false, "Enable memory profiler")

	// SIGUSR2 signal if available.
	usr2Signal os.Signal
	// Context that can be used for background tasks. Never cancelled or deadlined.
)

func main() {
	flag.Parse()

	lr := logrus.New()
	lr.Formatter = &logrus.TextFormatter{DisableColors: true}
	logger := goalogrus.New(lr)

	ctx := log.WithLogger(context.Background(), loggoa.Wrap(logger))
	conf, err := os.Open(*configPath)
	exitOnFailure(err)
	err = api.StartServer(ctx, conf, lr)
	conf.Close()
	exitOnFailure(err)

	shutdown.OnSignal(0, syscall.SIGTERM, syscall.SIGINT)
	if usr2Signal != nil {
		shutdown.OnSignal(0, usr2Signal)
	}
	shutdown.Logger = lr.WithField("service", "shutdown")
	var dumpOnce sync.Once
	shutdown.OnTimeout(func(stage shutdown.Stage, s string) {
		dumpOnce.Do(func() {
			pprof.Lookup("goroutine").WriteTo(lr.Out, 1)
		})
	})

	if *enableMemoryProfiler {
		go ramMonitor(ctx, "/var/tmp/rankdb/memory-dumps")
	}

	if *enableDebug {
		go func() {
			for {
				reader := bufio.NewReader(os.Stdin)
				key, err := reader.ReadString('\n')
				if err != nil {
					log.Error(ctx, "Error reading stdin", "error", err)
					return
				}
				key = strings.TrimSpace(key)
				if key == "'" {
					shutdown.Exit(0)
				}
			}
		}()
	}
	api.StartServices(logger, ctx, err)
}

// exitOnFailure prints a fatal error message and exits the process with status 1.
func exitOnFailure(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "[CRIT] %s", err.Error())
	os.Exit(1)
}
