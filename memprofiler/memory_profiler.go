package memprofiler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/Vivino/rankdb/log"
	shutdown "github.com/klauspost/shutdown2"
)

const dumpTimeLayout = "2006-01-02-150405"

// Run monitors the memory-usage of your application, and dumps profiles at peak usage.
func Run(ctx context.Context, path string) {
	// Don't dump in the quiet period while starting up.
	const (
		quietPeriod = 3 * time.Minute
		checkEvery  = 10 * time.Second
		minRise     = 100 << 20
	)

	stats := runtime.MemStats{}
	var high uint64
	if path == "" {
		return
	}
	ctx, cancel := shutdown.CancelCtx(log.WithValues(ctx, "module", "RAM Monitor"))
	defer cancel()

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		log.Error(ctx, "Unable to create memory dump folder")
	}

	t := time.NewTicker(checkEvery)
	defer t.Stop()
	go cleanOld(ctx, path)

	quietEnds := time.Now().Add(quietPeriod)
	for {
		select {
		case <-t.C:
			runtime.ReadMemStats(&stats)
			if stats.Alloc <= high+(minRise) {
				continue
			}
			high = stats.Alloc
			if time.Now().Before(quietEnds) {
				log.Info(
					log.WithValues(ctx, "alloc_mb", stats.Alloc>>20),
					"In quiet period, skipping dump")
				continue
			}

			timeStamp := time.Now().Format(dumpTimeLayout)
			fn := filepath.Join(path, fmt.Sprintf("%s-%dMB.bin", timeStamp, high>>20))

			log.Info(
				log.WithValues(ctx, "filename", fn, "alloc_mb", stats.Alloc>>20),
				"Memory peak, dumping memory profile")
			f, err := os.Create(fn)
			if err != nil {
				log.Error(
					log.WithValues(ctx, "error", err),
					"could not create memory profile")
			}
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Error(
					log.WithValues(ctx, "error", err),
					"could not write memory profile")
			}
		case <-ctx.Done():
			return
		}
	}
}

// cleanOld will remove all dumps more than 30 days old.
func cleanOld(ctx context.Context, path string) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			timeStamp := time.Now().Add(-31 * 24 * time.Hour).Format(dumpTimeLayout)
			log.Info(ctx, "Deleting old dumps containing %q", timeStamp)

			err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
				if err == nil || info == nil {
					return err
				}
				if !info.IsDir() && strings.Contains(info.Name(), timeStamp) {
					return os.Remove(info.Name())
				}
				return nil
			})
			if err != nil {
				log.Error(
					log.WithValues(ctx, "error", err),
					"error deleting old dumps")
			}
		case <-ctx.Done():
			return
		}
	}
}
