package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/blobstore/aerostore"
	"github.com/Vivino/rankdb/blobstore/badgerstore"
	"github.com/Vivino/rankdb/blobstore/boltstore"
	"github.com/Vivino/rankdb/blobstore/memstore"
	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/loggoa"
	"github.com/dgraph-io/badger"
	"github.com/goadesign/goa"
	lru "github.com/hashicorp/golang-lru"
	shutdown "github.com/klauspost/shutdown2"
	"github.com/mattn/go-colorable"
	"github.com/sirupsen/logrus"
)

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

var (
	db        *rankdb.Manager
	cache     rankdb.Cache
	lazySaver *blobstore.LazySaver

	config struct {
		MasterSet           string
		Storage             string
		BackupSet           string
		BackupStorage       string
		BackupEvery         int
		SaveEvery           duration
		PruneEvery          duration
		CacheEntries        int
		CacheType           string
		ListenAddress       string
		JwtKeyPath          string
		ShutdownStageWait   duration
		ShutdownRequestWait duration
		Debug               bool
		ColourLogs          bool
		TimestampLogs       bool
		LogInfo             bool
		ReadTimeout         duration
		ReadHeaderTimeout   duration
		WriteTimeout        duration
		IdleTimeout         duration
		MaxUpdates          int
		Aerospike           struct {
			Hosts          string
			Namespace      string
			KeyPrefix      string
			WriteBlockSize int // See http://www.aerospike.com/docs/reference/configuration#write-block-size
		}

		Badger struct {
			Path   string
			NoSync bool
		}

		BoltDB struct {
			FilePath   string
			NoSync     bool
			NoGrowSync bool
		}

		AWS struct {
			Enabled    bool
			AccessKey  string
			SecretKey  string
			Region     string
			S3Endpoint string
		}

		LazySaver struct {
			Debug        bool
			Enabled      bool
			MaxTime      duration
			LimitItems   int
			FlushAtItems int
			Savers       int
			SaveTimeout  duration
		}

		StorageRetry struct {
			Enabled   bool
			OpTimeout *duration
		}

		NewRelic NewRelicOptions

		Datadog DatadogOptions
	}
)

func StartServer(ctx context.Context, confData io.Reader, lr *logrus.Logger) error {
	bgCtx = ctx
	config.LogInfo = true
	if _, err := toml.DecodeReader(confData, &config); err != nil {
		return err
	}

	if lr != nil {
		if config.ColourLogs {
			lr.Formatter = &logrus.TextFormatter{ForceColors: true, DisableColors: false, DisableTimestamp: !config.TimestampLogs}
			lr.Out = colorable.NewColorableStdout()
		} else {
			lr.Formatter = &logrus.TextFormatter{DisableColors: true, DisableTimestamp: !config.TimestampLogs}
		}
	}

	log.Info(ctx, "Config read, starting server...")
	if config.ShutdownStageWait.Duration != 0 {
		shutdown.SetTimeout(config.ShutdownStageWait.Duration)
	}
	if config.ShutdownRequestWait.Duration != 0 {
		shutdown.SetTimeoutN(shutdown.StagePS, config.ShutdownRequestWait.Duration)
	}
	InitNewRelic(ctx, config.NewRelic)
	InitDatadog(ctx, config.Datadog)
	if nrApp.Enabled() {
		// Intercept error messages.
		ctx = log.WithLogger(ctx, log.Intercept(log.Logger(ctx), nil, func(msg string, keyvals ...interface{}) {
			params := make(map[string]interface{}, len(keyvals)/2)
			for len(keyvals) > 0 {
				key := fmt.Sprint(keyvals[0])
				keyvals = keyvals[1:]
				val := interface{}(errMissingLogValue)
				if len(keyvals) > 0 {
					val = keyvals[0]
					keyvals = keyvals[1:]
				}
				params[fmt.Sprint(key)] = fmt.Sprint(val)
			}
			evtType := strings.Map(func(r rune) rune {
				switch {
				case r >= 'a' && r <= 'z':
					return r
				case r >= 'A' && r <= 'Z':
					return r
				case r == ':' || r == ' ':
					return r
				}
				return '_'
			}, msg)
			// We only have ascii now.
			if len(evtType) > 255 {
				evtType = evtType[:255]
			}
			err := nrApp.app.RecordCustomEvent(evtType, params)
			if err != nil {
				log.Info(ctx, "RecordCustomEvent error", "err", err)
			}
		}))
	}

	initAws(ctx)
	// Get main storage
	store, closeStores, err := newBlobstore(ctx, config.Storage)
	if err != nil {
		return err
	}

	store, err = wrapRetry(store)
	if err != nil {
		return err
	}

	// Get backup storage
	var backup blobstore.WithSet
	if config.BackupStorage != "" && config.BackupEvery > 0 {
		if config.BackupSet == "" {
			return errors.New("backup enabled, but no BackupSet defined in config")
		}
		if config.BackupStorage != "Master" {
			s, cl, err := newBlobstore(ctx, config.BackupStorage)
			if err != nil {
				return err
			}
			s, err = wrapRetry(s)
			if err != nil {
				return err
			}
			backup = blobstore.StoreWithSet(s, config.BackupSet)
			closeStores = append(closeStores, cl...)
		} else {
			backup = blobstore.StoreWithSet(store, config.BackupSet)
		}
		log.Info(ctx, "Backup enabled", "BackupEvery", config.BackupEvery, "BackupSet", config.BackupSet)
	}

	if config.LazySaver.Enabled {
		lazy, err := blobstore.NewLazySaver(store,
			blobstore.WithLazySaveOption.Items(config.LazySaver.LimitItems, config.LazySaver.FlushAtItems),
			blobstore.WithLazySaveOption.Savers(config.LazySaver.Savers),
			blobstore.WithLazySaveOption.MaxTime(config.LazySaver.MaxTime.Duration),
			blobstore.WithLazySaveOption.SaveTimeout(config.LazySaver.SaveTimeout.Duration),
			blobstore.WithLazySaveOption.Logger(log.Logger(ctx)),
			blobstore.WithLazySaveOption.Verbose(config.LazySaver.Debug),
		)
		if err != nil {
			return fmt.Errorf("LazySaver config error: %v", err)
		}
		closeStores = append(closeStores, lazy.Shutdown)
		store = lazy
		lazySaver = lazy
	}

	if config.MaxUpdates > 0 {
		updateBucket = rankdb.NewBucket(config.MaxUpdates)
	}
	if config.CacheEntries > 0 {
		err = nil
		switch config.CacheType {
		case "", "ARC":
			cache, err = lru.NewARC(config.CacheEntries)
		case "LRU":
			var c *lru.Cache
			c, err = lru.New(config.CacheEntries)
			cache = lruWrapper{Cache: c}
		case "LRU2Q":
			cache, err = lru.New2Q(config.CacheEntries)
		default:
			return fmt.Errorf("unknown CacheType %q", config.CacheType)
		}
		if err != nil {
			return err
		}
	}
	db, err = rankdb.NewManager(store, config.MasterSet)
	if err != nil {
		return err
	}
	// Add backup storage
	db.Backup = backup
	db.BackupEvery = config.BackupEvery

	// Load lists
	err = db.LoadLists(ctx, cache)
	if err == blobstore.ErrBlobNotFound {
		log.Info(ctx, "Creating new lists")
		err = db.NewLists(cache)
	}
	db.StartIntervalSaver(ctx, config.SaveEvery.Duration, shutdown.Second("List saver"))
	db.StartListPruner(ctx, config.PruneEvery.Duration, shutdown.First("List pruner"))
	db.StartListSplitter(ctx, store, shutdown.First("List Splitter"))
	if len(closeStores) > 0 {
		shutdown.ThirdFn(func() {
			for i := range closeStores {
				// Do in reverse order.
				defer closeStores[i]()
			}
		}, "Close stores")
	}
	return err
}

// newBlobstore will create a store with config identifier 's'.
// Returned closers must be closed from last to first.
func newBlobstore(ctx context.Context, s string) (store blobstore.Store, closers []func(), err error) {
	switch s {
	case "Badger":
		opts := badger.DefaultOptions(config.Badger.Path)
		opts = opts.WithSyncWrites(config.Badger.NoSync).WithTruncate(config.Badger.NoSync)
		opts = opts.WithLogger(badgerstore.BadgerLogger(ctx))
		bs, err := badgerstore.New(opts)
		if err != nil {
			return nil, nil, err
		}
		store = bs
		closers = append(closers, func() { bs.Close() })
	case "BoltDB":
		bs, err := boltstore.NewBoltStore(config.BoltDB.FilePath, nil)
		if err != nil {
			return nil, nil, err
		}
		store = bs
		db := bs.DB()
		db.NoSync = config.BoltDB.NoSync
		db.NoGrowSync = config.BoltDB.NoGrowSync
		closers = append(closers, func() { bs.Close() })
	case "Memory":
		store = memstore.NewMemStore()
	case "Aerospike":
		as, err := aerostore.New(config.Aerospike.Namespace, config.Aerospike.KeyPrefix, config.Aerospike.Hosts)
		if err != nil {
			return nil, nil, err
		}
		if config.Aerospike.WriteBlockSize == 0 {
			config.Aerospike.WriteBlockSize = 1 << 20
		}
		store, err = blobstore.NewMaxSizeStore(as, config.Aerospike.WriteBlockSize-1024)
		if err != nil {
			return nil, nil, err
		}
	default:
		err = fmt.Errorf("unknown store: %v", config.Storage)
	}
	return store, closers, err
}

// wrapRetry will wrap the supplied store with the configured retry handler.
func wrapRetry(s blobstore.Store) (blobstore.Store, error) {
	// Add Retry if configured.
	if config.StorageRetry.Enabled {
		opts := []blobstore.RetryStoreOption{}
		if config.StorageRetry.OpTimeout != nil {
			opts = append(opts, blobstore.WithRetryOpt.OpTimeout(config.StorageRetry.OpTimeout.Duration))
		}

		return blobstore.NewRetryStore(s, opts...)
	}
	return s, nil
}

// SetLogger attaches loggers to the request.
func SetLogger() goa.Middleware {
	return func(h goa.Handler) goa.Handler {
		return func(ctx context.Context, rw http.ResponseWriter, req *http.Request) error {
			traceRequest := config.LogInfo
			logger := loggoa.Wrap(goa.ContextLogger(ctx))
			if trace := req.Header.Get("X-TRACE-REQUEST"); trace != "" {
				logger = logger.New("trace", trace)
				traceRequest = true
			}
			if strings.Contains(req.URL.RawQuery, "logtrace") {
				if trace, ok := req.URL.Query()["logtrace"]; ok {
					logger = logger.New("trace", trace)
					traceRequest = true
				}
			}
			if !traceRequest {
				logger = log.NullInfo(logger)
			}
			ctx = log.WithLogger(ctx, logger)
			ctx = goa.WithLogger(ctx, loggoa.WrapGoa(logger))
			return h(ctx, rw, req)
		}
	}
}

type lruWrapper struct {
	*lru.Cache
}

func (l lruWrapper) Add(k, v interface{}) {
	l.Cache.Add(k, v)
}

func (l lruWrapper) Remove(k interface{}) {
	l.Cache.Remove(k)
}
