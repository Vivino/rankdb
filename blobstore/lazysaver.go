package blobstore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"container/list"
	"context"
	"fmt"
	"math/bits"
	"sync"
	"time"

	"github.com/Vivino/rankdb/log"
)

// LazySaver enables lazy saving of blobs.
// Items pending saves can be modified and are instantly returned.
type LazySaver struct {
	lazySaveOptions

	store        Store
	mu           sync.RWMutex
	tokens       chan struct{}
	cache        *list.List
	cacheIdx     map[string]*list.Element
	itemAdded    chan struct{}
	savech       chan *list.Element
	shuttingDown bool
	shutdownCh   chan struct{}
	statsMu      sync.Mutex
	stats        LazyStats
	saversWG     sync.WaitGroup
	requestWG    sync.WaitGroup

	// Functions that provide operations.
	get    func(ctx context.Context, set, key string) ([]byte, error)
	set    func(ctx context.Context, set, key string, val []byte) error
	delete func(ctx context.Context, set, key string) error
}

type LazyStats struct {
	Options struct {
		MaxItems    int           `json:"max_items"`
		FlushItems  int           `json:"flush_items"`
		MaxTime     time.Duration `json:"max_time"`
		Savers      int           `json:"savers"`
		SaveTimeout time.Duration `json:"save_timeout"`
	} `json:"options"`
	ItemQueue        int     `json:"item_queue"`
	ItemBytes        int64   `json:"item_bytes"`
	Gets             int     `json:"gets"`
	Sets             int     `json:"sets"`
	Deletes          int     `json:"deletes"`
	LoadedBytes      int64   `json:"loaded_bytes"`
	SetBytes         int64   `json:"set_bytes"`
	GetBytes         int64   `json:"get_bytes"`
	OutGetBytes      int64   `json:"out_get_bytes"`
	OutSetBytes      int64   `json:"out_set_bytes"`
	OutGets          int     `json:"out_gets"`
	OutSets          int     `json:"out_sets"`
	OutDeletes       int     `json:"out_deletes"`
	SavedBytes       int64   `json:"saved_bytes"`
	GetSavedBytesPct float64 `json:"get_saved_pct"`
	SetSavedBytesPct float64 `json:"set_saved_pct"`
}

type updateItemRequest struct {
	ctx      context.Context
	set, key string
	b        []byte
	seen     time.Time
	token    struct{}
	saving   bool
}

type lazySaveOption func(*lazySaveOptions) error

type lazySaveOptions struct {
	verbose     bool
	maxItems    int
	flushItems  int
	maxTime     time.Duration
	savers      int
	saveTimeout time.Duration
	logger      log.Adapter
}

var defaultLazySaveOptions = lazySaveOptions{
	maxItems:    1000,
	flushItems:  900,
	maxTime:     10 * time.Second,
	savers:      10,
	saveTimeout: time.Second,
	logger:      log.DefaultLogger(),
}

// LazySaveOption provides access to LazySave Options that are methods on this struct.
type LazySaveOption struct{}

// WithLazySaveOption provides an element to create lazySave parameters.
var WithLazySaveOption = LazySaveOption{}

// MaxItems will set the maximum number of items in queue, and when to begin flushing items,
// even though they haven't expired.
func (l LazySaveOption) Items(limit, flushAt int) lazySaveOption {
	return func(o *lazySaveOptions) error {
		if limit < 0 {
			return fmt.Errorf("Items limit was < 0 (%v)", limit)
		}
		if flushAt <= 0 {
			return fmt.Errorf("Items flushAt was <= 0 (%v)", flushAt)
		}
		if limit < flushAt {
			return fmt.Errorf("Items limit (%v) < flushAt (%v)", limit, flushAt)
		}
		o.maxItems = limit
		o.flushItems = flushAt
		return nil
	}
}

// MaxTime will set the maximum time an item will stay in cache before being flushed.
func (l LazySaveOption) MaxTime(n time.Duration) lazySaveOption {
	return func(o *lazySaveOptions) error {
		if n <= 0 {
			return fmt.Errorf("TTL was <= 0 (%v)", n)
		}
		o.maxTime = n
		return nil
	}
}

// Savers is the number of concurrent savers running.
func (l LazySaveOption) Savers(n int) lazySaveOption {
	return func(o *lazySaveOptions) error {
		if n <= 0 {
			return fmt.Errorf("Savers was <= 0 (%v)", n)
		}
		o.savers = n
		return nil
	}
}

// SaveTimeout is the deadline set for saving items.
func (l LazySaveOption) SaveTimeout(n time.Duration) lazySaveOption {
	return func(o *lazySaveOptions) error {
		if n <= 0 {
			return fmt.Errorf("SaveTimeout was <= 0 (%v)", n)
		}
		o.saveTimeout = n
		return nil
	}
}

// Logger sets the logger for the async saver.
func (l LazySaveOption) Logger(logger log.Adapter) lazySaveOption {
	return func(o *lazySaveOptions) error {
		o.logger = logger
		return nil
	}
}

// Logger sets the logger for the async saver.
func (l LazySaveOption) Verbose(b bool) lazySaveOption {
	return func(o *lazySaveOptions) error {
		o.verbose = b
		return nil
	}
}

// NewLazySaver will create a new lazy saver backed by the provided store.
func NewLazySaver(store Store, opts ...lazySaveOption) (*LazySaver, error) {
	l := LazySaver{
		lazySaveOptions: defaultLazySaveOptions,
		store:           store,
		cache:           list.New(),
		cacheIdx:        make(map[string]*list.Element),
		itemAdded:       make(chan struct{}, 1),
		savech:          make(chan *list.Element),
		shutdownCh:      make(chan struct{}, 0),
	}
	for _, opt := range opts {
		err := opt(&l.lazySaveOptions)
		if err != nil {
			return nil, err
		}
	}
	l.stats.Options.SaveTimeout = l.saveTimeout
	l.stats.Options.MaxTime = l.maxTime
	l.stats.Options.MaxItems = l.maxItems
	l.stats.Options.Savers = l.savers
	l.stats.Options.FlushItems = l.flushItems

	l.get = func(ctx context.Context, set, key string) ([]byte, error) {
		ctx = log.WithValues(ctx, "set", set, "key", key, "func", "lazy-get")
		l.mu.RLock()
		e, ok := l.cacheIdx[lazyKey(set, key)]
		if ok {
			v := e.Value.(updateItemRequest)
			if v.set == set && v.key == key {
				if l.verbose {
					log.Logger(ctx).Info("Found existing")
				}
				if v.b == nil {
					// Deleted
					l.mu.RUnlock()
					return nil, ErrBlobNotFound
				}
				// Copy before returning.
				dst := append(getDataBufferChunk(len(v.b)), v.b...)
				l.mu.RUnlock()
				l.statsMu.Lock()
				l.stats.LoadedBytes += int64(len(dst))
				l.statsMu.Unlock()
				return dst, nil
			}
		}
		l.mu.RUnlock()
		if l.verbose {
			log.Logger(ctx).Info("Loading from storage")
		}
		b, err := l.store.Get(ctx, set, key)
		l.statsMu.Lock()
		l.stats.OutGets++
		l.stats.OutGetBytes += int64(len(b))
		l.stats.LoadedBytes += int64(len(b))
		l.statsMu.Unlock()
		if err == ErrBlobNotFound {
			log.Info(ctx, "Storage returned not found...")
		}
		return b, err
	}

	l.set = func(ctx context.Context, set, key string, b []byte) error {
		ctx = log.WithValues(ctx, "set", set, "key", key, "func", "lazy-set")
		// Copy b, so caller is free to re-use.
		b = append(getDataBufferChunk(len(b)), b...)
		l.mu.Lock()
		e, ok := l.cacheIdx[lazyKey(set, key)]
		if ok {
			// First update cache, since we have it.
			v := e.Value.(updateItemRequest)
			if !v.saving {
				if l.verbose {
					log.Logger(ctx).Info("Overwriting existing")
				}
				// Old buffer can go into the pool. We have lock.
				putDataBufferChunk(v.b)
				v.b = b
				e.Value = v
				l.mu.Unlock()
				return nil
			}
		}
		r := updateItemRequest{ctx: ctx, set: set, key: key, b: b, seen: time.Now()}
		if l.verbose {
			log.Logger(ctx).Info("Adding new Save")
		}
		l.add(r)
		r.token = <-l.tokens
		return nil
	}

	l.delete = func(ctx context.Context, set, key string) error {
		ctx = log.WithValues(ctx, "set", set, "key", key, "func", "lazy-delete")
		// First update cache, if we have it.
		r := updateItemRequest{ctx: ctx, set: set, key: key, seen: time.Now()}
		l.mu.Lock()
		e, ok := l.cacheIdx[lazyKey(set, key)]
		if ok {
			if l.verbose {
				log.Logger(ctx).Info("Found existing in buffer")
			}
			v := e.Value.(updateItemRequest)
			if !v.saving {
				// Old buffer can go into the pool. We have lock.
				putDataBufferChunk(v.b)
				v.b = nil
				e.Value = v
				l.mu.Unlock()
				return nil
			}
		}
		if l.verbose {
			log.Logger(ctx).Info("Not present in buffer")
		}
		l.add(r)
		r.token = <-l.tokens
		return nil
	}

	// Add tokens
	l.tokens = make(chan struct{}, l.maxItems)
	for i := 0; i < l.maxItems; i++ {
		l.tokens <- struct{}{}
	}
	l.requestWG.Add(1)
	go l.requestHandler()

	l.saversWG.Add(l.savers)
	for i := 0; i < l.savers; i++ {
		go l.startSaver(i)
	}

	return &l, nil
}

// caller must hold l.mu.Lock. This lock is released.
func (l *LazySaver) add(r updateItemRequest) {
	e := l.cache.PushBack(r)
	l.cacheIdx[lazyKey(r.set, r.key)] = e
	l.mu.Unlock()
	select {
	case l.itemAdded <- struct{}{}:
		if l.verbose {
			l.logger.Info("sent item added")
		}
	default:
		if l.verbose {
			l.logger.Info("unable to add sent item added")
		}
	}
}

// saveFront will send oldest element to saver.
// Returns the approximate number of elements in the queue.
func (l *LazySaver) saveFront() int {
	l.mu.Lock()
	e := l.cache.Front()
	for {
		if e == nil {
			l.mu.Unlock()
			return 0
		}
		v := e.Value.(updateItemRequest)
		if !v.saving {
			break
		}
		e = e.Next()
	}
	n := l.cache.Len()
	v := e.Value.(updateItemRequest)
	v.saving = true
	e.Value = v
	l.mu.Unlock()
	l.savech <- e
	return n
}

// requestHandler is a single goroutine that checks timeouts
// and sends to savers.
func (l *LazySaver) requestHandler() {
	defer l.requestWG.Done()
	defer close(l.savech)

	var nextUp *time.Timer
	opts := l.lazySaveOptions
	shutdownCh := l.shutdownCh
	nextUp = time.NewTimer(opts.maxTime)
	l.logger.Info("Starting lazy saver", "max_time", l.maxTime, "max_items", l.maxItems, "save_timeout", l.saveTimeout)
	for {
		l.mu.RLock()
		if l.verbose {
			l.logger.Info("Cycling saver")
		}
		if l.shuttingDown {
			e := l.cache.Front()
			for {
				if e == nil {
					l.mu.RUnlock()
					return
				}
				if !e.Value.(updateItemRequest).saving {
					break
				}
				e = e.Next()
			}
			if l.verbose {
				l.logger.Info("Flushing front item", "queued", l.cache.Len())
			}
			l.mu.RUnlock()
			l.saveFront()
			continue
		}
		if l.cache.Len() > opts.flushItems {
			if l.verbose {
				l.logger.Info("Flushing front item ( > flushItems)", "queued", l.cache.Len())
			}
			l.mu.RUnlock()
			l.saveFront()
			continue
		}
		next := l.cache.Front()
		for {
			if next == nil {
				break
			}
			if v := next.Value.(updateItemRequest); !v.saving {
				nextIn := time.Until(v.seen.Add(l.maxTime))
				if nextIn <= time.Nanosecond {
					nextIn = time.Nanosecond
				}
				nextUp.Reset(nextIn)
				if l.verbose {
					l.logger.Info("Queuing item", "next_duration", nextIn)
				}
				break
			}
			next = next.Next()
		}

		l.mu.RUnlock()
		if l.verbose {
			l.logger.Info("Waiting for signal")
		}
		select {
		case <-nextUp.C:
			if l.verbose {
				l.mu.RLock()
				n := l.cache.Len()
				l.mu.RUnlock()
				l.logger.Info("Flushing front item", "queued", n)
			}
			l.saveFront()
		case <-l.itemAdded:
			l.mu.RLock()
			n := l.cache.Len()
			l.mu.RUnlock()

			if l.verbose {
				l.logger.Info("Item added", "queue", n)
			}
			// Check if enough to force flush
			for n > opts.flushItems {
				n = l.saveFront()
			}
			// Reset timer.
			nextUp.Stop()

			select {
			// Grab timer if not used.
			case <-nextUp.C:
			default:
			}
		case <-shutdownCh:
			l.logger.Info("Shutdown received")
		}
	}
}

func (l *LazySaver) startSaver(n int) {
	// Read everything, so we can be lockless.
	l.mu.RLock()
	timeout := l.saveTimeout
	ctxb := log.WithLogger(context.Background(), l.lazySaveOptions.logger)
	ctxb = log.WithValues(ctxb, "saver_id", n)
	input := l.savech
	tokens := l.tokens
	setFn := l.store.Set
	delFn := l.store.Delete
	l.mu.RUnlock()
	defer l.saversWG.Done()
	for e := range input {
		v := e.Value.(updateItemRequest)
		var dataDone []byte
		ok := func() bool {
			ctx, cancel := context.WithTimeout(log.WithValues(ctxb, "set", v.set, "key", v.key), timeout)
			defer cancel()
			t := time.Now()
			if v.b != nil {
				if l.verbose {
					log.Info(ctx, "Saving")
					defer func() { log.Info(ctx, "Save done", "duration", time.Since(t)) }()
				}
				l.statsMu.Lock()
				l.stats.OutSets++
				l.stats.OutSetBytes += int64(len(v.b))
				l.statsMu.Unlock()

				err := setFn(ctx, v.set, v.key, v.b)
				dataDone = v.b
				if err != nil {
					log.Error(ctx, "Blob SET failed", "err", err)
					return false
				}
				return true
			}
			if l.verbose {
				log.Info(ctx, "Deleting")
				defer func() { log.Info(ctx, "Delete done", "duration", time.Since(t)) }()
			}
			l.statsMu.Lock()
			l.stats.OutDeletes++
			l.statsMu.Unlock()
			err := delFn(ctx, v.set, v.key)
			if err != nil {
				log.Error(ctx, "Blob DELETE failed", "err", err)
				return false
			}
			return true
		}()
		_ = ok
		// Now remove from cache
		tokens <- v.token
		l.mu.Lock()
		l.cache.Remove(e)
		lk := lazyKey(v.set, v.key)
		if found := l.cacheIdx[lk]; e == found {
			delete(l.cacheIdx, lk)
		}
		if dataDone != nil {
			putDataBufferChunk(dataDone)
		}
		l.mu.Unlock()
	}
}

// Shutdown should be called when the server is done writing and
// the remaining data should be flushed.
func (l *LazySaver) Shutdown() {
	log := l.lazySaveOptions.logger
	l.mu.Lock()
	if l.shuttingDown {
		l.mu.Unlock()
		return
	}
	log.Info("Flushing lazy entries", "queued", l.cache.Len())
	l.shuttingDown = true
	l.mu.Unlock()
	close(l.shutdownCh)
	// Wait for all tokens to return
	log.Info("Waiting for tokens")
	for i := 0; i < l.maxItems; i++ {
		<-l.tokens
	}
	log.Info("Waiting for request handler to exit")
	l.requestWG.Wait()
	log.Info("Waiting for savers to exit")
	l.saversWG.Wait()
	l.mu.RLock()
	if clen := l.cache.Len(); clen == 0 {
		log.Info("LazySaver shutdown complete", "queue_len", clen)
	} else {
		log.Error("LazySaver shutdown complete. Items left, did someone write?", "queue_len", clen)
		for e := l.cache.Front(); e != nil; e = e.Next() {
			if v, ok := e.Value.(updateItemRequest); ok {
				log.Error("item", "set", v.set, "key", v.key, "payload_len", len(v.b))
			}
		}

		l.mu.RUnlock()
		time.Sleep(time.Second)
		l.mu.RLock()
	}
	l.mu.RUnlock()
}

// Stats returns stats.
func (l *LazySaver) Stats() LazyStats {
	s := l.stats
	l.mu.RLock()
	for e := l.cache.Front(); e != nil; e = e.Next() {
		s.ItemQueue++
		if v, ok := e.Value.(updateItemRequest); ok {
			s.ItemBytes += int64(len(v.b))
		}
	}
	l.mu.RUnlock()
	s.SavedBytes = s.GetBytes - s.OutGetBytes
	s.SavedBytes += s.SetBytes - s.OutSetBytes
	s.SavedBytes -= s.ItemBytes
	s.GetSavedBytesPct = float64(s.GetBytes-s.OutGetBytes) / float64(s.GetBytes) * 100
	s.SetSavedBytesPct = float64(s.SetBytes-s.OutSetBytes) / float64(s.SetBytes) * 100
	return s
}

// Get a blob.
func (l *LazySaver) Get(ctx context.Context, set, key string) ([]byte, error) {
	l.mu.RLock()
	fn := l.get
	var err error
	if l.shuttingDown {
		err = ErrShuttingDown
	}
	l.mu.RUnlock()
	if err != nil {
		return nil, ErrShuttingDown
	}
	b, err := fn(ctx, set, key)
	l.statsMu.Lock()
	l.stats.Gets++
	l.stats.GetBytes += int64(len(b))
	l.statsMu.Unlock()
	return b, err
}

// Set a blob.
func (l *LazySaver) Set(ctx context.Context, set, key string, b []byte) error {
	l.mu.RLock()
	fn := l.set
	var err error
	if l.shuttingDown {
		err = ErrShuttingDown
	}
	l.mu.RUnlock()
	if err != nil {
		return err
	}
	l.statsMu.Lock()
	l.stats.Sets++
	l.stats.SetBytes += int64(len(b))
	l.statsMu.Unlock()
	return fn(ctx, set, key, b)
}

// Delete a blob.
func (l *LazySaver) Delete(ctx context.Context, set, key string) error {
	l.mu.RLock()
	fn := l.delete
	var err error
	if l.shuttingDown {
		err = ErrShuttingDown
	}
	l.mu.RUnlock()
	if err != nil {
		return ErrShuttingDown
	}
	l.statsMu.Lock()
	l.stats.Deletes++
	l.statsMu.Unlock()
	return fn(ctx, set, key)
}

func lazyKey(set, key string) string {
	return set + "/" + key
}

var (
	dataChunkPools = [...]sync.Pool{
		{New: func() interface{} { return make([]byte, 0, 1<<10) }}, // 0: <=1KB
		{New: func() interface{} { return make([]byte, 0, 1<<11) }}, // 1: <=2KB
		{New: func() interface{} { return make([]byte, 0, 1<<12) }}, // 2: <=4KB
		{New: func() interface{} { return make([]byte, 0, 1<<13) }}, // 3: <=8KB
		{New: func() interface{} { return make([]byte, 0, 1<<14) }}, // ...
		{New: func() interface{} { return make([]byte, 0, 1<<15) }},
		{New: func() interface{} { return make([]byte, 0, 1<<16) }},
		{New: func() interface{} { return make([]byte, 0, 1<<17) }},
		{New: func() interface{} { return make([]byte, 0, 1<<18) }},
		{New: func() interface{} { return make([]byte, 0, 1<<19) }},
		{New: func() interface{} { return make([]byte, 0, 1<<20) }}, // >=1MB
	}
)

// getDataBufferChunk will return a buffer that can at least hold size bytes.
// The returned slice always has length 0.
func getDataBufferChunk(size int) []byte {
	if size <= 1<<10 {
		return dataChunkPools[0].Get().([]byte)
	}
	if size >= 1<<20 {
		return dataChunkPools[10].Get().([]byte)
	}
	return dataChunkPools[bits.Len32(uint32(size-1))-10].Get().([]byte)
}

// putDataBufferChunk will store a used buffer in the appropriate pool.
// The buffer may no longer be used by the caller.
func putDataBufferChunk(p []byte) {
	if cap(p) == 0 {
		return
	}
	i := 0
	size := cap(p)
	if size >= 1<<10 {
		i = bits.Len32(uint32(size-1)) - 10
	}
	if size >= 1<<20 {
		i = 10
	}
	dataChunkPools[i].Put(p[:0])
}
