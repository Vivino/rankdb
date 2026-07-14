package rankdb

// End-to-end benchmarks for RankDB.
// These simulate realistic usage patterns: list creation, bulk ingestion,
// score updates, rank lookups, concurrent access, and segment splits/merges.

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Vivino/rankdb/blobstore/memstore"
)

// e2eRandElements generates random elements with a given seed.
func e2eRandElements(n int, seed int64) Elements {
	if n == 0 {
		return Elements{}
	}
	rng := rand.New(rand.NewSource(seed))
	res := make(Elements, n)
	for i := range res {
		res[i] = Element{
			Score:      uint64(rng.Uint32()),
			TieBreaker: rng.Uint32(),
			Updated:    uint32(time.Now().Unix()),
			ID:         ElementID(rng.Uint64()),
			Payload:    []byte(`{"value":"bench","type":"user-list"}`),
		}
	}
	return res
}

// BenchmarkE2E_BulkIngestion benchmarks populating a list with elements.
func BenchmarkE2E_BulkIngestion(b *testing.B) {
	sizes := []int{1000, 10000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Elements_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ctx := context.Background()
				store := memstore.NewMemStore()
				elems := e2eRandElements(size, int64(i))
				_, err := NewList(ctx, "bench-list", "bench-set", store,
					WithListOption.MergeSplitSize(500, 2000),
					WithListOption.Populate(elems),
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkE2E_UpdateElements benchmarks updating elements (the primary write path).
func BenchmarkE2E_UpdateElements(b *testing.B) {
	sizes := []int{1000, 10000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Elements_%d", size), func(b *testing.B) {
			ctx := context.Background()
			store := memstore.NewMemStore()
			elems := e2eRandElements(size, 0xBEEF)
			lst, err := NewList(ctx, "bench-list", "bench-set", store,
				WithListOption.MergeSplitSize(500, 2000),
				WithListOption.Populate(elems),
			)
			if err != nil {
				b.Fatal(err)
			}
			// Generate update batches (reuse same IDs with new scores)
			updateBatchSize := size / 10
			if updateBatchSize < 10 {
				updateBatchSize = 10
			}
			batches := make([]Elements, b.N)
			rng := rand.New(rand.NewSource(0xCAFE))
			for i := range batches {
				batch := make(Elements, updateBatchSize)
				for j := range batch {
					srcIdx := rng.Intn(size)
					batch[j] = Element{
						ID:         elems[srcIdx].ID,
						Score:      uint64(rng.Uint32()),
						TieBreaker: rng.Uint32(),
						Updated:    uint32(time.Now().Unix()),
						Payload:    elems[srcIdx].Payload,
					}
				}
				batches[i] = batch
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := lst.UpdateElements(ctx, store, batches[i], 0, false)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkE2E_GetElements benchmarks looking up elements by ID (read path).
func BenchmarkE2E_GetElements(b *testing.B) {
	sizes := []int{1000, 10000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Elements_%d", size), func(b *testing.B) {
			ctx := context.Background()
			store := memstore.NewMemStore()
			elems := e2eRandElements(size, 0xDEAD)
			lst, err := NewList(ctx, "bench-list", "bench-set", store,
				WithListOption.MergeSplitSize(500, 2000),
				WithListOption.Populate(elems),
			)
			if err != nil {
				b.Fatal(err)
			}
			// Prepare lookup batches
			lookupSize := 50
			rng := rand.New(rand.NewSource(0xFACE))
			lookups := make([][]ElementID, b.N)
			for i := range lookups {
				ids := make([]ElementID, lookupSize)
				for j := range ids {
					ids[j] = elems[rng.Intn(size)].ID
				}
				lookups[i] = ids
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := lst.GetElements(ctx, store, lookups[i], 0)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkE2E_GetRankTop benchmarks fetching top-ranked elements.
func BenchmarkE2E_GetRankTop(b *testing.B) {
	sizes := []int{1000, 10000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Elements_%d", size), func(b *testing.B) {
			ctx := context.Background()
			store := memstore.NewMemStore()
			elems := e2eRandElements(size, 0xFEED)
			lst, err := NewList(ctx, "bench-list", "bench-set", store,
				WithListOption.MergeSplitSize(500, 2000),
				WithListOption.Populate(elems),
			)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := lst.GetRankTop(ctx, store, 0, 100)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkE2E_ConcurrentReadWrite benchmarks concurrent readers and writers.
func BenchmarkE2E_ConcurrentReadWrite(b *testing.B) {
	sizes := []int{1000, 10000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Elements_%d", size), func(b *testing.B) {
			ctx := context.Background()
			store := memstore.NewMemStore()
			elems := e2eRandElements(size, 0x5678)
			lst, err := NewList(ctx, "bench-list", "bench-set", store,
				WithListOption.MergeSplitSize(500, 2000),
				WithListOption.Populate(elems),
			)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				// 4 concurrent readers
				for r := 0; r < 4; r++ {
					wg.Add(1)
					go func(r int) {
						defer wg.Done()
						rng := rand.New(rand.NewSource(int64(i*10 + r)))
						for q := 0; q < 10; q++ {
							ids := []ElementID{elems[rng.Intn(size)].ID}
							_, _ = lst.GetElements(ctx, store, ids, 0)
						}
					}(r)
				}
				// 2 concurrent writers
				for w := 0; w < 2; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						rng := rand.New(rand.NewSource(int64(i*10 + w + 100)))
						batch := make(Elements, 10)
						for j := range batch {
							srcIdx := rng.Intn(size)
							batch[j] = Element{
								ID:         elems[srcIdx].ID,
								Score:      uint64(rng.Uint32()),
								TieBreaker: rng.Uint32(),
								Updated:    uint32(time.Now().Unix()),
								Payload:    elems[srcIdx].Payload,
							}
						}
						_, _ = lst.UpdateElements(ctx, store, batch, 0, false)
					}(w)
				}
				wg.Wait()
			}
		})
	}
}

// BenchmarkE2E_MultiList benchmarks operations across multiple lists.
func BenchmarkE2E_MultiList(b *testing.B) {
	ctx := context.Background()
	store := memstore.NewMemStore()
	numLists := 5
	listSize := 2000
	lists := make([]*List, numLists)
	allElems := make([]Elements, numLists)
	for li := 0; li < numLists; li++ {
		elems := e2eRandElements(listSize, int64(li*1000))
		allElems[li] = elems
		id := ListID(fmt.Sprintf("bench-list-%d", li))
		l, err := NewList(ctx, id, "bench-set", store,
			WithListOption.MergeSplitSize(500, 2000),
			WithListOption.Populate(elems),
		)
		if err != nil {
			b.Fatal(err)
		}
		lists[li] = l
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for li := 0; li < numLists; li++ {
			wg.Add(1)
			go func(li int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(i*numLists + li)))
				// Update a batch
				batch := make(Elements, 20)
				for j := range batch {
					srcIdx := rng.Intn(listSize)
					batch[j] = Element{
						ID:         allElems[li][srcIdx].ID,
						Score:      uint64(rng.Uint32()),
						TieBreaker: rng.Uint32(),
						Updated:    uint32(time.Now().Unix()),
						Payload:    allElems[li][srcIdx].Payload,
					}
				}
				_, _ = lists[li].UpdateElements(ctx, store, batch, 0, false)
				// Lookup
				ids := []ElementID{allElems[li][rng.Intn(listSize)].ID}
				_, _ = lists[li].GetElements(ctx, store, ids, 0)
				// Top
				_, _ = lists[li].GetRankTop(ctx, store, 0, 10)
			}(li)
		}
		wg.Wait()
	}
}

// BenchmarkE2E_DeleteElements benchmarks element deletion.
func BenchmarkE2E_DeleteElements(b *testing.B) {
	ctx := context.Background()
	store := memstore.NewMemStore()
	size := 5000
	elems := e2eRandElements(size, 0xDE1)
	lst, err := NewList(ctx, "bench-list", "bench-set", store,
		WithListOption.MergeSplitSize(500, 2000),
		WithListOption.Populate(elems),
	)
	if err != nil {
		b.Fatal(err)
	}
	// Delete and re-add pattern: delete 50 elements, then re-insert them
	rng := rand.New(rand.NewSource(0xDE2))
	delCount := 50
	batches := make([][]ElementID, b.N)
	reinsertBatches := make([]Elements, b.N)
	for i := range batches {
		delIDs := make([]ElementID, delCount)
		reinsert := make(Elements, delCount)
		for j := range delIDs {
			idx := rng.Intn(size)
			delIDs[j] = elems[idx].ID
			reinsert[j] = Element{
				ID:         elems[idx].ID,
				Score:      elems[idx].Score,
				TieBreaker: elems[idx].TieBreaker,
				Updated:    uint32(time.Now().Unix()),
				Payload:    elems[idx].Payload,
			}
		}
		batches[i] = delIDs
		reinsertBatches[i] = reinsert
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = lst.DeleteElements(ctx, store, batches[i])
		_, _ = lst.UpdateElements(ctx, store, reinsertBatches[i], 0, false)
	}
}
