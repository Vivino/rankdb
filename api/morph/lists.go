package morph

import (
	"context"
	"strings"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/api/app"
	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/log"
)

type List struct {
	In *rankdb.List
}

func (l List) Default() *app.RankdbRanklist {
	in := l.In
	if in == nil {
		return nil
	}
	in.RLock()
	defer in.RUnlock()
	return &app.RankdbRanklist{
		ID:        string(in.ID),
		MergeSize: in.MergeSize,
		Metadata:  in.Metadata,
		Set:       in.Set,
		SplitSize: in.SplitSize,
		LoadIndex: in.LoadIndex,
	}
}

func (l List) Full(ctx context.Context, bs blobstore.Store, elements bool) *app.RankdbRanklistFull {
	in := l.In
	if in == nil {
		return nil
	}
	stats, err := in.Stats(ctx, bs, elements)
	if err != nil {
		log.Error(ctx, "Unable to get stats", "err", err)
		stats = &rankdb.ListStats{}
	}
	in.RLock()
	defer in.RUnlock()

	res := &app.RankdbRanklistFull{
		ID:            string(in.ID),
		Metadata:      in.Metadata,
		MergeSize:     in.MergeSize,
		SplitSize:     in.SplitSize,
		LoadIndex:     in.LoadIndex,
		Elements:      stats.Elements,
		Segments:      stats.Segments,
		Set:           in.Set,
		TopElement:    RankedElement{In: stats.Top}.Default(in.ID),
		BottomElement: RankedElement{In: stats.Bottom}.Default(in.ID),
		CacheHits:     int(stats.CacheHits),
		CacheMisses:   int(stats.CacheMisses),
		CachePercent:  stats.CachePct,
	}
	if stats.Segments > 0 {
		res.AvgSegmentElements = float64(stats.Elements) / float64(stats.Segments)
	}
	return res
}

type APIListIDs struct {
	In []string
}

// IDs returns API list ids.
// Empty and duplicate ids are removed.
func (a APIListIDs) IDs() rankdb.ListIDs {
	res := make(rankdb.ListIDs, 0, len(a.In))
	for _, id := range a.In {
		id = strings.TrimSpace(id)
		if id != "" {
			res = append(res, rankdb.ListID(id))
		}
	}
	res.Deduplicate()
	return res
}
