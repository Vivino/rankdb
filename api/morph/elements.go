package morph

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/api/app"
)

type RankedElement struct {
	In *rankdb.RankedElement
}

func (r RankedElement) Tiny() *app.RankdbElementTiny {
	in := r.In
	if in == nil {
		return nil
	}
	return &app.RankdbElementTiny{
		ID:         uint64(in.ID),
		FromBottom: in.FromBottom,
		FromTop:    in.FromTop,
		Score:      in.Score,
		Payload:    in.Payload,
	}
}

func (r RankedElement) Default(id rankdb.ListID) *app.RankdbElement {
	in := r.In
	if in == nil {
		return nil
	}
	return &app.RankdbElement{
		ID:         uint64(in.ID),
		ListID:     string(id),
		Payload:    json.RawMessage(in.Payload),
		FromBottom: in.FromBottom,
		FromTop:    in.FromTop,
		Score:      in.Score,
		TieBreaker: in.TieBreaker,
		UpdatedAt:  time.Unix(int64(in.Updated), 0),
	}
}

func (r RankedElement) Full(id rankdb.ListID) *app.RankdbElementFull {
	in := r.In
	if in == nil {
		return nil
	}
	n := struct {
		// WTF - didn't this change in Go, so you didn't have to match tags?
		Above app.RankdbElementCollection `form:"above,omitempty" json:"above,omitempty" yaml:"above,omitempty" xml:"above,omitempty"`
		Below app.RankdbElementCollection `form:"below,omitempty" json:"below,omitempty" yaml:"below,omitempty" xml:"below,omitempty"`
	}{
		Below: Elements{In: in.Below}.ToRanked(in.FromTop+1, in.FromBottom-1, id),
		Above: Elements{In: in.Above}.ToRanked(in.FromTop-len(in.Above), in.FromBottom+len(in.Above), id),
	}

	return &app.RankdbElementFull{
		ID:         uint64(in.ID),
		ListID:     string(id),
		Payload:    in.Payload,
		FromBottom: in.FromBottom,
		FromTop:    in.FromTop,
		Score:      in.Score,
		TieBreaker: in.TieBreaker,
		UpdatedAt:  time.Unix(int64(in.Updated), 0),
		Neighbors:  &n,
	}
}

func (r RankedElement) FullExt(id rankdb.ListID, below, above rankdb.RankedElements) *app.RankdbElementFull {
	in := r.In
	if in == nil {
		return nil
	}
	n := struct {
		// WTF - didn't this change in Go, so you didn't have to match tags?
		Above app.RankdbElementCollection `form:"above,omitempty" json:"above,omitempty" yaml:"above,omitempty" xml:"above,omitempty"`
		Below app.RankdbElementCollection `form:"below,omitempty" json:"below,omitempty" yaml:"below,omitempty" xml:"below,omitempty"`
	}{
		Below: RankedElements{In: below}.Default(id),
		Above: RankedElements{In: above}.Default(id),
	}

	return &app.RankdbElementFull{
		ID:         uint64(in.ID),
		ListID:     string(id),
		Payload:    in.Payload,
		FromBottom: in.FromBottom,
		FromTop:    in.FromTop,
		Score:      in.Score,
		TieBreaker: in.TieBreaker,
		UpdatedAt:  time.Unix(int64(in.Updated), 0),
		Neighbors:  &n,
	}
}

func (r RankedElement) FullUpdate(id rankdb.ListID) *app.RankdbElementFullUpdate {
	in := r.In
	if in == nil {
		return nil
	}
	n := struct {
		// WTF - didn't this change in Go, so you didn't have to match tags?
		Above app.RankdbElementCollection `form:"above,omitempty" json:"above,omitempty" yaml:"above,omitempty" xml:"above,omitempty"`
		Below app.RankdbElementCollection `form:"below,omitempty" json:"below,omitempty" yaml:"below,omitempty" xml:"below,omitempty"`
	}{
		Below: Elements{In: in.Below}.ToRanked(in.FromTop+1, in.FromBottom-1, id),
		Above: Elements{In: in.Above}.ToRanked(in.FromTop-len(in.Above), in.FromBottom+len(in.Above), id),
	}

	return &app.RankdbElementFullUpdate{
		ID:           uint64(in.ID),
		ListID:       string(id),
		Payload:      in.Payload,
		FromBottom:   in.FromBottom,
		FromTop:      in.FromTop,
		Score:        in.Score,
		TieBreaker:   in.TieBreaker,
		UpdatedAt:    time.Unix(int64(in.Updated), 0),
		Neighbors:    &n,
		PreviousRank: RankedElement{In: in.Before}.Default(id),
	}
}

type RankedElements struct {
	In rankdb.RankedElements
}

func (r RankedElements) Tiny() []*app.RankdbElementTiny {
	in := r.In
	res := make([]*app.RankdbElementTiny, len(in))
	for i, re := range in {
		res[i] = RankedElement{In: &re}.Tiny()
	}
	return res
}

func (r RankedElements) Default(id rankdb.ListID) []*app.RankdbElement {
	in := r.In
	res := make([]*app.RankdbElement, len(in))
	for i, re := range in {
		res[i] = RankedElement{In: &re}.Default(id)
	}
	return res
}

func (r RankedElements) Full(id rankdb.ListID) []*app.RankdbElementFull {
	in := r.In
	res := make([]*app.RankdbElementFull, len(in))
	for i, re := range in {
		res[i] = RankedElement{In: &re}.Full(id)
	}
	return res
}

type Elements struct {
	In []rankdb.Element
}

func (e Elements) ToRanked(firstFromTop, firstFromBottom int, id rankdb.ListID) []*app.RankdbElement {
	in := e.In
	var res []*app.RankdbElement
	for _, e := range in {
		res = append(res, &app.RankdbElement{
			ID:         uint64(e.ID),
			ListID:     string(id),
			Payload:    e.Payload,
			FromBottom: firstFromBottom,
			FromTop:    firstFromTop,
			Score:      e.Score,
			TieBreaker: e.TieBreaker,
			UpdatedAt:  time.Unix(int64(e.Updated), 0),
		})
		firstFromTop++
		firstFromBottom--
	}
	return res
}

func NewApiElements(e ...*app.Element) ApiElements {
	return ApiElements{In: e}
}

type ApiElements struct {
	In []*app.Element
}

func (a ApiElements) Elements() rankdb.Elements {
	e := a.In
	res := make([]rankdb.Element, 0, len(e))
	for _, in := range e {
		if in == nil {
			continue
		}
		elem := rankdb.Element{
			ID:      rankdb.ElementID(in.ID),
			Score:   in.Score,
			Payload: []byte(in.Payload),
		}
		if in.TieBreaker != nil {
			elem.TieBreaker = *in.TieBreaker
		}
		res = append(res, elem)
	}
	return res
}

type ElementIDs struct {
	In rankdb.ElementIDs
}

func (e ElementIDs) Array() []uint64 {
	v := make([]uint64, len(e.In))
	for i, id := range e.In {
		v[i] = uint64(id)
	}
	return v
}

func NewApiElementIDs(ids ...uint64) ApiElementIDs {
	return ApiElementIDs{In: ids}
}

func NewApiElementIDsString(ids ...string) (*ApiElementIDs, error) {
	v := make([]uint64, len(ids))
	for i := range ids {
		var err error
		v[i], err = strconv.ParseUint(ids[i], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing id %d: %v", i, err)
		}
	}
	return &ApiElementIDs{In: v}, nil
}

type ApiElementID struct {
	In uint64
}

func NewApiElementIDString(id string) (*ApiElementID, error) {
	v, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing id %v: %v", id, err)
	}
	return &ApiElementID{In: v}, nil
}

func (a ApiElementID) ElementID() rankdb.ElementID {
	return rankdb.ElementID(a.In)
}

type ApiElementIDs struct {
	In []uint64
}

func (a ApiElementIDs) ElementIDs() rankdb.ElementIDs {
	res := make([]rankdb.ElementID, len(a.In))
	for i, id := range a.In {
		res[i] = rankdb.ElementID(id)
	}
	return res
}
