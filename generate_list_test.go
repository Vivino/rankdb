package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/Vivino/rankdb/api/app"
	"github.com/ugorji/go/codec"
)

func TestGenerateList(t *testing.T) {
	t.SkipNow()
	var (
		mh       codec.MsgpackHandle
		h        = &mh // or mh to use msgpack
		elements = int(20 * 1e6)
	)
	f, err := os.Create(fmt.Sprintf("../%d-elements.msgp", elements))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	lst := app.RankList{
		ID:        fmt.Sprintf("list-%d-elements", elements),
		LoadIndex: false,
		Metadata: map[string]string{
			"country": "dk",
			"game":    "2",
		},
		MergeSize: 500,
		SplitSize: 2000,
		Populate:  make([]*app.Element, elements),
		Set:       "storage-set",
	}
	rng := rand.New(rand.NewSource(0x1984))
	w := bufio.NewWriter(f)
	defer w.Flush()

	for i := 0; i < elements; i++ {
		tb := rng.Uint32()
		lst.Populate[i] = &app.Element{
			ID:         rng.Uint64(),
			Score:      uint64(elements - i),
			TieBreaker: &tb,
		}
	}
	enc := codec.NewEncoder(w, h)
	err = enc.Encode(lst)
	if err != nil {
		t.Fatal(err)
	}
}
