package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	go func() {
		<-time.After(200 * time.Second)
		fmt.Println("200 second timeout... Goroutines:")
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		os.Exit(1)
	}()
	os.Exit(m.Run())
}
