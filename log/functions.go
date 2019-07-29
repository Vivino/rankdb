package log

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
)

// caller returns the funtion call line at the specified depth
// as "dir/file.go:n:
func caller(depth int) string {
	_, file, line, ok := runtime.Caller(depth + 1)
	if !ok {
		file = "???"
		line = 0
	}
	return fmt.Sprintf("%s:%d", shortenFile(file), line)
}

func fnName(depth int) (string, bool) {
	pc := make([]uintptr, 1) // at least 1 entry needed
	runtime.Callers(2+depth, pc)
	ffp := runtime.FuncForPC(pc[0])
	fnName := "unknown"
	if ffp != nil {
		fnName = ffp.Name()
		li := strings.LastIndex(fnName, "/")
		if li > 0 && li < len(fnName)-1 {
			fnName = fnName[li+1:]
		}
		return fnName, true
	}
	return fnName, false
}

// shortenFile returns the file name of an absolute file path.
func shortenFile(file string) string {
	_, f := filepath.Split(file)
	return f
}
