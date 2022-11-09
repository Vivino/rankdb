package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/Vivino/rankdb/log"
)

// Assume this time resolution of OS.
// 100 * time.Microsecond also ensure that all of the int64 time is used.
const timeResolution = 100 * time.Microsecond

// RandString returns a random string. x Random bytes + 8 bytes derived from current time.
// The length is x + 8 characters (same in bytes).
// The returned string has values 0-9,a-z,A-Z.
// The number of random bytes is prefixed to help avoid collisions
// and make strings sort with random distribution.
// Default number of random bytes is 8, giving 62^8 = 2e14 combinations for the given
// OS time resolution.
// Note that creation time is leaked.
func RandString(numRnd ...int) string {
	const (
		numDigits  = 10
		numLetters = 26
		variants   = 2*numLetters + numDigits
	)

	var numRandom = 8

	if len(numRnd) > 0 {
		numRandom = numRnd[0]
	}
	t := time.Now().UnixNano() / int64(timeResolution)
	var rnd = make([]byte, numRandom+8)

	_, err := rand.Read(rnd[:numRandom])
	if err != nil {
		panic("could not read random data:" + err.Error())
	}
	// Add time as last part.
	for i := range rnd[numRandom:] {
		rnd[numRandom+i] = byte(t % variants)
		t /= variants
	}
	out := make([]byte, len(rnd))
	for i, r := range rnd[:] {
		m := int(r) % variants
		switch {
		case m < numDigits:
			out[i] = byte('0' + m)
		case m < numDigits+numLetters:
			out[i] = byte('A' + m - numDigits)
		default:
			out[i] = byte('a' + m - numDigits - numLetters)
		}
	}
	return string(out)
}

// waitErr will wait for the supplied WaitGroup to finish.
// Meanwhile it will collect any errors on the supplied error channel.
// When the WaitGroup is finished the error channel is closed.
// If the context is cancelled all remaining errors are collected.
// These errors are ignored and only the context error is returned.
func waitErr(ctx context.Context, errch chan error, wg *sync.WaitGroup) error {
	go func() {
		wg.Wait()
		close(errch)
	}()
	var errs []error
finish:
	for {
		select {
		case <-ctx.Done():
			// Flush errors and return.
			for range errch {
			}
			return ctx.Err()
		case err, ok := <-errch:
			if !ok {
				break finish
			}
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		var logerr = make([]interface{}, 0, len(errs)*2)
		for i := range errs {
			logerr = append(logerr, fmt.Sprintf("err_%d", i), errs[i].Error())
		}
		log.Info(ctx, "All errors", logerr...)
		return fmt.Errorf("%d errors, first was: (%T):%s", len(errs), errs[0], errs[0])
	}
	return nil
}
