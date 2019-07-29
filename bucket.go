package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

// A Bucket contains a number of tokens.
// A bucket can safely be concurrently accessed.
// A user is free to access the channel directly.
type Bucket chan struct{}

// NewBucket creates a bucket with a given number of tokens.
func NewBucket(tokens int) Bucket {
	b := make(chan struct{}, tokens)
	for i := 0; i < tokens; i++ {
		b <- struct{}{}
	}
	return Bucket(b)
}

// A Token from a bucket.
// Can be put buck using the Release function.
type Token struct {
	b Bucket
}

// Get will return a token.
// The order in which tokens are handed out is random.
func (b Bucket) Get() Token {
	<-b
	return Token{b: b}
}

// Release a token and put it back in the bucket.
func (t Token) Release() {
	t.b <- struct{}{}
}
