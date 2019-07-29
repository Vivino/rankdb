package reader

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"io"
)

// Reader allows an io.ReadCloser to be used as a backup destination.
type Reader struct {
	r *io.PipeReader
	w *io.PipeWriter
}

// NewReader creates a new Writer -> Reader wrapper that can be used for backups.
func NewReader() *Reader {
	b := Reader{}
	b.r, b.w = io.Pipe()
	return &b
}

// Output returns the reader that must be consumed to receive the entire backup.
func (r *Reader) Output() io.ReadCloser {
	return r.r
}

// Save returns the writer for saving a backup.
func (r *Reader) Save(ctx context.Context) (io.WriteCloser, error) {
	return r.w, nil
}
