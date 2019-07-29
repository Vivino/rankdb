package file

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"io"
	"os"
)

type File struct {
	Name string
}

func New(name string) *File {
	return &File{Name: name}
}

func (f *File) Save(ctx context.Context) (io.WriteCloser, error) {
	return os.Create(f.Name)
}

func (f *File) Load(ctx context.Context) (io.ReadCloser, error) {
	return os.Open(f.Name)
}
