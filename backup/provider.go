package backup

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"errors"
	"io"
	"net/http"
)

// Saver provides a destination for saving a backup.
type Saver interface {
	Save(ctx context.Context) (io.WriteCloser, error)
}

// Loader provides a source for loading a backup set.
type Loader interface {
	Load(ctx context.Context) (io.ReadCloser, error)
}

// WrapReader will wrap an io.Reader so it can be used for restoring a backup.
type WrapReader struct {
	io.ReadCloser
}

// Load will return the wrapped ReadCloser.
func (h WrapReader) Load(ctx context.Context) (io.ReadCloser, error) {
	return h.ReadCloser, nil
}

// HTTPLoader provides a source for restoring a backup from a URL.
// The contained http client is used for the request.
type HTTPLoader struct {
	URL    string
	Client *http.Client
}

// Load a backup set from the URL in the http loader.
// Returns the response body as a stream.
func (h HTTPLoader) Load(ctx context.Context) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", h.URL, nil)
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)
	resp, err := h.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode > 299 {
		return nil, errors.New(resp.Status)
	}
	return resp.Body, nil
}
