package server

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/Vivino/rankdb/api/client"
	"github.com/Vivino/rankdb/log"
	"github.com/goadesign/goa"
)

// NewRankDB will provide a backup destination that is another server.
// Currently lists are always overwritten on the destintion.
func NewRankDB(ctx context.Context, host string) (*RankDB, error) {
	log.Info(context.Background(), "Created pipe for writing", "host", host)
	c := client.New(nil)
	c.Host = host
	resp, err := c.HealthHealth(ctx, client.HealthHealthPath())
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("Could not reach destination server")
	}
	return &RankDB{c: c}, nil
}

// RankDB contains info about the destination server
type RankDB struct {
	ListIDPrefix *string
	ListIDSuffix *string

	// client to send the data to.
	c *client.Client
}

// Save will return a writer to the restoration.
// Will return an error if the server cannot be reached.
func (r *RankDB) Save(ctx context.Context) (io.WriteCloser, error) {
	req, err := r.c.NewRestoreMultilistRequest(ctx, client.RestoreMultilistPath(), nil, r.ListIDPrefix, r.ListIDSuffix, nil, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	b := bodyWriter{req: req, c: r.c}
	b.r, b.w = io.Pipe()
	log.Info(ctx, "Created pipe for writing", "full_path", b.req.URL.String())
	go b.Transfer(ctx)
	return b.w, nil
}

// bodyWriter provides transfer from the pipe writer to the body of the request.
type bodyWriter struct {
	// r will provide the http call with data for the body.
	r *io.PipeReader
	// w is the writer where data is written to from the backup job.
	w *io.PipeWriter
	// remote client.
	c *client.Client
	// request to which the body is written.
	req *http.Request
}

// Transfer the body after executing the request.
// Errors are logged, and the pipe will be closed with an error if any occurs.
func (b *bodyWriter) Transfer(ctx context.Context) {
	log.Info(ctx, "Starting tranfer to destination server", "full_path", b.req.URL.String())
	b.req.Body = b.r
	resp, err := b.c.Do(ctx, b.req)
	if err != nil {
		log.Error(ctx, "Backup transfer failed", "error", err)
		_ = b.r.CloseWithError(err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		err := decodeError(ctx, resp, b.c)
		log.Error(ctx, "Backup transfer failed", "error", err)
		_ = b.r.CloseWithError(err)
		return
	}
	log.Info(ctx, "Destination server returned ok", "full_path", b.req.URL.String())
	b.r.Close()
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
}

// decodeError will decode an error response.
func decodeError(ctx context.Context, resp *http.Response, c *client.Client) error {
	defer resp.Body.Close()
	switch resp.Header.Get("Content-Type") {
	case goa.ErrorMediaIdentifier:
		r, err := c.DecodeErrorResponse(resp)
		if err != nil {
			log.Error(ctx, "Unable to decode result", "status_code", resp.StatusCode, "decode_error", err)
			return err
		}
		return r
	default:
		b, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("destination server api returned: %s (%d): %s", resp.Status, resp.StatusCode, string(b))
	}
}
