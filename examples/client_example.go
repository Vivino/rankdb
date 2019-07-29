//+build ignore

package doc

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/PuerkitoBio/rehttp"
	"github.com/Vivino/rankdb/api/client"
	"github.com/goadesign/goa"
	goaclient "github.com/goadesign/goa/client"
)

// Initialize a client to use a specific host and with reasonable retry mechanics.
func Client(host string) *client.Client {
	// http client to use as base.
	c := *http.DefaultClient
	const maxIdle = 10

	// Transport to use.
	trans := &http.Transport{
		MaxIdleConnsPerHost: maxIdle,
		MaxIdleConns:        maxIdle,
		IdleConnTimeout:     time.Minute,
	}

	tr := rehttp.NewTransport(
		trans,
		rehttp.RetryAll(
			rehttp.RetryAny(
				rehttp.RetryFn(func(a rehttp.Attempt) bool {
					// This only logs. The actual retry conditions are below.
					if a.Response != nil && a.Response.StatusCode > 500 {
						log.Println("Retry attempt.", "status:", a.Response.StatusCode,
							"retry_index:", a.Index,
							"url:", a.Request.URL.String())
					}
					if a.Error != nil {
						switch a.Error {
						case io.EOF, context.DeadlineExceeded, context.Canceled:
							// io.EOF is returned when server closes a reused connection.
							// See https://github.com/golang/go/issues/22158
						default:
							log.Println("Reply error.",
								"retry_index:", a.Index,
								"url:", a.Request.URL.String(),
								"error:", a.Error.Error(),
								"error_type:", fmt.Sprintf("%T", a.Error))
						}
					}
					return false
				}),
				rehttp.RetryStatusInterval(500, 1000),
				rehttp.RetryIsErr(func(err error) bool {
					switch err {
					case nil:
						return false
					case context.DeadlineExceeded, context.Canceled:
						return false
					default:
						switch err.Error() {
						case "net/http: request canceled":
							return false
						}
						return true
					}
				}),
			),
			rehttp.RetryMaxRetries(20),
		),
		rehttp.ExpJitterDelay(200*time.Millisecond, 5*time.Second), // wait up to 5s between retries
	)

	tr.PreventRetryWithBody = false
	c.Transport = tr
	c.Timeout = 5 * time.Minute
	db := client.New(goaclient.HTTPClientDoer(&c))
	db.Host = host
	return db
}

// DecodeError will decode an error response.
func DecodeError(resp *http.Response) error {
	defer resp.Body.Close()
	switch resp.Header.Get("Content-Type") {
	case goa.ErrorMediaIdentifier:
		r, err := DB.DecodeErrorResponse(resp)
		if err != nil {
			log.Println("Unable to decode result", "status_code", resp.StatusCode, "decode_error", err)
			return err
		}
		return r
	default:
		b, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("api returned: %s (%d): %s", resp.Status, resp.StatusCode, string(b))
	}
}
