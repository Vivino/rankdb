package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/loggoa"
	"github.com/goadesign/goa"
	shutdown "github.com/klauspost/shutdown2"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type DatadogOptions struct {
	Enabled bool
}

type datadogApp struct{}

func (d *datadogApp) Enabled() bool {
	return d != nil
}

var ddApp *datadogApp
var gitcommit string = "0000000000000000000000000000000000000000"

func InitDatadog(ctx context.Context, o DatadogOptions) {
	if !o.Enabled {
		log.Info(ctx, "Datadog disabled by config")
		return
	}
	ddApp = &datadogApp{}
	log.Info(ctx, "Datadog enabled", "gitcommit", gitcommit)
	tracer.Start(
		tracer.WithServiceVersion(gitcommit),
	)
	log.Info(ctx, "Datadog tracer started")
	shutdown.ThirdFn(func() {
		// When the tracer is stopped, it will flush everything it has to the Datadog Agent before quitting.
		// Make sure this line stays in your main function.
		tracer.Stop()
		log.Info(ctx, "Datadog tracer stopped")
	})
}

// DatadogTx creates a request datadog middleware.
func DatadogTx() goa.Middleware {
	return func(h goa.Handler) goa.Handler {
		return func(ctx context.Context, rw http.ResponseWriter, req *http.Request) error {
			r := goa.ContextRequest(ctx)
			span := tracer.StartSpan("web.request",
				tracer.SpanType("web"),
				tracer.ResourceName(goa.ContextController(ctx)+"."+goa.ContextAction(ctx)),
			)

			span.SetTag("http.method", r.Method)
			span.SetTag("http.url", r.URL.Path)
			span.SetTag("source_ip", from(req))

			if len(r.Params) > 0 {
				for k, v := range r.Params {
					span.SetTag("param_"+k, `"`+strings.Join(v, `","`)+`"`)
				}
			}
			ierr := func(msg string, keyvals ...interface{}) {
				for len(keyvals) > 0 {
					key := fmt.Sprint(keyvals[0])
					keyvals = keyvals[1:]
					val := errMissingLogValue
					if len(keyvals) > 0 {
						val = fmt.Sprint(keyvals[0])
						keyvals = keyvals[1:]
					}
					span.SetTag(key, val)
				}
			}
			var ninfo int
			var mu sync.Mutex
			iinfo := func(msg string, keyvals ...interface{}) {
				mu.Lock()
				n := ninfo
				ninfo++
				mu.Unlock()
				if n < 10 {
					span.SetTag(fmt.Sprintf("info_msg_%d", n), formatMsg(msg, keyvals, false))
				}
			}
			intLogger := log.Intercept(log.Logger(ctx), iinfo, ierr)
			ctx = goa.WithLogger(ctx, loggoa.WrapGoa(intLogger))
			ctx = log.WithLogger(ctx, intLogger)
			var err error
			defer func() {
				resp := goa.ContextResponse(ctx)
				span.SetTag("http.status_code", resp.Status)
				span.SetTag("http.response.size", resp.Length)
				if err != nil {
					span.Finish(tracer.WithError(err))
				} else {
					span.Finish()
				}
			}()
			err = h(ctx, rw, req)
			return err
		}
	}
}
