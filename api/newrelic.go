package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/loggoa"
	"github.com/goadesign/goa"
	shutdown "github.com/klauspost/shutdown2"
	newrelic "github.com/newrelic/go-agent"
)

type NewRelicOptions struct {
	AppName           string
	License           string
	RecordCustomEvent bool
	TxTracerThreshold duration
	ExcludeAttributes []string
	HostDisplayName   string
	Enabled           bool
}

type newRelic struct {
	app newrelic.Application
}

func (nr *newRelic) Enabled() bool {
	return nr != nil
}

var nrApp *newRelic

func InitNewRelic(ctx context.Context, o NewRelicOptions) {
	if !o.Enabled {
		log.Info(ctx, "New Relic agent disabled by config")
		return
	}
	nrCfg := newrelic.NewConfig(o.AppName, o.License)
	if len(o.ExcludeAttributes) > 0 {
		nrCfg.Attributes.Exclude = append(nrCfg.Attributes.Exclude, o.ExcludeAttributes...)
	}
	nrCfg.Logger = &nrLogger{l: log.Logger(ctx)}
	if o.TxTracerThreshold.Duration > 0 {
		nrCfg.TransactionTracer.Threshold.Duration = o.TxTracerThreshold.Duration
		nrCfg.TransactionTracer.Threshold.IsApdexFailing = false
	}
	if o.License == "" {
		log.Info(ctx, "New Relic license not found, agent disabled")
		if !config.Debug {
			nrApp = nil
			return
		}
		nrCfg.Enabled = false
		app, err := newrelic.NewApplication(nrCfg)
		if err != nil {
			log.Error(ctx, "Unable to run debug New Relic agent", "error", err)
			return
		}
		nrApp = &newRelic{app: app}
		shutdown.ThirdFn(func() {
			nrApp.app.Shutdown(time.Second)
		})
		return
	}
	nrCfg.License = o.License
	app, err := newrelic.NewApplication(nrCfg)
	if err != nil {
		log.Error(ctx, "Unable to run New Relic agent", "error", err)
		return
	}
	nrApp = &newRelic{app: app}
	shutdown.ThirdFn(func() {
		nrApp.app.Shutdown(time.Second)
	})
}

// NewRelicTx creates a request new relic middleware.
// If verbose is true then the middlware logs the request and response bodies.
func NewRelicTx() goa.Middleware {
	return func(h goa.Handler) goa.Handler {
		return func(ctx context.Context, rw http.ResponseWriter, req *http.Request) error {
			app := nrApp.app
			txn := app.StartTransaction(goa.ContextController(ctx)+"."+goa.ContextAction(ctx), rw, req)
			r := goa.ContextRequest(ctx)
			txn.AddAttribute("source_ip", from(req))

			if config.Debug && len(r.Header) > 0 {
				for k, v := range r.Header {
					txn.AddAttribute("header_"+k, `"`+strings.Join(v, `","`)+`"`)
				}
			}
			if len(r.Params) > 0 {
				for k, v := range r.Params {
					txn.AddAttribute("param_"+k, `"`+strings.Join(v, `","`)+`"`)
				}
			}
			if config.Debug && r.ContentLength > 0 && r.ContentLength < 1024 {
				// Not the most efficient but this is used for debugging
				js, err := json.Marshal(r.Payload)
				if err != nil {
					js = []byte("<invalid JSON>")
				}
				txn.AddAttribute("payload_json", string(js))
			}
			ierr := func(msg string, keyvals ...interface{}) {
				txn.NoticeError(errors.New(msg))
				for len(keyvals) > 0 {
					key := fmt.Sprint(keyvals[0])
					keyvals = keyvals[1:]
					val := errMissingLogValue
					if len(keyvals) > 0 {
						val = fmt.Sprint(keyvals[0])
						keyvals = keyvals[1:]
					}
					txn.AddAttribute(key, val)
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
					txn.AddAttribute(fmt.Sprintf("info_msg_%d", n), formatMsg(msg, keyvals, false))
				}
			}
			intLogger := log.Intercept(log.Logger(ctx), iinfo, ierr)
			ctx = goa.WithLogger(ctx, loggoa.WrapGoa(intLogger))
			ctx = log.WithLogger(ctx, intLogger)
			err := h(ctx, rw, req)
			defer func() {
				resp := goa.ContextResponse(ctx)
				if code := resp.ErrorCode; code != "" {
					txn.AddAttribute("error_code", code)
					txn.NoticeError(err)
				} else if resp.Status >= 500 || err != nil {
					err2 := err
					if err == nil {
						err2 = errors.New(resp.ErrorCode)
					}
					txn.NoticeError(err2)
				}
				txn.AddAttribute("response_status_code", resp.Status)
				txn.AddAttribute("response_bytes", resp.Length)
				txn.End()
			}()
			return err
		}
	}
}

const errMissingLogValue = "<missing>"

func formatMsg(msg string, keyvals []interface{}, iserror bool) string {
	n := (len(keyvals) + 1) / 2
	if len(keyvals)%2 != 0 {
		keyvals = append(keyvals, errMissingLogValue)
	}
	var fm bytes.Buffer
	lvl := "INFO"
	if iserror {
		lvl = "EROR" // Not a typo. It ensures all level strings are 4-chars long.
	}
	fm.WriteString(fmt.Sprintf("[%s] %s", lvl, msg))
	vals := make([]interface{}, n)
	offset := 0
	for i := 0; i < len(keyvals); i += 2 {
		k := keyvals[i]
		v := keyvals[i+1]
		vals[i/2+offset/2] = v
		fm.WriteString(fmt.Sprintf(" %s=%%+v", k))
	}
	return fmt.Sprintf(fm.String(), vals...)
}

// from makes a best effort to compute the request client IP.
func from(req *http.Request) string {
	if f := req.Header.Get("X-Forwarded-For"); f != "" {
		return f
	}
	f := req.RemoteAddr
	ip, _, err := net.SplitHostPort(f)
	if err != nil {
		return f
	}
	return ip
}

// nrLogger is a wrapper for our logger
type nrLogger struct {
	l log.Adapter
}

func (n *nrLogger) Error(msg string, context map[string]interface{}) {
	ctx := make([]interface{}, 0, len(context))
	for k, v := range context {
		ctx = append(ctx, k, v)
	}
	n.l.Error(msg, ctx...)
}

func (n *nrLogger) Warn(msg string, context map[string]interface{}) {
	ctx := make([]interface{}, 0, len(context))
	for k, v := range context {
		ctx = append(ctx, k, fmt.Sprint(v))
	}
	n.l.Info(msg, ctx...)
}

func (n *nrLogger) Info(msg string, context map[string]interface{}) {
	ctx := make([]interface{}, 0, len(context))
	for k, v := range context {
		ctx = append(ctx, k, fmt.Sprint(v))
	}
	n.l.Info(msg, ctx...)
}

func (n *nrLogger) Debug(msg string, context map[string]interface{}) {}

func (n *nrLogger) DebugEnabled() bool { return false }
