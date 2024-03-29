// Code generated by goagen v1.4.1, DO NOT EDIT.
//
// API "rankdb": jwt TestHelpers
//
// Command:
// $ goagen
// --design=github.com/Vivino/rankdb/api/design
// --out=$(GOPATH)/src/github.com/Vivino/rankdb/api
// --version=v1.4.1

package test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"

	"github.com/Vivino/rankdb/api/app"
	"github.com/goadesign/goa"
	"github.com/goadesign/goa/goatest"
)

// JWTJWTOK runs the method JWT of the given controller with the given parameters.
// It returns the response writer so it's possible to inspect the response headers.
// If ctx is nil then context.Background() is used.
// If service is nil then a default service is created.
func JWTJWTOK(t goatest.TInterface, ctx context.Context, service *goa.Service, ctrl app.JWTController, expire int, onlyElements *string, onlyLists *string, scope string) http.ResponseWriter {
	// Setup service
	var (
		logBuf bytes.Buffer

		respSetter goatest.ResponseSetterFunc = func(r interface{}) {}
	)
	if service == nil {
		service = goatest.Service(&logBuf, respSetter)
	} else {
		logger := log.New(&logBuf, "", log.Ltime)
		service.WithLogger(goa.NewLogger(logger))
		newEncoder := func(io.Writer) goa.Encoder { return respSetter }
		service.Encoder = goa.NewHTTPEncoder() // Make sure the code ends up using this decoder
		service.Encoder.Register(newEncoder, "*/*")
	}

	// Setup request context
	rw := httptest.NewRecorder()
	query := url.Values{}
	{
		sliceVal := []string{strconv.Itoa(expire)}
		query["expire"] = sliceVal
	}
	if onlyElements != nil {
		sliceVal := []string{*onlyElements}
		query["only_elements"] = sliceVal
	}
	if onlyLists != nil {
		sliceVal := []string{*onlyLists}
		query["only_lists"] = sliceVal
	}
	{
		sliceVal := []string{scope}
		query["scope"] = sliceVal
	}
	u := &url.URL{
		Path:     fmt.Sprintf("/jwt"),
		RawQuery: query.Encode(),
	}
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		panic("invalid test " + err.Error()) // bug
	}
	prms := url.Values{}
	{
		sliceVal := []string{strconv.Itoa(expire)}
		prms["expire"] = sliceVal
	}
	if onlyElements != nil {
		sliceVal := []string{*onlyElements}
		prms["only_elements"] = sliceVal
	}
	if onlyLists != nil {
		sliceVal := []string{*onlyLists}
		prms["only_lists"] = sliceVal
	}
	{
		sliceVal := []string{scope}
		prms["scope"] = sliceVal
	}
	if ctx == nil {
		ctx = context.Background()
	}
	goaCtx := goa.NewContext(goa.WithAction(ctx, "JWTTest"), rw, req, prms)
	jwtCtx, _err := app.NewJWTJWTContext(goaCtx, req, service)
	if _err != nil {
		e, ok := _err.(goa.ServiceError)
		if !ok {
			panic("invalid test data " + _err.Error()) // bug
		}
		t.Errorf("unexpected parameter validation error: %+v", e)
		return nil
	}

	// Perform action
	_err = ctrl.JWT(jwtCtx)

	// Validate response
	if _err != nil {
		t.Fatalf("controller returned %+v, logs:\n%s", _err, logBuf.String())
	}
	if rw.Code != 200 {
		t.Errorf("invalid response status code: got %+v, expected 200", rw.Code)
	}

	// Return results
	return rw
}

// JWTJWTUnauthorized runs the method JWT of the given controller with the given parameters.
// It returns the response writer so it's possible to inspect the response headers and the media type struct written to the response.
// If ctx is nil then context.Background() is used.
// If service is nil then a default service is created.
func JWTJWTUnauthorized(t goatest.TInterface, ctx context.Context, service *goa.Service, ctrl app.JWTController, expire int, onlyElements *string, onlyLists *string, scope string) (http.ResponseWriter, error) {
	// Setup service
	var (
		logBuf bytes.Buffer
		resp   interface{}

		respSetter goatest.ResponseSetterFunc = func(r interface{}) { resp = r }
	)
	if service == nil {
		service = goatest.Service(&logBuf, respSetter)
	} else {
		logger := log.New(&logBuf, "", log.Ltime)
		service.WithLogger(goa.NewLogger(logger))
		newEncoder := func(io.Writer) goa.Encoder { return respSetter }
		service.Encoder = goa.NewHTTPEncoder() // Make sure the code ends up using this decoder
		service.Encoder.Register(newEncoder, "*/*")
	}

	// Setup request context
	rw := httptest.NewRecorder()
	query := url.Values{}
	{
		sliceVal := []string{strconv.Itoa(expire)}
		query["expire"] = sliceVal
	}
	if onlyElements != nil {
		sliceVal := []string{*onlyElements}
		query["only_elements"] = sliceVal
	}
	if onlyLists != nil {
		sliceVal := []string{*onlyLists}
		query["only_lists"] = sliceVal
	}
	{
		sliceVal := []string{scope}
		query["scope"] = sliceVal
	}
	u := &url.URL{
		Path:     fmt.Sprintf("/jwt"),
		RawQuery: query.Encode(),
	}
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		panic("invalid test " + err.Error()) // bug
	}
	prms := url.Values{}
	{
		sliceVal := []string{strconv.Itoa(expire)}
		prms["expire"] = sliceVal
	}
	if onlyElements != nil {
		sliceVal := []string{*onlyElements}
		prms["only_elements"] = sliceVal
	}
	if onlyLists != nil {
		sliceVal := []string{*onlyLists}
		prms["only_lists"] = sliceVal
	}
	{
		sliceVal := []string{scope}
		prms["scope"] = sliceVal
	}
	if ctx == nil {
		ctx = context.Background()
	}
	goaCtx := goa.NewContext(goa.WithAction(ctx, "JWTTest"), rw, req, prms)
	jwtCtx, _err := app.NewJWTJWTContext(goaCtx, req, service)
	if _err != nil {
		e, ok := _err.(goa.ServiceError)
		if !ok {
			panic("invalid test data " + _err.Error()) // bug
		}
		return nil, e
	}

	// Perform action
	_err = ctrl.JWT(jwtCtx)

	// Validate response
	if _err != nil {
		t.Fatalf("controller returned %+v, logs:\n%s", _err, logBuf.String())
	}
	if rw.Code != 401 {
		t.Errorf("invalid response status code: got %+v, expected 401", rw.Code)
	}
	var mt error
	if resp != nil {
		var _ok bool
		mt, _ok = resp.(error)
		if !_ok {
			t.Fatalf("invalid response media: got variable of type %T, value %+v, expected instance of error", resp, resp)
		}
	}

	// Return results
	return rw, mt
}
