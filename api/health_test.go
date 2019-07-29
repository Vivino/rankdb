package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"net/http"
	"testing"

	"github.com/Vivino/rankdb/api/client"
)

func TestHealthController_Root(t *testing.T) {
	ctx := testCtx(t)
	t.Parallel()
	resp, err := tClient.RootHealth(ctx, client.RootHealthPath())
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientManage.RootHealth(ctx, client.RootHealthPath())
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
}

func TestHealthController_Health(t *testing.T) {
	ctx := testCtx(t)
	t.Parallel()
	resp, err := tClient.HealthHealth(ctx, client.HealthHealthPath())
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientManage.HealthHealth(ctx, client.HealthHealthPath())
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	si, err := tClient.DecodeRankdbSysinfo(resp)
	fatalErr(t, err)
	if len(si.ElementCache) == 0 {
		t.Error("no element cache")
	}
}
