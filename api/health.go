package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/Vivino/rankdb/api/app"
	"github.com/Vivino/rankdb/log"
	"github.com/goadesign/goa"
)

// HealthController implements the health resource.
type HealthController struct {
	*goa.Controller

	memoryStats   map[string]interface{}
	memoryMuStats sync.Mutex
}

// NewHealthController creates a health controller.
func NewHealthController(service *goa.Service) *HealthController {
	c := &HealthController{Controller: service.NewController("HealthController")}
	go c.memoryReader()

	return c
}

// Health runs the health action.
func (c *HealthController) Health(ctx *app.HealthHealthContext) error {
	// HealthController_Health: start_implement
	var res app.RankdbSysinfo
	c.memoryMuStats.Lock()
	res.Memory = c.memoryStats
	c.memoryMuStats.Unlock()
	res.ElementCache = make(map[string]interface{})
	if cache != nil {
		type cacheLen interface {
			Len() int
		}
		if l, ok := cache.(cacheLen); ok {
			res.ElementCache["current_entries"] = l.Len()
		}
		res.ElementCache["max_entries"] = config.CacheEntries
		res.ElementCache["cache_type"] = fmt.Sprintf("%T", cache)
	}
	if lazySaver != nil {
		stats := lazySaver.Stats()
		b, err := json.Marshal(stats)
		if err == nil {
			res.LazySaver = b
		}
	}
	return ctx.OK(&res)
}

func (c *HealthController) memoryReader() {
	ctx := c.Context
	ticker := time.Tick(time.Minute)
	for range ticker {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		j, err := json.MarshalIndent(mem, "", "\t")
		if err != nil {
			log.Error(ctx, "unable to marshal memstats", "error", err.Error())
		}
		var m map[string]interface{}
		err = json.Unmarshal(j, &m)
		if err != nil {
			log.Error(ctx, "unable to unmarshal memstats", "error", err.Error())
		}
		c.memoryMuStats.Lock()
		c.memoryStats = m
		c.memoryMuStats.Unlock()
	}
}

// Root allows to ping the server.
func (c *HealthController) Root(ctx *app.RootHealthContext) error {
	return ctx.OK([]byte(`{"running": true}`))
}
