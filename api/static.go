package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"github.com/goadesign/goa"
)

// StaticController implements the static resource.
type StaticController struct {
	*goa.Controller
}

// NewStaticController creates a static controller.
func NewStaticController(service *goa.Service) *StaticController {
	return &StaticController{Controller: service.NewController("StaticController")}
}
