package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import "github.com/goadesign/goa"

var (
	// ErrConflict is returned when a conflict is detected.
	ErrConflict = goa.NewErrorClass("conflict", 409)
)
