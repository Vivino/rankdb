package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import "errors"

var (

	// Enable per-step sanity checks.
	sanityChecks = false

	// ErrNotImplemented is returned if the functionality is not yet implemented.
	ErrNotImplemented = errors.New("not implemented")

	// ErrNotFound is returned if the requested item could not be found.
	ErrNotFound = errors.New("not found")

	// ErrVersionMismatch is returned when unable to decode a content because of version mismatch.
	ErrVersionMismatch = errors.New("version mismatch")

	// ErrOffsetOutOfBounds is returned if requested offset is outside list bounds.
	ErrOffsetOutOfBounds = errors.New("offset out of list bounds")

	// ErrEmptySet is returned if a set was empty.
	ErrEmptySet = errors.New("set cannot be empty")
)
