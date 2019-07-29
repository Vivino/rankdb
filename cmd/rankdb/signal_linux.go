package main

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import "syscall"

func init() {
	usr2Signal = syscall.SIGUSR2
}
