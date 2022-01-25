// +build tools

package tools

import (
	_ "github.com/goadesign/goa/goagen"
	_ "github.com/goreleaser/goreleaser"
	_ "github.com/tinylib/msgp"
	_ "golang.org/x/lint/golint"
	_ "golang.org/x/tools/cmd/goimports"
)
