package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/Vivino/rankdb/api/client"
	"github.com/Vivino/rankdb/api/tool/cli"
	goaclient "github.com/goadesign/goa/client"
	"github.com/spf13/cobra"
)

func main() {
	// Create command line parser
	app := &cobra.Command{
		Use:   "rankdb-cli",
		Short: `CLI client for the rankdb service`,
	}

	// Create client struct
	httpClient := newHTTPClient()
	c := client.New(goaclient.HTTPClientDoer(httpClient))

	// Register global flags
	app.PersistentFlags().StringVarP(&c.Scheme, "scheme", "s", "", "Set the requests scheme")
	app.PersistentFlags().StringVarP(&c.Host, "host", "H", "localhost:8080", "API hostname")
	app.PersistentFlags().DurationVarP(&httpClient.Timeout, "timeout", "t", time.Duration(20)*time.Second, "Set the request timeout")
	app.PersistentFlags().BoolVar(&c.Dump, "dump", false, "Dump HTTP request and response.")

	// Initialize API client
	c.UserAgent = "rankdb-cli/0"

	// Register API commands
	cli.RegisterCommands(app, c)

	// Execute!
	if err := app.Execute(); err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(-1)
	}
}

// newHTTPClient returns the HTTP client used by the API client to make requests to the service.
func newHTTPClient() *http.Client {
	// TBD: Change as needed (e.g. to use a different transport to control redirection policy or
	// disable cert validation or...)
	return http.DefaultClient
}
