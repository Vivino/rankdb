package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/api/client"
	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/loggoa"
	"github.com/Vivino/rankdb/log/testlogger"
	jwtgo "github.com/dgrijalva/jwt-go"
	"github.com/goadesign/goa"
	goaclient "github.com/goadesign/goa/client"
	goalogrus "github.com/goadesign/goa/logging/logrus"
	shutdown "github.com/klauspost/shutdown2"
	"github.com/sirupsen/logrus"
)

const (
	// ContentTypeDefault is the recommended content type
	contentDefault = contentMsgpack

	// ContentTypeMsgpack specifies a messagepack encoding.
	// Compact and fast.
	contentMsgpack = "application/msgpack"

	// ContentTypeJSON forces JSON as transport protocol.
	contentJSON = "application/json"
)

var (
	tClient       *client.Client
	tClientRead   *client.Client
	tClientUpdate *client.Client
	tClientDelete *client.Client
	tClientManage *client.Client
)

func TestMain(m *testing.M) {
	lr := logrus.New()
	lr.Formatter = &logrus.TextFormatter{DisableColors: true}
	logger := goalogrus.New(lr)
	ctx := log.WithLogger(context.Background(), loggoa.Wrap(logger))

	conf, err := os.Open("../conf/conf.test.toml")
	exitOnFailure(err)
	err = StartServer(ctx, conf, lr)
	exitOnFailure(err)
	err = conf.Close()
	exitOnFailure(err)
	enableJWTCreation = true
	go StartServices(logger, ctx, err)
	<-listening

	// Initialize clients
	initClient(ctx, &tClient, "")
	initClient(ctx, &tClientRead, "api:read")
	initClient(ctx, &tClientUpdate, "api:update")
	initClient(ctx, &tClientDelete, "api:delete")
	initClient(ctx, &tClientManage, "api:manage")

	shutdown.Exit(m.Run())
}

func initClient(ctx context.Context, dstClient **client.Client, scope string) {
	cl := client.New(goaclient.HTTPClientDoer(http.DefaultClient))
	*dstClient = cl
	cl.Host = listenAddr.String()
	if scope == "" {
		return
	}

	// Load private key
	b, err := ioutil.ReadFile(filepath.Join(config.JwtKeyPath, "jwt.key"))
	exitOnFailure(err)
	privKey, err := jwtgo.ParseRSAPrivateKeyFromPEM(b)
	exitOnFailure(err)
	if privKey == nil {
		exitOnFailure(errors.New("private key not found"))
	}

	// Generate JWT
	token := jwtgo.New(jwtgo.SigningMethodRS512)
	expire := time.Now().Add(time.Hour).Unix()
	claims := jwtgo.MapClaims{
		"iss":    "RankDB",             // who creates the token and signs it
		"exp":    expire,               // time when the token will expire (10 minutes from now)
		"jti":    rankdb.RandString(8), // a unique identifier for the token
		"iat":    time.Now().Unix(),    // when the token was issued/created (now)
		"nbf":    2,                    // time before which the token is not yet valid (2 minutes ago)
		"scopes": scope,                // token scope - not a standard claim
	}
	token.Claims = claims
	signedToken, err := token.SignedString(privKey)

	cl.JWTSigner = &goaclient.JWTSigner{
		TokenSource: &goaclient.StaticTokenSource{
			StaticToken: &goaclient.StaticToken{
				Value: signedToken,
				Type:  "Bearer",
			},
		},
	}
}

func testCtx(t *testing.T) context.Context {
	return log.WithLogger(context.Background(), testlogger.New(t))
}

func fatalErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// decodeError will decode an error response.
func decodeError(resp *http.Response) error {
	defer resp.Body.Close()
	switch resp.Header.Get("Content-Type") {
	case goa.ErrorMediaIdentifier:
		r, err := tClient.DecodeErrorResponse(resp)
		if err != nil {
			return err
		}
		return r
	default:
		b, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 10*1024))
		return fmt.Errorf("rankdb api returned: %s (%d): %s", resp.Status, resp.StatusCode, string(b))
	}
}

func expectCode(t *testing.T, resp *http.Response, want int) func() {
	t.Helper()
	if resp.StatusCode != want {
		t.Fatal("response code", resp.StatusCode, "!=", want, "msg:", decodeError(resp))
	}
	return func() {
		if resp.Body != nil {
			_, _ = io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}

func intp(v int) *int {
	return &v
}

func uint32p(v uint32) *uint32 {
	return &v
}

func boolp(b bool) *bool {
	return &b
}

func stringp(b string) *string {
	return &b
}

func idToString(id uint64) *string {
	s := strconv.FormatUint(id, 10)
	return &s
}

var clientElement1 = client.Element{
	ID:         100,
	Payload:    json.RawMessage(`{"another":"value"}`),
	Score:      300,
	TieBreaker: uint32p(43),
}

var clientRankdbElement = func(list string, updated time.Time) *client.RankdbElement {
	return &client.RankdbElement{
		FromBottom: 0,
		FromTop:    0,
		ID:         100,
		ListID:     list,
		Payload:    json.RawMessage(`{"another":"value"}`),
		Score:      300,
		TieBreaker: 43,
		UpdatedAt:  updated,
	}
}

// randElements will generate n random elements.
// All elements will have ID and score above 10000.
func randElements(n int, seed ...int64) []*client.Element {
	if n == 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(0x1337beefc0cac01a))
	if len(seed) > 0 {
		rng = rand.New(rand.NewSource(seed[0]))
	}
	res := make([]*client.Element, n)
	for i := range res {
		res[i] = &client.Element{
			Score:      uint64(10000 + rng.Uint32()),
			TieBreaker: uint32p(rng.Uint32()),
			ID:         uint64(rng.Uint32() + 10000),
			Payload:    []byte(`{"value":"` + rankdb.RandString(5) + `", "type": "user-list"}`),
		}
	}
	return res
}

// createList will generate a list with n random elements.
// All elements will have ID and score above 10000.
func createList(t *testing.T, id string, elements int) {
	t.Helper()
	r := client.RankList{
		ID:        id,
		LoadIndex: true,
		Metadata:  map[string]string{"test": "value"},
		Populate:  randElements(elements),
		Set:       "test-set",
		MergeSize: 1000,
		SplitSize: 2000,
	}
	// Create a list with proper credentials
	resp, err := tClientManage.CreateLists(context.Background(), client.CreateListsPath(), &r, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	_, err = tClient.DecodeRankdbRanklistFull(resp)
	fatalErr(t, err)
}

func customTokenClient(ctx context.Context, t *testing.T, scope string, onlyElements *string, onlyLists *string, expire *int) *client.Client {
	t.Helper()
	resp, err := tClientManage.JWTJWT(ctx, client.JWTJWTPath(), scope, expire, onlyElements, onlyLists)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatal("Unexpected response status:", resp.Status)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("token: ", string(b))

	cl := client.New(goaclient.HTTPClientDoer(http.DefaultClient))
	cl.Host = listenAddr.String()
	cl.JWTSigner = &goaclient.JWTSigner{
		TokenSource: &goaclient.StaticTokenSource{
			StaticToken: &goaclient.StaticToken{
				Value: string(b),
				Type:  "Bearer",
			},
		},
	}
	return cl
}
