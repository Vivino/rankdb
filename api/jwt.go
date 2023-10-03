package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/api/app"
	"github.com/Vivino/rankdb/log"
	"github.com/goadesign/goa"
	goajwt "github.com/goadesign/goa/middleware/security/jwt"
	"github.com/golang-jwt/jwt/v4"
)

// Change to enable JWT for all api endpoints.
var (
	enableJWT         = false
	enableJWTCreation = false
)

// NewJWTMiddleware creates a middleware that checks for the presence of a JWT Authorization header
// and validates its content. A real app would probably use goa's JWT security middleware instead.
//
// Note: the code below assumes the example is compiled against the master branch of goa.
// If compiling against goa v1 the call to jwt.New needs to be:
//
//	middleware := jwt.New(keys, ForceFail(), app.NewJWTSecurity())
func NewJWTMiddleware() (goa.Middleware, error) {
	// Skip all JWT if needed.
	if !enableJWT {
		return func(nextHandler goa.Handler) goa.Handler {
			return func(ctx context.Context, rw http.ResponseWriter, req *http.Request) error {
				return nextHandler(ctx, rw, req)
			}
		}, nil
	}
	keys, err := LoadJWTPublicKeys()
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, errors.New("no public keys found")
	}
	return goajwt.New(keys, nil, app.NewJWTSecurity()), nil
}

// LoadJWTPublicKeys loads PEM encoded RSA public keys used to validate and decrypt the JWT.
func LoadJWTPublicKeys() ([]*rsa.PublicKey, error) {
	keyFiles, err := filepath.Glob(filepath.Join(config.JwtKeyPath, "*.pub"))
	if err != nil {
		return nil, err
	}
	keys := make([]*rsa.PublicKey, len(keyFiles))
	for i, keyFile := range keyFiles {
		pem, err := ioutil.ReadFile(keyFile)
		if err != nil {
			return nil, err
		}
		key, err := jwt.ParseRSAPublicKeyFromPEM([]byte(pem))
		if err != nil {
			return nil, fmt.Errorf("failed to load key %s: %s", keyFile, err)
		}
		keys[i] = key
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("couldn't load public keys for JWT security")
	}

	return keys, nil
}

func HasAccessToList(ctx context.Context, ids ...rankdb.ListID) error {
	token := goajwt.ContextJWT(ctx)
	if token == nil {
		return nil
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return goajwt.ErrJWTError("unsupported claims shape")
	}
	if val, ok := claims["only_lists"].(string); ok {
		// Check all ids
		for _, id := range ids {
			ok := false
			lists := strings.Split(val, ",")
			for i := range lists {
				if strings.TrimSpace(lists[i]) == string(id) {
					ok = true
					break
				}
			}
			if !ok {
				return goajwt.ErrJWTError("Access not granted to list " + string(id))
			}
		}
	}
	return nil
}

// HasAccessToElement returns true
// Not optimized for big lists of IDs.
func HasAccessToElement(ctx context.Context, ids ...rankdb.ElementID) error {
	token := goajwt.ContextJWT(ctx)
	if token == nil {
		return nil
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return goajwt.ErrJWTError("unsupported claims shape")
	}
	if val, ok := claims["only_elements"].(string); ok {
		lists := strings.Split(val, ",")
		for _, id := range ids {
			ok := false
			for i := range lists {
				v, err := strconv.ParseUint(strings.TrimSpace(lists[i]), 10, 64)
				if err == nil && v == uint64(id) {
					ok = true
					break
				}
			}
			if !ok {
				return goajwt.ErrJWTError(fmt.Sprint("Access not granted to element ", id))
			}
		}
	}
	return nil
}

// JWTController implements the jwt resource.
type JWTController struct {
	*goa.Controller
	privateKey *rsa.PrivateKey
}

// NewJWTController creates a jwt controller.
func NewJWTController(service *goa.Service) (*JWTController, error) {
	if !enableJWTCreation {
		return &JWTController{
			Controller: service.NewController("JWTController"),
		}, nil
	}
	log.Info(context.Background(), "Warning: JWT Key creation enabled.")
	b, err := ioutil.ReadFile(filepath.Join(config.JwtKeyPath, "jwt.key"))
	if err != nil {
		return nil, err
	}
	privKey, err := jwt.ParseRSAPrivateKeyFromPEM(b)
	if err != nil {
		return nil, fmt.Errorf("jwt: failed to load private key: %s", err) // bug
	}
	return &JWTController{
		Controller: service.NewController("JWTController"),
		privateKey: privKey,
	}, nil
}

// JWT runs the jwt action.
func (c *JWTController) JWT(ctx *app.JWTJWTContext) error {
	// JWTController_JWT: start_implement
	if c.privateKey == nil || !enableJWTCreation {
		return ctx.Unauthorized(errors.New("jwt signing not enabled"))
	}
	// Generate JWT
	token := jwt.New(jwt.SigningMethodRS512)
	expire := time.Now().Add(time.Minute * time.Duration(ctx.Expire)).Unix()
	claims := jwt.MapClaims{
		"iss":    "RankDB",             // who creates the token and signs it
		"exp":    expire,               // time when the token will expire (10 minutes from now)
		"jti":    rankdb.RandString(8), // a unique identifier for the token
		"iat":    time.Now().Unix(),    // when the token was issued/created (now)
		"nbf":    2,                    // time before which the token is not yet valid (2 minutes ago)
		"scopes": ctx.Scope,            // token scope - not a standard claim
	}
	if ctx.OnlyLists != nil {
		claims["only_lists"] = *ctx.OnlyLists
	}
	if ctx.OnlyElements != nil {
		claims["only_elements"] = *ctx.OnlyElements
	}
	token.Claims = claims
	signedToken, err := token.SignedString(c.privateKey)
	if err != nil {
		return fmt.Errorf("failed to sign token: %s", err) // internal error
	}

	// Set auth header for client retrieval
	ctx.ResponseData.Header().Set("Authorization", "Bearer "+signedToken)

	// JWTController_JWT: end_implement
	return ctx.OK([]byte(signedToken))
}
