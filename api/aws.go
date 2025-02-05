package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"

	"github.com/Vivino/rankdb/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

var (
	awsSession *session.Session
)

func initAws(ctx context.Context) {
	if !config.AWS.Enabled {
		log.Info(ctx, "AWS not enabled in config")
		return
	}

	awsSession = session.Must(session.NewSession(aws.NewConfig().WithRegion(config.AWS.Region)))
	log.Info(ctx, "AWS initialized")
}
