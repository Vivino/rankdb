package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"

	"github.com/Vivino/rankdb/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
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

	cfg := aws.NewConfig().WithRegion(config.AWS.Region)
	if config.AWS.S3Endpoint != "" {
		cfg = cfg.WithEndpoint(config.AWS.S3Endpoint)
	}

	if config.AWS.AccessKey != "" && config.AWS.SecretKey != "" {
		providers := []credentials.Provider{
			&credentials.StaticProvider{
				Value: credentials.Value{
					AccessKeyID:     config.AWS.AccessKey,
					SecretAccessKey: config.AWS.SecretKey,
					SessionToken:    "",
				},
			},
			&ec2rolecreds.EC2RoleProvider{
				Client: ec2metadata.New(session.Must(session.NewSession())),
			},
			&credentials.EnvProvider{},
		}

		creds := credentials.NewChainCredentials(providers)
		_, err := creds.Get()
		if err != nil {
			log.Error(ctx, "AWS not initialized", "error", err.Error())
			return
		}
		cfg = cfg.WithCredentials(creds)
	}

	awsSession = session.Must(session.NewSession(cfg))
	log.Info(ctx, "AWS initialized")
}
