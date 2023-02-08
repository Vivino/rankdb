package s3

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"fmt"
	"io"

	"github.com/Vivino/rankdb/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/klauspost/readahead"
)

type File struct {
	Key    string
	Bucket string
	Client client.ConfigProvider

	// These are the results of a save.
	// Must be drained if reused.
	Result    chan *s3manager.UploadOutput
	ResultErr chan error
}

func New(key, bucket string, c client.ConfigProvider) *File {
	return &File{Key: key, Client: c, Result: make(chan *s3manager.UploadOutput, 1), ResultErr: make(chan error, 1), Bucket: bucket}
}

func (f *File) Save(ctx context.Context) (io.WriteCloser, error) {
	uploader := s3manager.NewUploader(f.Client, func(u *s3manager.Uploader) {
		u.PartSize = 64 * 1024 * 1024 // 64MB per part. Default is 5MB
	})
	reader, writer := io.Pipe()
	cType := "application/octet-stream"
	acl := "bucket-owner-full-control"
	input := s3manager.UploadInput{
		ACL:                &acl,
		Bucket:             &f.Bucket,
		ContentDisposition: nil,
		ContentType:        &cType,
		Key:                &f.Key,
		Body:               reader,
	}
	go func() {
		res, err := uploader.UploadWithContext(ctx, &input)
		if err != nil {
			_ = reader.CloseWithError(err)
			log.Error(ctx, "Unable to upload data", "error", err.Error())
		}
		f.Result <- res
		f.ResultErr <- err
		log.Info(ctx, "S3 Data uploaded", "result", fmt.Sprintf("%#v", res))
	}()
	return writer, nil
}

func (f *File) Load(ctx context.Context) (io.ReadCloser, error) {
	s3svc := s3.New(f.Client)
	result, err := s3svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(f.Bucket),
		Key:    aws.String(f.Key),
	})
	if err != nil {
		return nil, err
	}
	// FIXME: Never closes result.Body.
	return readahead.NewReader(result.Body), nil
}
