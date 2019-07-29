package dynamo

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Store struct {
	db       *dynamodb.DynamoDB
	blobName string // Attribute name to store blobs in.
	id       string
}

// New creates a new instance of the DynamoDB client with a session.
// If additional configuration is needed for the client instance use the
// optional aws.Config parameter to add your extra config.
// Item size is 400K in DynamoDB. Use GetStore to get a automatically split items.
func New(p client.ConfigProvider, cfgs ...*aws.Config) (*Store, error) {
	s := Store{db: dynamodb.New(p, cfgs...), blobName: "blob", id: "id"}
	return &s, nil
}

// SetAttributeNames will set the names of the id and blob attributes to use.
// Unless changed, attribute names are "id" and "blob".
func (s *Store) SetAttributeNames(id, blob string) {
	s.id, s.blobName = id, blob
}

// GetStore will return a blobstore that will respect the 400K
// item limit of DynamoDB.
func (s *Store) GetStore() blobstore.Store {
	// The maximum item size in DynamoDB is 400 KB, which includes both attribute name binary length (UTF-8 length)
	// and attribute value lengths (again binary length). The attribute name counts towards the size limit.
	// We subtract a KB to ensure there is enough space.
	bs, err := blobstore.NewMaxSizeStore(s, 399<<10)
	if err != nil {
		panic(err)
	}
	return bs
}

// CreateSet will create a table that can be used for as a set for storing data.
func (s *Store) CreateSet(ctx context.Context, set string, provision *dynamodb.ProvisionedThroughput) error {
	in := dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{AttributeName: &s.id, AttributeType: aws.String("S")},
			{AttributeName: &s.blobName, AttributeType: aws.String("B")},
		},
		TableName:             aws.String(set),
		KeySchema:             []*dynamodb.KeySchemaElement{{AttributeName: &s.id, KeyType: aws.String("HASH")}},
		ProvisionedThroughput: provision,
	}

	_, err := s.db.CreateTableWithContext(ctx, &in)
	return err
}

func (s *Store) Get(ctx context.Context, set, key string) ([]byte, error) {
	in := dynamodb.GetItemInput{
		Key:       map[string]*dynamodb.AttributeValue{s.id: {S: aws.String(key)}},
		TableName: aws.String(set),
	}
	res, err := s.db.GetItemWithContext(ctx, &in)
	if err != nil {
		// FIXME: Filter out not found.
		return nil, err
	}
	item, ok := res.Item[s.blobName]
	if !ok {
		return nil, blobstore.ErrBlobNotFound
	}
	return item.B, nil
}

func (s *Store) Set(ctx context.Context, set, key string, val []byte) error {
	in := dynamodb.PutItemInput{
		Item:      map[string]*dynamodb.AttributeValue{s.id: {S: aws.String(key)}, s.blobName: {B: val}},
		TableName: aws.String(set),
	}
	_, err := s.db.PutItemWithContext(ctx, &in)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, set, key string) error {
	in := dynamodb.DeleteItemInput{
		Key:       map[string]*dynamodb.AttributeValue{s.id: {S: aws.String(key)}},
		TableName: aws.String(set),
	}
	_, err := s.db.DeleteItemWithContext(ctx, &in)

	// FIXME: Filter out not found.
	return err
}
