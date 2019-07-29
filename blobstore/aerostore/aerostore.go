package aerostore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/log"
	as "github.com/aerospike/aerospike-client-go"
	astypes "github.com/aerospike/aerospike-client-go/types"
)

// AeroStore provides Aerospike storage.
// It is highly recommended to include a MaxSizeStore prior to this
// matching maximum storage size.
type AeroStore struct {
	WritePolicy *as.WritePolicy
	BasePolicy  *as.BasePolicy

	c  *as.Client
	ns string
}

// New creates a storage with the supplied namespace.
// Multiple hosts can be specified separated by ','.
// If no port is given on hosts, port 3000 is assumed.
// It is highly recommended to include a MaxSizeStore prior to this
// matching maximum storage size.
func New(namespace, hosts string) (*AeroStore, error) {
	var h []*as.Host
	for _, hwp := range parseHosts(hosts, 3000) {
		h = append(h, as.NewHost(hwp.Name, hwp.Port))
	}
	if len(h) == 0 {
		return nil, fmt.Errorf("no hosts specified")
	}
	cl, err := as.NewClientWithPolicyAndHost(nil, h...)
	if err != nil {
		return nil, fmt.Errorf("Aerospike client initialization failed: %s", err)
	}
	defaultWritePolicy := as.NewWritePolicy(0, 0)
	defaultWritePolicy.CommitLevel = as.COMMIT_MASTER
	defaultWritePolicy.TotalTimeout = time.Minute

	return &AeroStore{
		WritePolicy: defaultWritePolicy,
		BasePolicy:  as.NewPolicy(),
		c:           cl,
		ns:          namespace,
	}, nil
}

func (a *AeroStore) Get(ctx context.Context, set, key string) ([]byte, error) {
	k, err := as.NewKey(a.ns, set, key)
	if err != nil {
		log.Error(ctx, err.Error())
		return nil, err
	}

	record, err := a.c.Get(a.BasePolicy, k)
	if err != nil {
		if err == astypes.ErrKeyNotFound {
			err = blobstore.ErrBlobNotFound
		}
		return nil, err
	}
	if record == nil || record.Bins == nil {
		return nil, blobstore.ErrBlobNotFound
	}
	bin, ok := record.Bins["blob"]
	if !ok {
		return nil, blobstore.ErrBlobNotFound
	}
	res, ok := bin.([]byte)
	if !ok {
		err := fmt.Errorf("unexpected type: %T", bin)
		log.Error(ctx, err.Error())
		return nil, err
	}
	return res, nil
}

func (a *AeroStore) Delete(ctx context.Context, set, key string) error {
	k, err := as.NewKey(a.ns, set, key)
	if err != nil {
		log.Error(ctx, err.Error())
		return err
	}
	_, err = a.c.Delete(a.WritePolicy, k)
	return err
}

func (a *AeroStore) Set(ctx context.Context, set, key string, val []byte) error {
	if len(val) > 1<<20 {
		return blobstore.ErrBlobTooBig
	}
	k, err := as.NewKey(a.ns, set, key)
	if err != nil {
		return err
	}
	p := *a.WritePolicy
	p.RecordExistsAction = as.UPDATE
	return a.c.Put(&p, k, as.BinMap{"blob": val})
}

// Host represents a host name and port.
type host struct {
	Name string
	Port int
}

// parseHosts takes a single string with comma seperated host:port pairs,
// where the port is optional and returns a slice of Hosts with values extracted.
func parseHosts(hosts string, defaultPort int) []host {
	var h []host

	hostWithPort := strings.Split(hosts, ",")
	for _, hwp := range hostWithPort {
		hap := strings.Split(hwp, ":")
		name := strings.TrimSpace(hap[0])

		if len(hap) > 1 {
			port, _ := strconv.Atoi(hap[1])
			if port > 0 {
				h = append(h, host{name, port})
				continue
			}
		}
		h = append(h, host{name, defaultPort})
	}

	return h
}
