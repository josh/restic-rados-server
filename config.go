package main

import (
	"fmt"
	"slices"
)

type BlobType string

const (
	BlobTypeConfig    BlobType = "config"
	BlobTypeKeys      BlobType = "keys"
	BlobTypeLocks     BlobType = "locks"
	BlobTypeSnapshots BlobType = "snapshots"
	BlobTypeData      BlobType = "data"
	BlobTypeIndex     BlobType = "index"
)

var AllBlobTypes = []BlobType{
	BlobTypeConfig, BlobTypeKeys, BlobTypeLocks,
	BlobTypeSnapshots, BlobTypeData, BlobTypeIndex,
}

type ServerConfigPools struct {
	Config    string `json:"config"`
	Keys      string `json:"keys"`
	Locks     string `json:"locks"`
	Snapshots string `json:"snapshots"`
	Data      string `json:"data"`
	Index     string `json:"index"`
}

type ServerConfig struct {
	Version        int               `json:"version"`
	Pools          ServerConfigPools `json:"pools"`
	StriperEnabled bool              `json:"striper_enabled"`
}

type CephConfig struct {
	ConfigPoolName string
	KeyringPath    string
	ClientID       string
	CephConf       string
	MaxObjectSize  int64
}

type PoolConfig struct {
	Name      string
	Alignment uint64
	Striped   bool
}

func (p *ServerConfigPools) getPoolForType(bt BlobType) string {
	switch bt {
	case BlobTypeConfig:
		return p.Config
	case BlobTypeKeys:
		return p.Keys
	case BlobTypeLocks:
		return p.Locks
	case BlobTypeSnapshots:
		return p.Snapshots
	case BlobTypeData:
		return p.Data
	case BlobTypeIndex:
		return p.Index
	default:
		panic(fmt.Sprintf("unknown blob type: %q", bt))
	}
}

func (p *ServerConfigPools) UniquePools() []string {
	poolSet := make(map[string]struct{})
	if p.Config != "" {
		poolSet[p.Config] = struct{}{}
	}
	if p.Keys != "" {
		poolSet[p.Keys] = struct{}{}
	}
	if p.Locks != "" {
		poolSet[p.Locks] = struct{}{}
	}
	if p.Snapshots != "" {
		poolSet[p.Snapshots] = struct{}{}
	}
	if p.Data != "" {
		poolSet[p.Data] = struct{}{}
	}
	if p.Index != "" {
		poolSet[p.Index] = struct{}{}
	}

	pools := make([]string, 0, len(poolSet))
	for pool := range poolSet {
		pools = append(pools, pool)
	}
	slices.Sort(pools)
	return pools
}
