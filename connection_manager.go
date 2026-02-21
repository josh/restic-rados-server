package main

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ceph/go-ceph/rados"
)

const (
	defaultMaxObjectSize int64 = 128 * 1024 * 1024
	defaultMaxWriteSize  int64 = 90 * 1024 * 1024
)

var (
	errConnectionUnavailable = errors.New("ceph connection unavailable")
	errPoolNotConfigured     = errors.New("pool not configured for blob type")
)

type ConnectionManager struct {
	mu                sync.RWMutex
	conn              *rados.Conn
	config            CephConfig
	reconnecting      bool
	lastReconnectTime time.Time
	minReconnectDelay time.Duration
	maxReconnectDelay time.Duration
	maxObjectSize     int64
	maxWriteSize      int64
	repoBlobPools     map[string]map[BlobType]*BlobPool
}

func NewConnectionManager(config CephConfig) *ConnectionManager {
	cm := &ConnectionManager{
		config:            config,
		minReconnectDelay: 1 * time.Second,
		maxReconnectDelay: 30 * time.Second,
	}

	if err := cm.connect(); err != nil {
		slog.Warn("initial ceph connection failed, will retry on first request", "error", err)
	} else {
		slog.Info("ceph connection established")
	}

	return cm
}

func (cm *ConnectionManager) connect() error {
	var conn *rados.Conn
	var err error

	if cm.config.ClientID != "" {
		conn, err = rados.NewConnWithUser(cm.config.ClientID)
	} else {
		conn, err = rados.NewConn()
	}
	if err != nil {
		return fmt.Errorf("failed to create RADOS connection: %w", err)
	}

	success := false
	defer func() {
		if !success {
			conn.Shutdown()
		}
	}()

	err = conn.ParseDefaultConfigEnv()
	if err != nil {
		return fmt.Errorf("failed to parse CEPH_ARGS: %w", err)
	}

	if cm.config.CephConf != "" {
		err = conn.ReadConfigFile(cm.config.CephConf)
	} else {
		err = conn.ReadDefaultConfigFile()
	}
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if cm.config.KeyringPath != "" {
		err = conn.SetConfigOption("keyring", cm.config.KeyringPath)
		if err != nil {
			return fmt.Errorf("failed to set keyring path: %w", err)
		}
	}

	err = conn.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to RADOS: %w", err)
	}

	success = true

	clusterMaxSize := int64(0)
	sizeStr, err := conn.GetConfigOption("osd_max_object_size")
	if err != nil {
		slog.Warn("failed to read osd_max_object_size from cluster", "error", err)
	} else {
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			slog.Warn("invalid osd_max_object_size value from cluster", "value", sizeStr, "error", err)
		} else if size <= 0 || size > math.MaxUint32 {
			slog.Warn("osd_max_object_size from cluster out of valid range", "value", size)
		} else {
			clusterMaxSize = size
			slog.Debug("loaded cluster max object size", "max_object_size", size)
		}
	}

	clusterMaxWriteSize := int64(0)
	writeSizeStr, err := conn.GetConfigOption("osd_max_write_size")
	if err != nil {
		slog.Warn("failed to read osd_max_write_size from cluster", "error", err)
	} else {
		writeSizeMB, err := strconv.ParseInt(writeSizeStr, 10, 64)
		if err != nil {
			slog.Warn("invalid osd_max_write_size value from cluster", "value", writeSizeStr, "error", err)
		} else if writeSizeMB <= 0 {
			slog.Warn("osd_max_write_size from cluster out of valid range", "value", writeSizeMB)
		} else {
			clusterMaxWriteSize = writeSizeMB * 1024 * 1024
			slog.Debug("loaded cluster max write size", "max_write_size", clusterMaxWriteSize)
		}
	}

	var maxSize int64
	if clusterMaxSize > 0 {
		maxSize = clusterMaxSize
	} else {
		maxSize = defaultMaxObjectSize
		slog.Warn("using default max object size", "default", maxSize)
	}

	var maxWriteSize int64
	if clusterMaxWriteSize > 0 {
		maxWriteSize = clusterMaxWriteSize
	} else {
		maxWriteSize = defaultMaxWriteSize
		slog.Warn("using default max write size", "default", maxWriteSize)
	}

	cm.mu.Lock()
	oldConn := cm.conn
	cm.conn = conn
	cm.maxObjectSize = maxSize
	cm.maxWriteSize = maxWriteSize
	cm.mu.Unlock()

	if oldConn != nil {
		oldConn.Shutdown()
	}

	return nil
}

func (cm *ConnectionManager) getIOContextForPool(poolName string, radosCalls *uint64) (*rados.IOContext, error) {
	const maxAttempts = 2
	for attempt := 0; attempt < maxAttempts; attempt++ {
		cm.mu.RLock()
		conn := cm.conn
		cm.mu.RUnlock()

		if conn == nil {
			if err := cm.tryReconnect(); err != nil {
				return nil, errConnectionUnavailable
			}

			cm.mu.RLock()
			conn = cm.conn
			cm.mu.RUnlock()

			if conn == nil {
				return nil, errConnectionUnavailable
			}
		}

		atomic.AddUint64(radosCalls, 1)
		slog.Debug("rados.OpenIOContext", "pool", poolName)
		ioctx, err := conn.OpenIOContext(poolName)
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				return nil, err
			}

			slog.Error("failed to open IO context", "pool", poolName, "error", err, "attempt", attempt+1)
			cm.markConnectionBroken()
			if attempt < maxAttempts-1 {
				if err := cm.tryReconnect(); err != nil {
					return nil, errConnectionUnavailable
				}
				continue
			}
			return nil, errConnectionUnavailable
		}

		return ioctx, nil
	}

	return nil, errConnectionUnavailable
}

func (cm *ConnectionManager) GetBlobPoolForRepo(repo string, bt BlobType) (*BlobPool, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.repoBlobPools == nil {
		return nil, fmt.Errorf("%w: pool configs not initialized", errPoolNotConfigured)
	}
	repoPools := cm.repoBlobPools[repo]
	if repoPools == nil {
		return nil, fmt.Errorf("%w: repo %q not configured", errPoolNotConfigured, repo)
	}
	bp := repoPools[bt]
	if bp == nil {
		return nil, fmt.Errorf("%w: %s", errPoolNotConfigured, bt)
	}
	return bp, nil
}

func (cm *ConnectionManager) GetIOContextForRepo(repo string, blobType BlobType) (*rados.IOContext, *BlobPool, error) {
	var radosCalls uint64
	defer func() {
		slog.Debug("GetIOContextForRepo", "repo", repo, "blob_type", blobType, "rados_calls", atomic.LoadUint64(&radosCalls))
	}()

	bp, err := cm.GetBlobPoolForRepo(repo, blobType)
	if err != nil {
		return nil, nil, err
	}

	ioctx, err := cm.getIOContextForPool(bp.Pool, &radosCalls)
	if err != nil {
		return nil, nil, err
	}

	if bp.Namespace != "" {
		ioctx.SetNamespace(bp.Namespace)
	}

	return ioctx, bp, nil
}

func (cm *ConnectionManager) InitializeAllPoolConfigs(repos map[string]*RepoConfig) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	conn := cm.conn
	if conn == nil {
		return errConnectionUnavailable
	}

	allPools := make(map[string]struct{})
	for _, repo := range repos {
		if repo.BlobPools == nil {
			continue
		}
		for _, bt := range AllBlobTypes {
			if p := repo.BlobPools.getPoolForType(bt).Pool; p != "" {
				allPools[p] = struct{}{}
			}
		}
	}

	poolAlignments := make(map[string]uint64)
	for poolName := range allPools {
		slog.Debug("rados.OpenIOContext", "pool", poolName)
		ioctx, err := conn.OpenIOContext(poolName)
		if err != nil {
			return fmt.Errorf("open pool %q: %w", poolName, err)
		}

		var alignment uint64 = 1
		slog.Debug("rados.RequiresAlignment", "pool", poolName)
		if ra, err := ioctx.RequiresAlignment(); err == nil && ra {
			slog.Debug("rados.Alignment", "pool", poolName)
			if align, err := ioctx.Alignment(); err == nil && align > 1 {
				alignment = align
				slog.Debug("pool requires alignment", "pool", poolName, "alignment", align)
			}
		}
		poolAlignments[poolName] = alignment
		ioctx.Destroy()
	}

	type blobPoolKey struct {
		pool          string
		namespace     string
		striped       bool
		maxObjectSize int64
	}

	repoBlobPools := make(map[string]map[BlobType]*BlobPool)

	for repoName, repo := range repos {
		if repo.BlobPools == nil {
			continue
		}

		dedup := make(map[blobPoolKey]*BlobPool)
		configs := make(map[BlobType]*BlobPool)

		for _, bt := range AllBlobTypes {
			bpc := repo.BlobPools.getPoolForType(bt)
			if bpc.Pool == "" {
				continue
			}

			striped := !repo.DisableStriper
			if bpc.Striped != nil {
				striped = *bpc.Striped
			}

			maxObjSize := cm.maxObjectSize
			if repo.MaxObjectSize > 0 {
				if repo.MaxObjectSize > maxObjSize {
					slog.Warn("repo max_object_size exceeds cluster limit, clamping",
						"repo", repoName,
						"configured", repo.MaxObjectSize,
						"cluster_limit", maxObjSize)
				} else {
					maxObjSize = repo.MaxObjectSize
				}
			}
			if bpc.MaxObjectSize != nil {
				if *bpc.MaxObjectSize > maxObjSize {
					slog.Warn("blob pool max_object_size exceeds limit, clamping",
						"repo", repoName,
						"blob_type", bt,
						"configured", *bpc.MaxObjectSize,
						"limit", maxObjSize)
				} else {
					maxObjSize = *bpc.MaxObjectSize
				}
			}

			key := blobPoolKey{pool: bpc.Pool, namespace: bpc.Namespace, striped: striped, maxObjectSize: maxObjSize}
			bp, ok := dedup[key]
			if !ok {
				bp = &BlobPool{
					Pool:          bpc.Pool,
					Namespace:     bpc.Namespace,
					Striped:       striped,
					Alignment:     poolAlignments[bpc.Pool],
					MaxObjectSize: maxObjSize,
				}
				dedup[key] = bp
			}
			configs[bt] = bp
		}
		repoBlobPools[repoName] = configs

		poolNames := make([]string, 0, len(allPools))
		for p := range allPools {
			poolNames = append(poolNames, p)
		}
		slices.Sort(poolNames)
		slog.Info("initialized pool configs", "repo", repoName, "pools", poolNames)
	}

	cm.repoBlobPools = repoBlobPools

	return nil
}

func (cm *ConnectionManager) GetMaxWriteSize() (int64, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.conn == nil {
		return 0, errConnectionUnavailable
	}

	return cm.maxWriteSize, nil
}

func (cm *ConnectionManager) markConnectionBroken() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.conn != nil {
		cm.conn.Shutdown()
		cm.conn = nil
	}
}

func (cm *ConnectionManager) tryReconnect() error {
	cm.mu.Lock()
	if cm.reconnecting {
		cm.mu.Unlock()
		return errConnectionUnavailable
	}

	now := time.Now()
	timeSinceLastReconnect := now.Sub(cm.lastReconnectTime)
	delay := cm.calculateBackoff(timeSinceLastReconnect)

	if timeSinceLastReconnect < delay {
		cm.mu.Unlock()
		return errConnectionUnavailable
	}

	cm.reconnecting = true
	cm.lastReconnectTime = now
	cm.mu.Unlock()

	defer func() {
		cm.mu.Lock()
		cm.reconnecting = false
		cm.mu.Unlock()
	}()

	slog.Info("attempting to reconnect to ceph")
	if err := cm.connect(); err != nil {
		slog.Warn("reconnection failed", "error", err)
		return err
	}

	slog.Info("successfully reconnected to ceph")
	return nil
}

func (cm *ConnectionManager) calculateBackoff(timeSinceLastReconnect time.Duration) time.Duration {
	if timeSinceLastReconnect >= cm.maxReconnectDelay {
		return cm.minReconnectDelay
	}

	backoff := cm.minReconnectDelay
	for backoff < cm.maxReconnectDelay && backoff < timeSinceLastReconnect {
		backoff *= 2
	}

	if backoff > cm.maxReconnectDelay {
		backoff = cm.maxReconnectDelay
	}

	return backoff
}

func (cm *ConnectionManager) Shutdown() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.conn != nil {
		cm.conn.Shutdown()
		cm.conn = nil
	}
}
