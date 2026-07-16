package main

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
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

type connHandle struct {
	conn *rados.Conn
	refs atomic.Int64
}

func newConnHandle(conn *rados.Conn) *connHandle {
	handle := &connHandle{conn: conn}
	handle.refs.Store(1)
	return handle
}

func (h *connHandle) acquire() bool {
	for {
		refs := h.refs.Load()
		if refs <= 0 {
			return false
		}
		if h.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
}

func (h *connHandle) release() {
	if h.refs.Add(-1) == 0 {
		h.conn.Shutdown()
	}
}

type ConnectionManager struct {
	mu                sync.RWMutex
	handle            *connHandle
	closed            bool
	config            CephConfig
	reconnecting      bool
	lastReconnectTime time.Time
	reconnectBackoff  time.Duration
	minReconnectDelay time.Duration
	maxReconnectDelay time.Duration
	maxObjectSize     int64
	maxWriteSize      int64
	repoBlobPools     map[string]map[BlobType]*BlobPool
	repoPatterns      []repoPattern
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
	cm.mu.RLock()
	closed := cm.closed
	cm.mu.RUnlock()
	if closed {
		return errConnectionUnavailable
	}

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
	if cm.closed {
		cm.mu.Unlock()
		conn.Shutdown()
		return errConnectionUnavailable
	}
	oldHandle := cm.handle
	cm.handle = newConnHandle(conn)
	cm.maxObjectSize = maxSize
	cm.maxWriteSize = maxWriteSize
	cm.mu.Unlock()

	if oldHandle != nil {
		oldHandle.release()
	}

	return nil
}

func (cm *ConnectionManager) acquireHandle() *connHandle {
	for {
		cm.mu.RLock()
		handle := cm.handle
		cm.mu.RUnlock()
		if handle == nil {
			return nil
		}
		if handle.acquire() {
			return handle
		}
	}
}

func (cm *ConnectionManager) getIOContextsForBlobPool(bp *BlobPool, radosCalls *uint64) (*rados.IOContext, *rados.IOContext, *connHandle, error) {
	const maxAttempts = 2
	for attempt := 0; attempt < maxAttempts; attempt++ {
		handle := cm.acquireHandle()
		if handle == nil {
			if err := cm.tryReconnect(); err != nil {
				return nil, nil, nil, errConnectionUnavailable
			}

			handle = cm.acquireHandle()
			if handle == nil {
				return nil, nil, nil, errConnectionUnavailable
			}
		}

		atomic.AddUint64(radosCalls, 1)
		slog.Debug("rados.OpenIOContext", "pool", bp.Pool)
		ioctx, err := handle.conn.OpenIOContext(bp.Pool)
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				handle.release()
				return nil, nil, nil, fmt.Errorf("%w: pool %q not found", errConnectionUnavailable, bp.Pool)
			}

			slog.Error("failed to open IO context", "pool", bp.Pool, "error", err, "attempt", attempt+1)
			cm.markConnectionBroken(handle)
			handle.release()
			if attempt < maxAttempts-1 {
				continue
			}
			return nil, nil, nil, errConnectionUnavailable
		}

		if bp.Lower == nil {
			return ioctx, nil, handle, nil
		}

		atomic.AddUint64(radosCalls, 1)
		slog.Debug("rados.OpenIOContext", "pool", bp.Lower.Pool)
		lowerIoctx, err := handle.conn.OpenIOContext(bp.Lower.Pool)
		if err != nil {
			ioctx.Destroy()
			if errors.Is(err, rados.ErrNotFound) {
				handle.release()
				return nil, nil, nil, fmt.Errorf("%w: lower pool %q not found", errConnectionUnavailable, bp.Lower.Pool)
			}

			slog.Error("failed to open IO context", "pool", bp.Lower.Pool, "error", err, "attempt", attempt+1)
			cm.markConnectionBroken(handle)
			handle.release()
			if attempt < maxAttempts-1 {
				continue
			}
			return nil, nil, nil, errConnectionUnavailable
		}

		return ioctx, lowerIoctx, handle, nil
	}

	return nil, nil, nil, errConnectionUnavailable
}

func (cm *ConnectionManager) GetBlobPoolForRepo(repo string, bt BlobType) (*BlobPool, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.repoBlobPools == nil {
		return nil, fmt.Errorf("%w: pool configs not initialized", errPoolNotConfigured)
	}
	if strings.Contains(repo, "*") {
		return nil, fmt.Errorf("%w: repo %q not configured", errPoolNotConfigured, repo)
	}
	repoPools := cm.repoBlobPools[repo]
	match := ""
	dynamic := false
	if repoPools == nil {
		for _, p := range cm.repoPatterns {
			if m, ok := p.match(repo); ok {
				repoPools = cm.repoBlobPools[p.key]
				match = m
				dynamic = true
				break
			}
		}
	}
	if repoPools == nil {
		return nil, fmt.Errorf("%w: repo %q not configured", errPoolNotConfigured, repo)
	}
	bp := repoPools[bt]
	if bp == nil {
		return nil, fmt.Errorf("%w: %s", errPoolNotConfigured, bt)
	}
	if dynamic {
		bp = bp.forRepo(repo, match)
	}
	return bp, nil
}

func (cm *ConnectionManager) GetIOContextForRepo(repo string, blobType BlobType) (*rados.IOContext, *rados.IOContext, *connHandle, *BlobPool, error) {
	var radosCalls uint64
	defer func() {
		slog.Debug("GetIOContextForRepo", "repo", repo, "blob_type", blobType, "rados_calls", atomic.LoadUint64(&radosCalls))
	}()

	bp, err := cm.GetBlobPoolForRepo(repo, blobType)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	ioctx, lowerIoctx, handle, err := cm.getIOContextsForBlobPool(bp, &radosCalls)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if bp.Namespace != "" {
		ioctx.SetNamespace(bp.Namespace)
	}

	if lowerIoctx != nil && bp.Lower.Namespace != "" {
		lowerIoctx.SetNamespace(bp.Lower.Namespace)
	}

	return ioctx, lowerIoctx, handle, bp, nil
}

func (cm *ConnectionManager) InitializeAllPoolConfigs(repos map[string]*RepoConfig) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.handle == nil {
		return errConnectionUnavailable
	}
	conn := cm.handle.conn

	allPools := make(map[string]struct{})
	for _, repo := range repos {
		if repo.BlobPools == nil {
			continue
		}
		for _, bt := range AllBlobTypes {
			bpc := repo.BlobPools.getPoolForType(bt)
			if bpc.Pool != "" {
				allPools[bpc.Pool] = struct{}{}
			}
			if bpc.Lower != nil && bpc.Lower.Pool != "" {
				allPools[bpc.Lower.Pool] = struct{}{}
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
		pool               string
		namespace          string
		prefix             string
		striped            bool
		maxObjectSize      int64
		lowerPool          string
		lowerNS            string
		lowerPrefix        string
		lowerStriped       bool
		lowerMaxObjectSize int64
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

			striped := repo.Striper == nil || *repo.Striper
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

			key := blobPoolKey{pool: bpc.Pool, namespace: bpc.Namespace, prefix: bpc.Prefix, striped: striped, maxObjectSize: maxObjSize}
			if bpc.Lower != nil {
				lowerStriped := striped
				if bpc.Lower.Striped != nil {
					lowerStriped = *bpc.Lower.Striped
				}
				lowerMaxObjSize := maxObjSize
				if bpc.Lower.MaxObjectSize != nil {
					if *bpc.Lower.MaxObjectSize > cm.maxObjectSize {
						slog.Warn("lower pool max_object_size exceeds cluster limit, clamping",
							"repo", repoName,
							"blob_type", bt,
							"configured", *bpc.Lower.MaxObjectSize,
							"limit", cm.maxObjectSize)
						lowerMaxObjSize = cm.maxObjectSize
					} else {
						lowerMaxObjSize = *bpc.Lower.MaxObjectSize
					}
				}
				key.lowerPool = bpc.Lower.Pool
				key.lowerNS = bpc.Lower.Namespace
				key.lowerPrefix = bpc.Lower.Prefix
				key.lowerStriped = lowerStriped
				key.lowerMaxObjectSize = lowerMaxObjSize
			}
			bp, ok := dedup[key]
			if !ok {
				bp = &BlobPool{
					Pool:          bpc.Pool,
					Namespace:     bpc.Namespace,
					Prefix:        bpc.Prefix,
					Striped:       striped,
					Alignment:     poolAlignments[bpc.Pool],
					MaxObjectSize: maxObjSize,
				}
				if bpc.Lower != nil {
					bp.Lower = &BlobPool{
						Pool:          bpc.Lower.Pool,
						Namespace:     bpc.Lower.Namespace,
						Prefix:        bpc.Lower.Prefix,
						Striped:       key.lowerStriped,
						Alignment:     poolAlignments[bpc.Lower.Pool],
						MaxObjectSize: key.lowerMaxObjectSize,
					}
				}
				dedup[key] = bp
			}
			configs[bt] = bp
		}
		repoBlobPools[repoName] = configs

		repoPools := make(map[string]struct{})
		for _, bp := range configs {
			repoPools[bp.Pool] = struct{}{}
			if bp.Lower != nil {
				repoPools[bp.Lower.Pool] = struct{}{}
			}
		}
		poolNames := make([]string, 0, len(repoPools))
		for p := range repoPools {
			poolNames = append(poolNames, p)
		}
		slices.Sort(poolNames)
		slog.Info("initialized pool configs", "repo", repoName, "pools", poolNames)
	}

	cm.repoBlobPools = repoBlobPools
	cm.repoPatterns = compileRepoPatterns(repos)

	for _, c := range storageCollisions(repos) {
		slog.Warn("repos may share storage", "repo", c.repo, "other_repo", c.otherRepo, "blob_types", c.blobTypes)
	}

	return nil
}

func (cm *ConnectionManager) GetMaxWriteSize() (int64, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.handle == nil {
		return 0, errConnectionUnavailable
	}

	return cm.maxWriteSize, nil
}

func (cm *ConnectionManager) Ready() bool {
	if handle := cm.acquireHandle(); handle != nil {
		handle.release()
		return true
	}
	go func() { _ = cm.tryReconnect() }()
	return false
}

func (cm *ConnectionManager) markConnectionBroken(handle *connHandle) {
	cm.mu.Lock()
	if cm.handle != handle {
		cm.mu.Unlock()
		return
	}
	cm.handle = nil
	cm.mu.Unlock()

	handle.release()
}

func (cm *ConnectionManager) tryReconnect() error {
	cm.mu.Lock()
	if cm.reconnecting {
		cm.mu.Unlock()
		return errConnectionUnavailable
	}

	now := time.Now()
	if !cm.lastReconnectTime.IsZero() && now.Sub(cm.lastReconnectTime) < cm.reconnectBackoff {
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
		cm.mu.Lock()
		if cm.reconnectBackoff == 0 {
			cm.reconnectBackoff = cm.minReconnectDelay
		} else {
			cm.reconnectBackoff = min(cm.reconnectBackoff*2, cm.maxReconnectDelay)
		}
		cm.mu.Unlock()
		return err
	}

	cm.mu.Lock()
	cm.reconnectBackoff = 0
	cm.mu.Unlock()

	slog.Info("successfully reconnected to ceph")
	return nil
}

func (cm *ConnectionManager) Shutdown() {
	cm.mu.Lock()
	cm.closed = true
	handle := cm.handle
	cm.handle = nil
	cm.mu.Unlock()

	if handle != nil {
		handle.release()
	}
}
