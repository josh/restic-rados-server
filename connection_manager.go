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
	"syscall"
	"time"

	"github.com/ceph/go-ceph/rados"
)

const (
	defaultMaxObjectSize int64 = 128 * 1024 * 1024
	defaultMaxWriteSize  int64 = 90 * 1024 * 1024

	maxMountTimeout = 5
)

var (
	errConnectionUnavailable = errors.New("ceph connection unavailable")
	errPoolNotConfigured     = errors.New("pool not configured for blob type")
	errCephConfigInvalid     = errors.New("invalid ceph configuration")
	errConfiguredPoolMissing = errors.New("configured ceph pool missing")
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
	reconnectBackoff  time.Duration
	minReconnectDelay time.Duration
	maxReconnectDelay time.Duration
	maxObjectSize     int64
	maxWriteSize      int64
	repoBlobPools     map[string]map[BlobType]*BlobPool
	repoPatterns      []repoPattern
	repos             map[string]*RepoConfig
	reconnectSignal   chan struct{}
	shutdown          chan struct{}
	shutdownOnce      sync.Once
}

func NewConnectionManager(config CephConfig) *ConnectionManager {
	cm := &ConnectionManager{
		config:            config,
		minReconnectDelay: 1 * time.Second,
		maxReconnectDelay: 30 * time.Second,
		reconnectSignal:   make(chan struct{}, 1),
		shutdown:          make(chan struct{}),
	}
	go cm.reconnectLoop()
	return cm
}

func (cm *ConnectionManager) connect() error {
	cm.mu.RLock()
	closed := cm.closed
	repos := cm.repos
	cm.mu.RUnlock()
	if closed || repos == nil {
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
		return fmt.Errorf("%w: failed to create RADOS connection: %v", errCephConfigInvalid, err)
	}

	success := false
	defer func() {
		if !success {
			conn.Shutdown()
		}
	}()

	err = conn.ParseDefaultConfigEnv()
	if err != nil {
		return fmt.Errorf("%w: failed to parse CEPH_ARGS: %v", errCephConfigInvalid, err)
	}

	if cm.config.CephConf != "" {
		err = conn.ReadConfigFile(cm.config.CephConf)
	} else {
		err = conn.ReadDefaultConfigFile()
	}
	if err != nil {
		return fmt.Errorf("%w: failed to read config file: %v", errCephConfigInvalid, err)
	}

	if cm.config.KeyringPath != "" {
		err = conn.SetConfigOption("keyring", cm.config.KeyringPath)
		if err != nil {
			return fmt.Errorf("%w: failed to set keyring path: %v", errCephConfigInvalid, err)
		}
	}

	if timeout, err := conn.GetConfigOption("client_mount_timeout"); err == nil {
		if secs, err := strconv.ParseFloat(timeout, 64); err == nil && secs > maxMountTimeout {
			slog.Debug("clamping client_mount_timeout", "configured", secs, "clamped_to", maxMountTimeout)
			if err := conn.SetConfigOption("client_mount_timeout", strconv.Itoa(maxMountTimeout)); err != nil {
				return fmt.Errorf("%w: failed to set mount timeout: %v", errCephConfigInvalid, err)
			}
		}
	}

	err = conn.Connect()
	if err != nil {
		if isPermissionDenied(err) {
			return fmt.Errorf("%w: failed to authenticate with RADOS: %v", errCephConfigInvalid, err)
		}
		return fmt.Errorf("%w: failed to connect to RADOS: %v", errConnectionUnavailable, err)
	}

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

	if cm.config.WriteBufferSize > 0 && cm.config.WriteBufferSize > maxWriteSize {
		slog.Warn("write buffer size exceeds cluster max write size, writes may be chunked or fail",
			"write_buffer_size", cm.config.WriteBufferSize,
			"cluster_max_write_size", maxWriteSize)
	}

	poolAlignments, err := resolvePoolAlignments(conn, repos)
	if err != nil {
		return err
	}
	repoBlobPools := buildRepoBlobPools(repos, maxSize, poolAlignments)
	repoPatterns := compileRepoPatterns(repos)

	cm.mu.Lock()
	if cm.closed {
		cm.mu.Unlock()
		return errConnectionUnavailable
	}
	oldHandle := cm.handle
	cm.handle = newConnHandle(conn)
	cm.maxObjectSize = maxSize
	cm.maxWriteSize = maxWriteSize
	cm.repoBlobPools = repoBlobPools
	cm.repoPatterns = repoPatterns
	cm.mu.Unlock()
	success = true

	if oldHandle != nil {
		oldHandle.release()
	}
	logInitializedPoolConfigs(repoBlobPools)

	if missing := len(configuredPoolNames(repos)) - len(poolAlignments); missing > 0 {
		return fmt.Errorf("%w: %d of %d configured pools unavailable", errConfiguredPoolMissing, missing, len(configuredPoolNames(repos)))
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
	handle := cm.acquireHandle()
	if handle == nil {
		cm.requestReconnect()
		return nil, nil, nil, errConnectionUnavailable
	}

	atomic.AddUint64(radosCalls, 1)
	slog.Debug("rados.OpenIOContext", "pool", bp.Pool)
	ioctx, err := handle.conn.OpenIOContext(bp.Pool)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			handle.release()
			return nil, nil, nil, fmt.Errorf("%w: pool %q not found", errConnectionUnavailable, bp.Pool)
		}

		slog.Error("failed to open IO context", "pool", bp.Pool, "error", err)
		cm.markConnectionBroken(handle)
		handle.release()
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

		slog.Error("failed to open IO context", "pool", bp.Lower.Pool, "error", err)
		cm.markConnectionBroken(handle)
		handle.release()
		return nil, nil, nil, errConnectionUnavailable
	}

	return ioctx, lowerIoctx, handle, nil
}

func (cm *ConnectionManager) OpenNamespaceContext(pool, namespace string, radosCalls *uint64) (*rados.IOContext, *connHandle, error) {
	ioctx, _, handle, err := cm.getIOContextsForBlobPool(&BlobPool{Pool: pool}, radosCalls)
	if err != nil {
		return nil, nil, err
	}
	if namespace != "" {
		ioctx.SetNamespace(namespace)
	}
	return ioctx, handle, nil
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

func configuredPoolNames(repos map[string]*RepoConfig) map[string]struct{} {
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
	return allPools
}

func isPermissionDenied(err error) bool {
	var ec interface{ ErrorCode() int }
	if !errors.As(err, &ec) {
		return false
	}
	switch ec.ErrorCode() {
	case -int(syscall.EPERM), -int(syscall.EACCES), -int(syscall.EKEYREJECTED), -int(syscall.EKEYEXPIRED):
		return true
	}
	return false
}

func resolvePoolAlignments(conn *rados.Conn, repos map[string]*RepoConfig) (map[string]uint64, error) {
	configured := configuredPoolNames(repos)
	poolAlignments := make(map[string]uint64)
	for poolName := range configured {
		slog.Debug("rados.OpenIOContext", "pool", poolName)
		ioctx, err := conn.OpenIOContext(poolName)
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				slog.Warn("configured pool missing, repos using it will be unavailable", "pool", poolName)
				continue
			}
			if isPermissionDenied(err) {
				return nil, fmt.Errorf("%w: open pool %q: %v", errCephConfigInvalid, poolName, err)
			}
			return nil, fmt.Errorf("%w: open pool %q: %v", errConnectionUnavailable, poolName, err)
		}

		alignment, err := func() (uint64, error) {
			defer ioctx.Destroy()
			slog.Debug("rados.RequiresAlignment", "pool", poolName)
			requiresAlignment, err := ioctx.RequiresAlignment()
			if err != nil {
				return 0, err
			}
			if !requiresAlignment {
				return 1, nil
			}
			slog.Debug("rados.Alignment", "pool", poolName)
			alignment, err := ioctx.Alignment()
			if err != nil {
				return 0, err
			}
			if alignment == 0 {
				return 0, errors.New("pool returned zero alignment")
			}
			return alignment, nil
		}()
		if err != nil {
			slog.Warn("failed to resolve pool alignment, assuming unaligned", "pool", poolName, "error", err)
			alignment = 1
		}
		if alignment > 1 {
			slog.Debug("pool requires alignment", "pool", poolName, "alignment", alignment)
		}
		poolAlignments[poolName] = alignment
	}
	if len(configured) > 0 && len(poolAlignments) == 0 {
		return nil, fmt.Errorf("%w: no configured pool could be opened", errConfiguredPoolMissing)
	}
	return poolAlignments, nil
}

func buildRepoBlobPools(repos map[string]*RepoConfig, clusterMaxObjectSize int64, poolAlignments map[string]uint64) map[string]map[BlobType]*BlobPool {
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

			if _, ok := poolAlignments[bpc.Pool]; !ok {
				continue
			}
			if bpc.Lower != nil {
				if _, ok := poolAlignments[bpc.Lower.Pool]; !ok {
					continue
				}
			}

			striped := repo.Striper == nil || *repo.Striper
			if bpc.Striped != nil {
				striped = *bpc.Striped
			}

			maxObjSize := clusterMaxObjectSize
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
					if *bpc.Lower.MaxObjectSize > clusterMaxObjectSize {
						slog.Warn("lower pool max_object_size exceeds cluster limit, clamping",
							"repo", repoName,
							"blob_type", bt,
							"configured", *bpc.Lower.MaxObjectSize,
							"limit", clusterMaxObjectSize)
						lowerMaxObjSize = clusterMaxObjectSize
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
				alignment := poolAlignments[bpc.Pool]
				if alignment == 0 {
					alignment = 1
				}
				bp = &BlobPool{
					Pool:          bpc.Pool,
					Namespace:     bpc.Namespace,
					Prefix:        bpc.Prefix,
					Striped:       striped,
					Alignment:     alignment,
					MaxObjectSize: maxObjSize,
				}
				if bpc.Lower != nil {
					lowerAlignment := poolAlignments[bpc.Lower.Pool]
					if lowerAlignment == 0 {
						lowerAlignment = 1
					}
					bp.Lower = &BlobPool{
						Pool:          bpc.Lower.Pool,
						Namespace:     bpc.Lower.Namespace,
						Prefix:        bpc.Lower.Prefix,
						Striped:       key.lowerStriped,
						Alignment:     lowerAlignment,
						MaxObjectSize: key.lowerMaxObjectSize,
					}
				}
				dedup[key] = bp
			}
			configs[bt] = bp
		}
		repoBlobPools[repoName] = configs
	}
	return repoBlobPools
}

func logInitializedPoolConfigs(repoBlobPools map[string]map[BlobType]*BlobPool) {
	for repoName, configs := range repoBlobPools {
		repoPools := make(map[string]struct{})
		for _, bp := range configs {
			repoPools[bp.Pool] = struct{}{}
			if bp.Lower != nil {
				repoPools[bp.Lower.Pool] = struct{}{}
			}
		}
		poolNames := make([]string, 0, len(repoPools))
		for pool := range repoPools {
			poolNames = append(poolNames, pool)
		}
		slices.Sort(poolNames)
		slog.Info("initialized pool configs", "repo", repoName, "pools", poolNames)
	}
}

func (cm *ConnectionManager) InitializeAllPoolConfigs(repos map[string]*RepoConfig) error {
	cm.mu.Lock()
	if cm.closed {
		cm.mu.Unlock()
		return errConnectionUnavailable
	}
	cm.repos = repos
	cm.repoPatterns = compileRepoPatterns(repos)
	cm.maxObjectSize = defaultMaxObjectSize
	cm.maxWriteSize = defaultMaxWriteSize
	cm.mu.Unlock()

	for _, c := range storageCollisions(repos) {
		slog.Warn("repos may share storage", "repo", c.repo, "other_repo", c.otherRepo, "blob_types", c.blobTypes)
	}

	if err := cm.connect(); err != nil {
		if errors.Is(err, errCephConfigInvalid) || errors.Is(err, errConfiguredPoolMissing) {
			return err
		}
		cm.recordReconnectFailure()
		slog.Warn("initial ceph connection failed, starting unready", "error", err)
		cm.requestReconnect()
		return nil
	}

	slog.Info("ceph connection established")
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
	cm.requestReconnect()
}

func (cm *ConnectionManager) requestReconnect() {
	cm.mu.RLock()
	canReconnect := !cm.closed && cm.repos != nil && cm.handle == nil
	cm.mu.RUnlock()
	if !canReconnect {
		return
	}
	select {
	case <-cm.shutdown:
	case cm.reconnectSignal <- struct{}{}:
	default:
	}
}

func (cm *ConnectionManager) recordReconnectFailure() {
	cm.mu.Lock()
	if cm.reconnectBackoff == 0 {
		cm.reconnectBackoff = cm.minReconnectDelay
	} else {
		cm.reconnectBackoff = min(cm.reconnectBackoff*2, cm.maxReconnectDelay)
	}
	cm.mu.Unlock()
}

func (cm *ConnectionManager) resetReconnectBackoff() {
	cm.mu.Lock()
	cm.reconnectBackoff = 0
	cm.mu.Unlock()
}

func (cm *ConnectionManager) waitForReconnectDelay() bool {
	cm.mu.RLock()
	delay := cm.reconnectBackoff
	cm.mu.RUnlock()
	if delay <= 0 {
		select {
		case <-cm.shutdown:
			return false
		default:
			return true
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-cm.shutdown:
		return false
	case <-timer.C:
		return true
	}
}

func (cm *ConnectionManager) reconnectLoop() {
	for {
		select {
		case <-cm.shutdown:
			return
		case <-cm.reconnectSignal:
		}

		for {
			if !cm.waitForReconnectDelay() {
				return
			}

			slog.Info("attempting to reconnect to ceph")
			if err := cm.connect(); err != nil {
				slog.Warn("reconnection failed", "error", err)
				cm.recordReconnectFailure()
				continue
			}

			cm.resetReconnectBackoff()
			slog.Info("successfully reconnected to ceph")
			break
		}
	}
}

func (cm *ConnectionManager) Shutdown() {
	cm.shutdownOnce.Do(func() {
		close(cm.shutdown)
	})
	cm.mu.Lock()
	cm.closed = true
	handle := cm.handle
	cm.handle = nil
	cm.mu.Unlock()

	if handle != nil {
		handle.release()
	}
}
