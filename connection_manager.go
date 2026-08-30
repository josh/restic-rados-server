package main

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ceph/go-ceph/rados"
)

const (
	connectionRetryInterval        = time.Second
	maximumConnectionRetryInterval = 30 * time.Second
	maximumConnectTimeout          = 5 * time.Second
	defaultMaxObjectSize           = 128 * 1024 * 1024
)

var (
	errConnectionUnavailable = errors.New("ceph connection unavailable")
	errPoolNotConfigured     = errors.New("pool not configured")
	errConfiguredPoolMissing = errors.New("configured ceph pool missing")
)

type poolProperties struct {
	alignment uint64
}

type connectionState struct {
	conn    *rados.Conn
	active  int
	retired bool
}

type connHandle struct {
	manager    *ConnectionManager
	connection *connectionState
	once       sync.Once
}

func (h *connHandle) release() {
	if h == nil {
		return
	}
	h.once.Do(func() {
		h.manager.release(h.connection)
	})
}

type ConnectionManager struct {
	config CephConfig

	mu           sync.Mutex
	current      *connectionState
	poolConfigs  map[string]map[BlobType]*BlobPool
	patterns     []repoPattern
	repos        map[string]*RepoConfig
	initialized  bool
	shuttingDown bool
	retrying     bool
	retryStop    chan struct{}
	shutdown     sync.Once
}

func NewConnectionManager(config CephConfig) *ConnectionManager {
	manager := &ConnectionManager{
		config:    config,
		retryStop: make(chan struct{}),
	}
	return manager
}

func (m *ConnectionManager) InitializeAllPoolConfigs(repos map[string]*RepoConfig) error {
	m.mu.Lock()
	if m.initialized {
		m.mu.Unlock()
		return errors.New("connection manager already initialized")
	}
	if m.shuttingDown {
		m.mu.Unlock()
		return errConnectionUnavailable
	}
	m.initialized = true
	m.repos = repos
	m.mu.Unlock()

	for _, collision := range storageCollisions(repos) {
		slog.Warn("repos may share storage", "repo", collision.repo, "other_repo", collision.otherRepo, "blob_types", strings.Join(collision.blobTypes, ","))
	}

	conn, err := m.newConfiguredConnection()
	if err != nil {
		return fmt.Errorf("invalid ceph configuration: %w", err)
	}
	if err := conn.Connect(); err != nil {
		conn.Shutdown()
		if isPermanentConnectError(err) {
			return fmt.Errorf("invalid ceph configuration: connect: %w", err)
		}
		slog.Warn("initial ceph connection failed, starting unready", "error", err)
		m.startRetry()
		return nil
	}

	poolConfigs, patterns, err := m.resolveAllPoolConfigs(conn, repos)
	if err != nil {
		conn.Shutdown()
		if isTransientConnectionError(err) {
			slog.Warn("initial ceph pool resolution failed, starting unready", "error", err)
			m.startRetry()
			return nil
		}
		return err
	}
	if !m.publishConnection(conn, poolConfigs, patterns) {
		conn.Shutdown()
		return errConnectionUnavailable
	}
	return nil
}

func (m *ConnectionManager) Ready() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.shuttingDown && m.current != nil && m.poolConfigs != nil
}

func (m *ConnectionManager) Shutdown() {
	m.shutdown.Do(func() {
		m.mu.Lock()
		m.shuttingDown = true
		close(m.retryStop)
		current := m.current
		m.current = nil
		m.poolConfigs = nil
		m.patterns = nil
		conn := m.retireConnectionLocked(current)
		serverMetrics.cephConnected.set(0)
		m.mu.Unlock()

		if conn != nil {
			conn.Shutdown()
		}
	})
}

func (m *ConnectionManager) GetBlobPoolForRepo(repo string, blobType BlobType) (*BlobPool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shuttingDown || m.current == nil || m.poolConfigs == nil {
		return nil, errConnectionUnavailable
	}
	return m.lookupBlobPool(repo, blobType)
}

func (m *ConnectionManager) GetIOContextForRepo(repo string, blobType BlobType, radosCalls *uint64) (*rados.IOContext, *rados.IOContext, *connHandle, *BlobPool, error) {
	conn, handle, bp, err := m.acquireForRepo(repo, blobType)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	ioctx, err := openNamespaceContext(conn, bp.Pool, bp.Namespace, radosCalls)
	if err != nil {
		m.reconnectAfterError(handle.connection, err)
		handle.release()
		return nil, nil, nil, nil, classifyOpenContextError(bp.Pool, err)
	}

	var lowerIoctx *rados.IOContext
	if bp.Lower != nil {
		lowerIoctx, err = openNamespaceContext(conn, bp.Lower.Pool, bp.Lower.Namespace, radosCalls)
		if err != nil {
			ioctx.Destroy()
			m.reconnectAfterError(handle.connection, err)
			handle.release()
			return nil, nil, nil, nil, classifyOpenContextError(bp.Lower.Pool, err)
		}
	}

	return ioctx, lowerIoctx, handle, bp, nil
}

func (m *ConnectionManager) OpenNamespaceContext(pool, namespace string, radosCalls *uint64) (*rados.IOContext, *connHandle, error) {
	conn, handle, err := m.acquire()
	if err != nil {
		return nil, nil, err
	}

	ioctx, err := openNamespaceContext(conn, pool, namespace, radosCalls)
	if err != nil {
		m.reconnectAfterError(handle.connection, err)
		handle.release()
		return nil, nil, classifyOpenContextError(pool, err)
	}
	return ioctx, handle, nil
}

func (m *ConnectionManager) newConfiguredConnection() (*rados.Conn, error) {
	var (
		conn *rados.Conn
		err  error
	)
	if m.config.ClientID == "" {
		conn, err = rados.NewConn()
	} else {
		conn, err = rados.NewConnWithUser(m.config.ClientID)
	}
	if err != nil {
		return nil, fmt.Errorf("create connection: %w", err)
	}

	if m.config.CephConf == "" {
		err = conn.ReadDefaultConfigFile()
	} else {
		err = conn.ReadConfigFile(m.config.CephConf)
	}
	if err != nil {
		conn.Shutdown()
		return nil, fmt.Errorf("read ceph config: %w", err)
	}
	if err := conn.ParseDefaultConfigEnv(); err != nil {
		conn.Shutdown()
		return nil, fmt.Errorf("parse CEPH_ARGS: %w", err)
	}

	if m.config.KeyringPath != "" {
		if err := conn.SetConfigOption("keyring", m.config.KeyringPath); err != nil {
			conn.Shutdown()
			return nil, fmt.Errorf("set keyring: %w", err)
		}
	}
	if err := capConnectionTimeout(conn); err != nil {
		conn.Shutdown()
		return nil, err
	}
	return conn, nil
}

func (m *ConnectionManager) startRetry() {
	m.mu.Lock()
	start := m.startRetryLocked()
	m.mu.Unlock()
	if start {
		go m.retryConnection()
	}
}

func (m *ConnectionManager) startRetryLocked() bool {
	if m.shuttingDown || m.retrying || m.current != nil {
		return false
	}
	m.retrying = true
	return true
}

func (m *ConnectionManager) retryConnection() {
	retryInterval := connectionRetryInterval
	recordFailure := func(err error) {
		retryInterval = min(retryInterval*2, maximumConnectionRetryInterval)
		slog.Warn("ceph reconnect attempt failed", "error", err, "retry_after", retryInterval)
	}

	for {
		timer := time.NewTimer(retryInterval)
		select {
		case <-m.retryStop:
			timer.Stop()
			return
		case <-timer.C:
		}

		conn, err := m.newConfiguredConnection()
		if err != nil {
			recordFailure(err)
			continue
		}
		if err := conn.Connect(); err != nil {
			conn.Shutdown()
			recordFailure(err)
			continue
		}

		poolConfigs, patterns, err := m.resolveAllPoolConfigs(conn, m.repos)
		if err != nil {
			conn.Shutdown()
			recordFailure(err)
			continue
		}
		if !m.publishConnection(conn, poolConfigs, patterns) {
			conn.Shutdown()
			return
		}
		serverMetrics.cephReconnects.add(1)
		slog.Info("successfully reconnected to ceph")
		return
	}
}

func (m *ConnectionManager) publishConnection(conn *rados.Conn, poolConfigs map[string]map[BlobType]*BlobPool, patterns []repoPattern) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shuttingDown || m.current != nil {
		return false
	}
	m.current = &connectionState{conn: conn}
	m.poolConfigs = poolConfigs
	m.patterns = patterns
	m.retrying = false
	serverMetrics.cephConnected.set(1)
	return true
}

func (m *ConnectionManager) resolveAllPoolConfigs(conn *rados.Conn, repos map[string]*RepoConfig) (map[string]map[BlobType]*BlobPool, []repoPattern, error) {
	maxObjectSize := int64(defaultMaxObjectSize)
	if configured, err := conn.GetConfigOption("osd_max_object_size"); err == nil {
		maxObjectSize = parseClusterMaxObjectSize(configured)
	}

	properties := make(map[string]poolProperties)
	resolved := make(map[string]map[BlobType]*BlobPool, len(repos))
	names := make([]string, 0, len(repos))
	for name := range repos {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		repo := repos[name]
		if repo == nil || repo.BlobPools == nil {
			continue
		}
		resolved[name] = make(map[BlobType]*BlobPool)
		for _, blobType := range AllBlobTypes {
			poolConfig := repo.BlobPools.getPoolForType(blobType)
			if poolConfig.Pool == "" {
				continue
			}
			bp, err := m.resolveBlobPool(conn, repo, poolConfig, maxObjectSize, properties)
			if err != nil {
				return nil, nil, fmt.Errorf("repo %q blob type %q: %w", name, blobType, err)
			}
			resolved[name][blobType] = bp
		}
	}
	return resolved, compileRepoPatterns(repos), nil
}

func (m *ConnectionManager) resolveBlobPool(conn *rados.Conn, repo *RepoConfig, config BlobPoolConfig, clusterMaxObjectSize int64, properties map[string]poolProperties) (*BlobPool, error) {
	upperProperties, err := m.getPoolProperties(conn, config.Pool, properties)
	if err != nil {
		return nil, err
	}

	bp := &BlobPool{
		Pool:          config.Pool,
		Namespace:     config.Namespace,
		Prefix:        config.Prefix,
		Striped:       resolveStriped(config.Striped, repo.Striper),
		Alignment:     upperProperties.alignment,
		MaxObjectSize: resolveMaxObjectSize(config.MaxObjectSize, repo.MaxObjectSize, clusterMaxObjectSize),
	}

	if config.Lower != nil {
		lowerProperties, err := m.getPoolProperties(conn, config.Lower.Pool, properties)
		if err != nil {
			return nil, err
		}
		bp.Lower = &BlobPool{
			Pool:          config.Lower.Pool,
			Namespace:     config.Lower.Namespace,
			Prefix:        config.Lower.Prefix,
			Striped:       resolveStriped(config.Lower.Striped, repo.Striper),
			Alignment:     lowerProperties.alignment,
			MaxObjectSize: resolveMaxObjectSize(config.Lower.MaxObjectSize, repo.MaxObjectSize, clusterMaxObjectSize),
		}
	}
	return bp, nil
}

func (m *ConnectionManager) getPoolProperties(conn *rados.Conn, pool string, properties map[string]poolProperties) (poolProperties, error) {
	if property, ok := properties[pool]; ok {
		return property, nil
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return poolProperties{}, fmt.Errorf("%w %q: %v", errConfiguredPoolMissing, pool, err)
		}
		return poolProperties{}, fmt.Errorf("open configured pool %q: %w", pool, err)
	}
	defer ioctx.Destroy()

	alignment := uint64(1)
	requiresAlignment, err := ioctx.RequiresAlignment()
	if err != nil {
		return poolProperties{}, fmt.Errorf("get alignment requirement for pool %q: %w", pool, err)
	}
	if requiresAlignment {
		alignment, err = ioctx.Alignment()
		if err != nil {
			return poolProperties{}, fmt.Errorf("get alignment for pool %q: %w", pool, err)
		}
		if alignment == 0 {
			return poolProperties{}, fmt.Errorf("pool %q requires zero-byte alignment", pool)
		}
		if m.config.WriteBufferSize <= 0 || uint64(m.config.WriteBufferSize) < alignment {
			return poolProperties{}, fmt.Errorf("write buffer size %d is smaller than required alignment %d for pool %q", m.config.WriteBufferSize, alignment, pool)
		}
	}

	property := poolProperties{alignment: alignment}
	properties[pool] = property
	return property, nil
}

func (m *ConnectionManager) lookupBlobPool(repo string, blobType BlobType) (*BlobPool, error) {
	poolConfigs, ok := m.poolConfigs[repo]
	match := ""
	if !ok {
		for _, pattern := range m.patterns {
			candidate, matches := pattern.match(repo)
			if !matches {
				continue
			}
			poolConfigs, ok = m.poolConfigs[pattern.key]
			match = candidate
			break
		}
	}
	if !ok {
		return nil, fmt.Errorf("%w for repo %q", errPoolNotConfigured, repo)
	}
	bp := poolConfigs[blobType]
	if bp == nil {
		return nil, fmt.Errorf("%w for repo %q blob type %q", errPoolNotConfigured, repo, blobType)
	}
	return bp.forRepo(repo, match), nil
}

func (m *ConnectionManager) acquire() (*rados.Conn, *connHandle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shuttingDown || m.current == nil {
		return nil, nil, errConnectionUnavailable
	}
	m.current.active++
	return m.current.conn, &connHandle{manager: m, connection: m.current}, nil
}

func (m *ConnectionManager) acquireForRepo(repo string, blobType BlobType) (*rados.Conn, *connHandle, *BlobPool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shuttingDown || m.current == nil || m.poolConfigs == nil {
		return nil, nil, nil, errConnectionUnavailable
	}
	bp, err := m.lookupBlobPool(repo, blobType)
	if err != nil {
		return nil, nil, nil, err
	}
	m.current.active++
	return m.current.conn, &connHandle{manager: m, connection: m.current}, bp, nil
}

func (m *ConnectionManager) reconnectAfterError(connection *connectionState, err error) {
	if !isTransientConnectionError(err) {
		return
	}

	var conn *rados.Conn
	var retry bool
	m.mu.Lock()
	if !m.shuttingDown && m.current == connection {
		m.current = nil
		m.poolConfigs = nil
		m.patterns = nil
		conn = m.retireConnectionLocked(connection)
		retry = m.startRetryLocked()
		serverMetrics.cephConnected.set(0)
		serverMetrics.cephLosses.add(1)
	}
	m.mu.Unlock()

	if conn != nil {
		conn.Shutdown()
	}
	if retry {
		slog.Warn("ceph connection lost, starting reconnect", "error", err)
		go m.retryConnection()
	}
}

func (m *ConnectionManager) release(connection *connectionState) {
	var conn *rados.Conn
	m.mu.Lock()
	connection.active--
	if connection.active == 0 && connection.retired {
		conn = connection.conn
		connection.conn = nil
	}
	m.mu.Unlock()
	if conn != nil {
		conn.Shutdown()
	}
}

func (m *ConnectionManager) retireConnectionLocked(connection *connectionState) *rados.Conn {
	if connection == nil || connection.retired {
		return nil
	}
	connection.retired = true
	if connection.active != 0 {
		return nil
	}
	conn := connection.conn
	connection.conn = nil
	return conn
}

func resolveStriped(layer, repo *bool) bool {
	if layer != nil {
		return *layer
	}
	if repo != nil {
		return *repo
	}
	return true
}

func resolveMaxObjectSize(layer *int64, repo, cluster int64) int64 {
	resolved := cluster
	if layer != nil {
		resolved = *layer
	} else if repo > 0 {
		resolved = repo
	}
	if resolved > cluster {
		return cluster
	}
	return resolved
}

func capConnectionTimeout(conn *rados.Conn) error {
	configured, err := conn.GetConfigOption("client_mount_timeout")
	if err != nil {
		return fmt.Errorf("get client_mount_timeout: %w", err)
	}
	seconds, parseErr := strconv.ParseFloat(strings.TrimSpace(configured), 64)
	if parseErr == nil && seconds > 0 && seconds <= maximumConnectTimeout.Seconds() {
		return nil
	}
	if err := conn.SetConfigOption("client_mount_timeout", strconv.FormatFloat(maximumConnectTimeout.Seconds(), 'f', -1, 64)); err != nil {
		return fmt.Errorf("set client_mount_timeout: %w", err)
	}
	return nil
}

func parseClusterMaxObjectSize(configured string) int64 {
	value, err := strconv.ParseInt(strings.TrimSpace(configured), 10, 64)
	if err != nil || value <= 0 {
		return defaultMaxObjectSize
	}
	return value
}

func isPermanentConnectError(err error) bool {
	if errors.Is(err, rados.ErrPermissionDenied) {
		return true
	}
	var coded interface{ ErrorCode() int }
	if !errors.As(err, &coded) {
		return false
	}
	return coded.ErrorCode() == -int(syscall.EACCES) ||
		coded.ErrorCode() == -int(syscall.EPERM) ||
		coded.ErrorCode() == -int(syscall.EKEYREJECTED) ||
		coded.ErrorCode() == -int(syscall.EKEYEXPIRED) ||
		coded.ErrorCode() == -int(syscall.EOPNOTSUPP)
}

func isTransientConnectionError(err error) bool {
	var coded interface{ ErrorCode() int }
	if !errors.As(err, &coded) {
		return false
	}
	return coded.ErrorCode() == -int(syscall.ENOTCONN) ||
		coded.ErrorCode() == -int(syscall.ETIMEDOUT)
}

func openNamespaceContext(conn *rados.Conn, pool, namespace string, radosCalls *uint64) (*rados.IOContext, error) {
	slog.Debug("rados.OpenIOContext", "pool", pool, "namespace", namespace)
	done := radosObserve("open_ioctx", radosCalls)
	ioctx, err := conn.OpenIOContext(pool)
	done(err)
	if err != nil {
		return nil, err
	}
	ioctx.SetNamespace(namespace)
	return ioctx, nil
}

func classifyOpenContextError(pool string, err error) error {
	if errors.Is(err, rados.ErrNotFound) {
		return fmt.Errorf("%w: %q", errPoolNotConfigured, pool)
	}
	if isTransientConnectionError(err) {
		return fmt.Errorf("%w: %v", errConnectionUnavailable, err)
	}
	return fmt.Errorf("open pool %q: %w", pool, err)
}
