package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	defaultReadBufferSize  int64 = 16 * 1024 * 1024
	defaultWriteBufferSize int64 = 16 * 1024 * 1024
)

var logger *slog.Logger

var version = "0.3.1"

type Config struct {
	Verbose         bool
	Listeners       listenerFlags
	UseStdio        bool
	ShutdownTimeout time.Duration
	AppendOnly      bool
	MaxIdleTime     time.Duration
	LogFile         string
	KeyringPath     string
	ClientID        string
	PoolSpecs       poolFlags
	CephConf        string
	EnableStriper   bool
	ReadBufferSize  int64
	WriteBufferSize int64
	MaxObjectSize   int64
}

type poolFlags []string

func initLogger(verbose bool, logFilePath string) error {
	logOutput := io.Writer(os.Stderr)

	if logFilePath != "" {
		file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("failed to open log file %s: %w", logFilePath, err)
		}
		logOutput = file
	}

	logLevel := slog.LevelInfo
	if verbose {
		logLevel = slog.LevelDebug
	}

	handler := slog.NewTextHandler(logOutput, &slog.HandlerOptions{
		Level: logLevel,
	})
	logger = slog.New(handler)
	slog.SetDefault(logger)

	return nil
}

func parseBoolEnv(key string) bool {
	val := os.Getenv(key)
	return val == "true" || val == "1" || val == "yes"
}

func parseInt64Env(key string, defaultVal int64) int64 {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return defaultVal
	}
	return parsed
}

func (p *poolFlags) String() string {
	if p == nil {
		return ""
	}
	return strings.Join(*p, ";")
}

func (p *poolFlags) Set(value string) error {
	for _, spec := range strings.Split(value, ";") {
		spec = strings.TrimSpace(spec)
		if spec != "" {
			*p = append(*p, spec)
		}
	}
	return nil
}

func parseConfig() (Config, error) {
	var showVersion bool
	var verbose bool
	var listeners listenerFlags
	var useStdio bool
	var shutdownTimeout time.Duration
	var appendOnly bool
	var maxIdleTime time.Duration
	var logFile string
	var keyringPath string
	var clientID string
	var poolSpecs poolFlags
	var cephConf string
	var enableStriper bool
	var readBufferSize int64
	var writeBufferSize int64
	var maxObjectSize int64

	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.BoolVar(&verbose, "v", false, "enable verbose logging")
	flag.BoolVar(&verbose, "verbose", false, "enable verbose logging")
	flag.Var(&listeners, "listen", "Address or Unix socket path to listen on, repeatable")
	flag.BoolVar(&useStdio, "stdio", false, "use HTTP/2 over stdin/stdout (default when no listeners specified)")
	flag.DurationVar(&shutdownTimeout, "shutdown-timeout", 60*time.Second, "graceful shutdown timeout for listeners")
	flag.BoolVar(&appendOnly, "append-only", false, "enable append-only mode (delete allowed for locks only)")
	flag.DurationVar(&maxIdleTime, "max-idle-time", 0, "exit after duration with no active connections (e.g., 30s, 5m; 0 = disabled)")
	flag.StringVar(&logFile, "log-file", "", "path to log file (default: stderr)")
	flag.StringVar(&keyringPath, "keyring", "", "path to Ceph keyring file")
	flag.StringVar(&clientID, "id", "", "Ceph client ID (e.g., 'restic' for client.restic)")
	flag.Var(&poolSpecs, "pool", "Pool specification: 'poolname' or 'poolname:types' where types is '*' or comma-separated list (repeatable, or semicolon-separated)")
	flag.StringVar(&cephConf, "ceph-conf", "", "path to ceph.conf file")
	flag.BoolVar(&enableStriper, "enable-striper", false, "use librados striper for large objects")
	flag.Int64Var(&readBufferSize, "read-buffer-size", defaultReadBufferSize, "buffer size for reading objects in bytes")
	flag.Int64Var(&writeBufferSize, "write-buffer-size", defaultWriteBufferSize, "buffer size for writing objects in bytes")
	flag.Int64Var(&maxObjectSize, "max-object-size", 0, "max object size override (0 = use cluster config or 128MB default)")
	flag.Parse()

	if showVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	if !verbose {
		verbose = parseBoolEnv("CEPH_SERVER_VERBOSE")
	}

	if !appendOnly {
		appendOnly = parseBoolEnv("CEPH_SERVER_APPEND_ONLY")
	}

	if !enableStriper {
		enableStriper = parseBoolEnv("CEPH_SERVER_ENABLE_STRIPER")
	}

	if logFile == "" {
		logFile = os.Getenv("CEPH_SERVER_LOG_FILE")
	}

	if keyringPath == "" {
		keyringPath = os.Getenv("CEPH_KEYRING")
	}

	if clientID == "" {
		clientID = os.Getenv("CEPH_ID")
	}

	if len(poolSpecs) == 0 {
		if envPool := os.Getenv("CEPH_POOL"); envPool != "" {
			for _, spec := range strings.Split(envPool, ";") {
				spec = strings.TrimSpace(spec)
				if spec != "" {
					poolSpecs = append(poolSpecs, spec)
				}
			}
		}
	}

	if cephConf == "" {
		cephConf = os.Getenv("CEPH_CONF")
	}

	if readBufferSize == defaultReadBufferSize {
		readBufferSize = parseInt64Env("CEPH_SERVER_READ_BUFFER_SIZE", readBufferSize)
	}

	if writeBufferSize == defaultWriteBufferSize {
		writeBufferSize = parseInt64Env("CEPH_SERVER_WRITE_BUFFER_SIZE", writeBufferSize)
	}

	if maxObjectSize == 0 {
		maxObjectSize = parseInt64Env("CEPH_SERVER_MAX_OBJECT_SIZE", maxObjectSize)
	}

	if readBufferSize <= 0 {
		return Config{}, fmt.Errorf("read-buffer-size must be positive, got %d", readBufferSize)
	}

	if writeBufferSize <= 0 {
		return Config{}, fmt.Errorf("write-buffer-size must be positive, got %d", writeBufferSize)
	}

	if maxObjectSize < 0 {
		return Config{}, fmt.Errorf("max-object-size cannot be negative, got %d", maxObjectSize)
	}

	return Config{
		Verbose:         verbose,
		Listeners:       listeners,
		UseStdio:        useStdio,
		ShutdownTimeout: shutdownTimeout,
		AppendOnly:      appendOnly,
		MaxIdleTime:     maxIdleTime,
		LogFile:         logFile,
		KeyringPath:     keyringPath,
		ClientID:        clientID,
		PoolSpecs:       poolSpecs,
		CephConf:        cephConf,
		EnableStriper:   enableStriper,
		ReadBufferSize:  readBufferSize,
		WriteBufferSize: writeBufferSize,
		MaxObjectSize:   maxObjectSize,
	}, nil
}

func ParsePoolSpecs(specs []string) (ServerConfigPools, error) {
	if len(specs) == 0 {
		return ServerConfigPools{}, errors.New("no pool specifications provided")
	}

	typeToPool := make(map[BlobType]string)
	var catchAllPool string

	for _, spec := range specs {
		poolName, types, err := parsePoolSpec(spec)
		if err != nil {
			return ServerConfigPools{}, err
		}

		if len(types) == 0 || (len(types) == 1 && types[0] == "*") {
			if catchAllPool != "" {
				return ServerConfigPools{}, fmt.Errorf("multiple catch-all pools specified: %q and %q", catchAllPool, poolName)
			}
			catchAllPool = poolName
			continue
		}

		for _, t := range types {
			if t == "*" {
				return ServerConfigPools{}, fmt.Errorf("pool %q: wildcard '*' cannot be mixed with explicit types", poolName)
			}
			blobType := BlobType(t)
			if !isValidBlobTypeForMapping(blobType) {
				return ServerConfigPools{}, fmt.Errorf("pool %q: unknown blob type %q", poolName, t)
			}
			if existing, ok := typeToPool[blobType]; ok {
				return ServerConfigPools{}, fmt.Errorf("blob type %q assigned to multiple pools: %q and %q", t, existing, poolName)
			}
			typeToPool[blobType] = poolName
		}
	}

	for _, bt := range AllBlobTypes {
		if _, ok := typeToPool[bt]; !ok && catchAllPool != "" {
			typeToPool[bt] = catchAllPool
		}
	}

	return ServerConfigPools{
		Config:    typeToPool[BlobTypeConfig],
		Keys:      typeToPool[BlobTypeKeys],
		Locks:     typeToPool[BlobTypeLocks],
		Snapshots: typeToPool[BlobTypeSnapshots],
		Data:      typeToPool[BlobTypeData],
		Index:     typeToPool[BlobTypeIndex],
	}, nil
}

func parsePoolSpec(spec string) (poolName string, types []string, err error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "", nil, errors.New("empty pool specification")
	}

	colonIdx := strings.Index(spec, ":")
	if colonIdx == -1 {
		return spec, []string{"*"}, nil
	}

	poolName = strings.TrimSpace(spec[:colonIdx])
	if poolName == "" {
		return "", nil, fmt.Errorf("empty pool name in specification: %q", spec)
	}

	typesPart := strings.TrimSpace(spec[colonIdx+1:])
	if typesPart == "" {
		return "", nil, fmt.Errorf("empty types list in specification: %q", spec)
	}

	if typesPart == "*" {
		return poolName, []string{"*"}, nil
	}

	for _, t := range strings.Split(typesPart, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		types = append(types, t)
	}

	if len(types) == 0 {
		return "", nil, fmt.Errorf("no valid types in specification: %q", spec)
	}

	return poolName, types, nil
}

func isValidBlobTypeForMapping(bt BlobType) bool {
	for _, valid := range AllBlobTypes {
		if bt == valid {
			return true
		}
	}
	return false
}

func main() {
	config, err := parseConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err := initLogger(config.Verbose, config.LogFile); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if len(config.PoolSpecs) == 0 {
		fmt.Fprintln(os.Stderr, "Ceph pool not set (use --pool or CEPH_POOL)")
		os.Exit(1)
	}

	cliPools, err := ParsePoolSpecs(config.PoolSpecs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid pool configuration: %v\n", err)
		os.Exit(1)
	}

	if cliPools.Config == "" {
		fmt.Fprintln(os.Stderr, "config pool must be specified (use 'poolname' or 'poolname:config,...')")
		os.Exit(1)
	}

	cephConfig := CephConfig{
		ConfigPoolName: cliPools.Config,
		KeyringPath:    config.KeyringPath,
		ClientID:       config.ClientID,
		CephConf:       config.CephConf,
		MaxObjectSize:  config.MaxObjectSize,
	}

	connMgr := NewConnectionManager(cephConfig)
	defer connMgr.Shutdown()

	maxWriteSize, err := connMgr.GetMaxWriteSize()
	if err != nil {
		slog.Warn("failed to get max write size for validation", "error", err)
	} else if config.WriteBufferSize > maxWriteSize {
		slog.Warn("write buffer size exceeds cluster max write size, writes may be chunked or fail",
			"write_buffer_size", config.WriteBufferSize,
			"cluster_max_write_size", maxWriteSize)
	}

	h := &Handler{
		connMgr: connMgr,
		serverConfigTemplate: &ServerConfig{
			Version:        1,
			Pools:          cliPools,
			StriperEnabled: config.EnableStriper,
		},
		appendOnly:      config.AppendOnly,
		readBufferPool:  NewBufferPool(config.ReadBufferSize),
		writeBufferPool: NewBufferPool(config.WriteBufferSize),
	}

	mux := http.NewServeMux()
	h.setupRoutes(mux)

	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	systemdSpecs, err := systemdListeners()
	if err != nil {
		slog.Error("failed to get systemd listeners", "error", err)
		os.Exit(1)
	}

	config.Listeners = append(config.Listeners, systemdSpecs...)
	if config.UseStdio && len(config.Listeners) > 0 {
		slog.Error("--stdio cannot be combined with --listen")
		os.Exit(1)
	}
	hasConfiguredListeners := len(config.Listeners) > 0

	if !config.UseStdio && !hasConfiguredListeners {
		config.UseStdio = true
	}

	if config.UseStdio && config.MaxIdleTime > 0 {
		slog.Error("--max-idle-time is not supported in stdio mode")
		os.Exit(1)
	}

	var monitor *idleMonitor
	if config.MaxIdleTime > 0 {
		monitor = newIdleMonitor(config.MaxIdleTime)
		defer monitor.Stop()
		go func() {
			select {
			case <-monitor.Done():
				cancel()
			case <-ctx.Done():
				monitor.Stop()
			}
		}()
	}

	if config.UseStdio {
		for _, cfg := range config.Listeners {
			cfg.Close()
		}

		stdioCfg := listenerConfig{
			kind: listenerTypeStdio,
			raw:  "stdio",
		}
		if err := stdioCfg.Serve(ctx, mux, config.ShutdownTimeout, monitor); err != nil && ctx.Err() == nil {
			slog.Error("stdio server error", "error", err)
			os.Exit(1)
		}
	} else {
		if err := serveAllListeners(ctx, cancel, config.Listeners, mux, config.ShutdownTimeout, monitor); err != nil {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}
}
