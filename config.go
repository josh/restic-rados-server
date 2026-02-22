package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
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

type Duration time.Duration

func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) Set(s string) error {
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) String() string {
	return time.Duration(d).String()
}

const (
	defaultReadBufferSize  int64 = 16 * 1024 * 1024
	defaultWriteBufferSize int64 = 16 * 1024 * 1024
)

type poolFlags []string

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

type RepoConfig struct {
	Pools          poolFlags          `json:"pools,omitempty"`
	BlobPools      *ServerConfigPools `json:"blob_pools,omitempty"`
	Access         string             `json:"access,omitempty"`
	DisableStriper bool               `json:"disable_striper,omitempty"`
	MaxObjectSize  int64              `json:"max_object_size,omitempty"`
}

type Config struct {
	Verbose             bool                   `json:"verbose,omitempty"`
	Listeners           listenerFlags          `json:"listen,omitempty"`
	Stdio               bool                   `json:"-"`
	ShutdownTimeout     Duration               `json:"shutdown_timeout,omitempty"`
	MaxIdleTime         Duration               `json:"max_idle_time,omitempty"`
	LogFile             string                 `json:"log_file,omitempty"`
	Keyring             string                 `json:"keyring,omitempty"`
	ClientID            string                 `json:"client_id,omitempty"`
	CephConf            string                 `json:"ceph_conf,omitempty"`
	ReadBufferSize      int64                  `json:"read_buffer_size,omitempty"`
	WriteBufferSize     int64                  `json:"write_buffer_size,omitempty"`
	TailscaleCapability string                 `json:"tailscale_capability,omitempty"`
	Repos               map[string]*RepoConfig `json:"repos,omitempty"`
}

func (c *Config) loadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(c)
}

func (c *Config) loadFromArgs(args []string) (configFile string, showVersion bool, err error) {
	fs := flag.NewFlagSet("restic-rados-server", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "Usage: restic-rados-server [options]\n\nOptions:\n")
		fs.PrintDefaults()
	}

	var verbose bool
	var listeners listenerFlags
	var useStdio bool
	var shutdownTimeout time.Duration
	var access string
	var maxIdleTime time.Duration
	var logFile string
	var keyringPath string
	var clientID string
	var poolSpecs poolFlags
	var cephConf string
	var disableStriper bool
	var readBufferSize int64
	var writeBufferSize int64
	var maxObjectSize int64

	fs.BoolVar(&showVersion, "version", false, "print version and exit")
	fs.StringVar(&configFile, "config", "", "path to JSON configuration file")
	fs.BoolVar(&verbose, "v", false, "enable verbose logging")
	fs.BoolVar(&verbose, "verbose", false, "enable verbose logging")
	fs.Var(&listeners, "listen", "Address or Unix socket path to listen on, repeatable")
	fs.BoolVar(&useStdio, "stdio", false, "use HTTP/2 over stdin/stdout (default when no listeners specified)")
	fs.DurationVar(&shutdownTimeout, "shutdown-timeout", 60*time.Second, "graceful shutdown timeout for listeners")
	fs.StringVar(&access, "access", "", "access level: r/read-only, ra/read-append, rw/read-write")
	fs.DurationVar(&maxIdleTime, "max-idle-time", 0, "exit after duration with no active connections (e.g., 30s, 5m; 0 = disabled)")
	fs.StringVar(&logFile, "log-file", "", "path to log file (default: stderr)")
	fs.StringVar(&keyringPath, "keyring", "", "path to Ceph keyring file")
	fs.StringVar(&clientID, "id", "", "Ceph client ID (e.g., 'restic' for client.restic)")
	fs.Var(&poolSpecs, "pool", "Pool specification: 'pool[/namespace][:types]' where types is '*' or comma-separated list (repeatable, or semicolon-separated)")
	fs.StringVar(&cephConf, "ceph-conf", "", "path to ceph.conf file")
	fs.BoolVar(&disableStriper, "disable-striper", false, "disable librados striper for large objects")
	fs.Int64Var(&readBufferSize, "read-buffer-size", defaultReadBufferSize, "buffer size for reading objects in bytes")
	fs.Int64Var(&writeBufferSize, "write-buffer-size", defaultWriteBufferSize, "buffer size for writing objects in bytes")
	fs.Int64Var(&maxObjectSize, "max-object-size", 0, "max object size override (0 = use cluster config or 128MB default)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(os.Stderr)
			fs.Usage()
		}
		return "", false, err
	}

	set := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		set[f.Name] = true
	})

	if set["verbose"] || set["v"] {
		c.Verbose = verbose
	}
	if set["listen"] {
		c.Listeners = listeners
	}
	if set["stdio"] {
		c.Stdio = useStdio
	}
	if set["shutdown-timeout"] {
		c.ShutdownTimeout = Duration(shutdownTimeout)
	}
	if set["max-idle-time"] {
		c.MaxIdleTime = Duration(maxIdleTime)
	}
	if set["log-file"] {
		c.LogFile = logFile
	}
	if set["keyring"] {
		c.Keyring = keyringPath
	}
	if set["id"] {
		c.ClientID = clientID
	}
	if set["ceph-conf"] {
		c.CephConf = cephConf
	}
	if set["read-buffer-size"] {
		c.ReadBufferSize = readBufferSize
	}
	if set["write-buffer-size"] {
		c.WriteBufferSize = writeBufferSize
	}

	if set["pool"] || set["access"] || set["disable-striper"] || set["max-object-size"] {
		if c.Repos == nil {
			c.Repos = make(map[string]*RepoConfig)
		}
		if c.Repos["default"] == nil {
			c.Repos["default"] = &RepoConfig{}
		}
		def := c.Repos["default"]
		if set["pool"] {
			def.Pools = poolSpecs
			def.BlobPools = nil
		}
		if set["access"] {
			def.Access = access
		}
		if set["disable-striper"] {
			def.DisableStriper = disableStriper
		}
		if set["max-object-size"] {
			def.MaxObjectSize = maxObjectSize
		}
	}

	return configFile, showVersion, nil
}

var envPrefixes = []string{"RESTIC_RADOS_SERVER_", "RESTIC_CEPH_SERVER_", "CEPH_RESTIC_SERVER_", "RADOS_RESTIC_SERVER_"}

func getEnv(suffix string) string {
	for _, prefix := range envPrefixes {
		if v := os.Getenv(prefix + suffix); v != "" {
			return v
		}
	}
	return ""
}

func parseBoolEnv(suffix string) (bool, bool) {
	val := getEnv(suffix)
	if val == "" {
		return false, false
	}
	return val == "true" || val == "1" || val == "yes", true
}

func parseInt64Env(suffix string) (int64, bool) {
	val := getEnv(suffix)
	if val == "" {
		return 0, false
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func (c *Config) loadFromEnv() {
	if v, ok := parseBoolEnv("VERBOSE"); ok {
		c.Verbose = v
	}
	if v := getEnv("LOG_FILE"); v != "" {
		c.LogFile = v
	}
	if v := os.Getenv("CEPH_KEYRING"); v != "" {
		c.Keyring = v
	}
	if v := os.Getenv("CEPH_ID"); v != "" {
		c.ClientID = v
	}
	if v := os.Getenv("CEPH_CONF"); v != "" {
		c.CephConf = v
	}
	if v, ok := parseInt64Env("READ_BUFFER_SIZE"); ok {
		c.ReadBufferSize = v
	}
	if v, ok := parseInt64Env("WRITE_BUFFER_SIZE"); ok {
		c.WriteBufferSize = v
	}

	envAccess := getEnv("ACCESS")
	disableStriper, hasDisableStriper := parseBoolEnv("DISABLE_STRIPER")
	maxObjectSize, hasMaxObjectSize := parseInt64Env("MAX_OBJECT_SIZE")
	envPool := getEnv("POOL")

	if envAccess != "" || hasDisableStriper || hasMaxObjectSize || envPool != "" {
		if c.Repos == nil {
			c.Repos = make(map[string]*RepoConfig)
		}
		if c.Repos["default"] == nil {
			c.Repos["default"] = &RepoConfig{}
		}
		def := c.Repos["default"]
		if envAccess != "" {
			def.Access = envAccess
		}
		if hasDisableStriper {
			def.DisableStriper = disableStriper
		}
		if envPool != "" {
			def.Pools = nil
			def.BlobPools = nil
			for _, spec := range strings.Split(envPool, ";") {
				spec = strings.TrimSpace(spec)
				if spec != "" {
					def.Pools = append(def.Pools, spec)
				}
			}
		}
		if hasMaxObjectSize {
			def.MaxObjectSize = maxObjectSize
		}
	}
}

func loadConfig(args []string) (Config, bool, error) {
	config := Config{
		ShutdownTimeout: Duration(60 * time.Second),
		ReadBufferSize:  defaultReadBufferSize,
		WriteBufferSize: defaultWriteBufferSize,
	}

	configFile, showVersion, err := config.loadFromArgs(args)
	if err != nil {
		return Config{}, false, err
	}

	if showVersion {
		return Config{}, true, nil
	}

	if configFile == "" {
		configFile = getEnv("CONFIG")
	}

	if configFile != "" {
		if err := config.loadFromFile(configFile); err != nil {
			return Config{}, false, fmt.Errorf("failed to load config file %s: %w", configFile, err)
		}
	}

	config.loadFromEnv()
	_, _, _ = config.loadFromArgs(args)

	if config.ReadBufferSize <= 0 {
		return Config{}, false, fmt.Errorf("read-buffer-size must be positive, got %d", config.ReadBufferSize)
	}

	if config.WriteBufferSize <= 0 {
		return Config{}, false, fmt.Errorf("write-buffer-size must be positive, got %d", config.WriteBufferSize)
	}

	if err := config.normalizeRepos(); err != nil {
		return Config{}, false, err
	}

	return config, false, nil
}

func isReservedRepoName(name string) bool {
	switch name {
	case "keys", "locks", "snapshots", "data", "index", "config":
		return true
	default:
		return false
	}
}

func (c *Config) normalizeRepos() error {
	for name, repo := range c.Repos {
		if name != "default" && isReservedRepoName(name) {
			return fmt.Errorf("reserved repo name %q (conflicts with blob type)", name)
		}

		if repo.Access == "" {
			repo.Access = "rw"
		}
		switch repo.Access {
		case "r", "read-only":
			repo.Access = "r"
		case "ra", "read-append":
			repo.Access = "ra"
		case "rw", "read-write":
			repo.Access = "rw"
		default:
			return fmt.Errorf("repo %q: invalid access %q (must be r, ra, or rw)", name, repo.Access)
		}

		if repo.BlobPools == nil && len(repo.Pools) > 0 {
			pools, err := parsePoolSpecs(repo.Pools)
			if err != nil {
				return fmt.Errorf("repo %q: invalid pool configuration: %v", name, err)
			}
			repo.BlobPools = &pools
		}

		if repo.MaxObjectSize < 0 {
			return fmt.Errorf("repo %q: max-object-size cannot be negative, got %d", name, repo.MaxObjectSize)
		}
	}
	return nil
}

func (c *Config) validateTailscaleCapabilityListeners() {
	if c.TailscaleCapability == "" {
		return
	}

	if c.Stdio {
		slog.Warn("tailscale_capability is not supported in stdio mode; capability headers will be ignored")
	}

	for _, l := range c.Listeners {
		if l.kind != listenerTypeTCP {
			continue
		}
		host, _, err := net.SplitHostPort(l.address)
		if err != nil {
			continue
		}
		if host == "" || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			slog.Warn("tailscale_capability with non-loopback TCP listener; ensure Tailscale is the only route to this address", "address", l.address)
		}
	}
}

type BlobPoolConfig struct {
	Pool          string `json:"pool"`
	Namespace     string `json:"namespace,omitempty"`
	Striped       *bool  `json:"striped,omitempty"`
	MaxObjectSize *int64 `json:"max_object_size,omitempty"`
}

type ServerConfigPools struct {
	Config    BlobPoolConfig `json:"config"`
	Keys      BlobPoolConfig `json:"keys"`
	Locks     BlobPoolConfig `json:"locks"`
	Snapshots BlobPoolConfig `json:"snapshots"`
	Data      BlobPoolConfig `json:"data"`
	Index     BlobPoolConfig `json:"index"`
}

type CephConfig struct {
	KeyringPath string
	ClientID    string
	CephConf    string
}

type BlobPool struct {
	Pool          string
	Namespace     string
	Striped       bool
	Alignment     uint64
	MaxObjectSize int64
}

func (p *ServerConfigPools) getPoolForType(bt BlobType) BlobPoolConfig {
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

func parsePoolSpecs(specs []string) (ServerConfigPools, error) {
	if len(specs) == 0 {
		return ServerConfigPools{}, errors.New("no pool specifications provided")
	}

	typeToConfig := make(map[BlobType]BlobPoolConfig)
	var catchAll *BlobPoolConfig

	for _, spec := range specs {
		poolName, namespace, types, err := parsePoolSpec(spec)
		if err != nil {
			return ServerConfigPools{}, err
		}

		bpc := BlobPoolConfig{Pool: poolName, Namespace: namespace}

		if len(types) == 0 || (len(types) == 1 && types[0] == "*") {
			if catchAll != nil {
				return ServerConfigPools{}, fmt.Errorf("multiple catch-all pools specified: %q and %q", catchAll.Pool, poolName)
			}
			catchAll = &bpc
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
			if existing, ok := typeToConfig[blobType]; ok {
				return ServerConfigPools{}, fmt.Errorf("blob type %q assigned to multiple pools: %q and %q", t, existing.Pool, poolName)
			}
			typeToConfig[blobType] = bpc
		}
	}

	for _, bt := range AllBlobTypes {
		if _, ok := typeToConfig[bt]; !ok && catchAll != nil {
			typeToConfig[bt] = *catchAll
		}
	}

	return ServerConfigPools{
		Config:    typeToConfig[BlobTypeConfig],
		Keys:      typeToConfig[BlobTypeKeys],
		Locks:     typeToConfig[BlobTypeLocks],
		Snapshots: typeToConfig[BlobTypeSnapshots],
		Data:      typeToConfig[BlobTypeData],
		Index:     typeToConfig[BlobTypeIndex],
	}, nil
}

func parsePoolSpec(spec string) (poolName, namespace string, types []string, err error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "", "", nil, errors.New("empty pool specification")
	}

	colonIdx := strings.Index(spec, ":")
	poolPart := spec
	var typesPart string
	if colonIdx != -1 {
		poolPart = spec[:colonIdx]
		typesPart = strings.TrimSpace(spec[colonIdx+1:])
	}

	poolPart = strings.TrimSpace(poolPart)
	if slashIdx := strings.Index(poolPart, "/"); slashIdx != -1 {
		poolName = poolPart[:slashIdx]
		namespace = poolPart[slashIdx+1:]
		if poolName == "" {
			return "", "", nil, fmt.Errorf("empty pool name in specification: %q", spec)
		}
		if namespace == "" {
			return "", "", nil, fmt.Errorf("empty namespace in specification: %q", spec)
		}
	} else {
		poolName = poolPart
	}

	if poolName == "" {
		return "", "", nil, fmt.Errorf("empty pool name in specification: %q", spec)
	}

	if colonIdx == -1 {
		return poolName, namespace, []string{"*"}, nil
	}

	if typesPart == "" {
		return "", "", nil, fmt.Errorf("empty types list in specification: %q", spec)
	}

	if typesPart == "*" {
		return poolName, namespace, []string{"*"}, nil
	}

	for _, t := range strings.Split(typesPart, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		types = append(types, t)
	}

	if len(types) == 0 {
		return "", "", nil, fmt.Errorf("no valid types in specification: %q", spec)
	}

	return poolName, namespace, types, nil
}

func isValidBlobTypeForMapping(bt BlobType) bool {
	for _, valid := range AllBlobTypes {
		if bt == valid {
			return true
		}
	}
	return false
}
