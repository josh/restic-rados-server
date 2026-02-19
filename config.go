package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
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
	AppendOnly     bool               `json:"append_only,omitempty"`
	DisableStriper bool               `json:"disable_striper,omitempty"`
	MaxObjectSize  int64              `json:"max_object_size,omitempty"`
}

type Config struct {
	Verbose         bool                   `json:"verbose,omitempty"`
	Listeners       listenerFlags          `json:"listen,omitempty"`
	Stdio           bool                   `json:"-"`
	ShutdownTimeout Duration               `json:"shutdown_timeout,omitempty"`
	MaxIdleTime     Duration               `json:"max_idle_time,omitempty"`
	LogFile         string                 `json:"log_file,omitempty"`
	Keyring         string                 `json:"keyring,omitempty"`
	ClientID        string                 `json:"client_id,omitempty"`
	CephConf        string                 `json:"ceph_conf,omitempty"`
	ReadBufferSize  int64                  `json:"read_buffer_size,omitempty"`
	WriteBufferSize int64                  `json:"write_buffer_size,omitempty"`
	Repos           map[string]*RepoConfig `json:"repos,omitempty"`
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
	fs.BoolVar(&appendOnly, "append-only", false, "enable append-only mode (delete allowed for locks only)")
	fs.DurationVar(&maxIdleTime, "max-idle-time", 0, "exit after duration with no active connections (e.g., 30s, 5m; 0 = disabled)")
	fs.StringVar(&logFile, "log-file", "", "path to log file (default: stderr)")
	fs.StringVar(&keyringPath, "keyring", "", "path to Ceph keyring file")
	fs.StringVar(&clientID, "id", "", "Ceph client ID (e.g., 'restic' for client.restic)")
	fs.Var(&poolSpecs, "pool", "Pool specification: 'poolname' or 'poolname:types' where types is '*' or comma-separated list (repeatable, or semicolon-separated)")
	fs.StringVar(&cephConf, "ceph-conf", "", "path to ceph.conf file")
	fs.BoolVar(&disableStriper, "disable-striper", false, "disable librados striper for large objects")
	fs.Int64Var(&readBufferSize, "read-buffer-size", defaultReadBufferSize, "buffer size for reading objects in bytes")
	fs.Int64Var(&writeBufferSize, "write-buffer-size", defaultWriteBufferSize, "buffer size for writing objects in bytes")
	fs.Int64Var(&maxObjectSize, "max-object-size", 0, "max object size override (0 = use cluster config or 128MB default)")

	if err := fs.Parse(args); err != nil {
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

	if set["pool"] || set["append-only"] || set["disable-striper"] || set["max-object-size"] {
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
		if set["append-only"] {
			def.AppendOnly = appendOnly
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

func parseBoolEnv(key string) (bool, bool) {
	val := os.Getenv(key)
	if val == "" {
		return false, false
	}
	return val == "true" || val == "1" || val == "yes", true
}

func parseInt64Env(key string) (int64, bool) {
	val := os.Getenv(key)
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
	if v, ok := parseBoolEnv("CEPH_SERVER_VERBOSE"); ok {
		c.Verbose = v
	}
	if v := os.Getenv("CEPH_SERVER_LOG_FILE"); v != "" {
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
	if v, ok := parseInt64Env("CEPH_SERVER_READ_BUFFER_SIZE"); ok {
		c.ReadBufferSize = v
	}
	if v, ok := parseInt64Env("CEPH_SERVER_WRITE_BUFFER_SIZE"); ok {
		c.WriteBufferSize = v
	}

	appendOnly, hasAppendOnly := parseBoolEnv("CEPH_SERVER_APPEND_ONLY")
	disableStriper, hasDisableStriper := parseBoolEnv("CEPH_SERVER_DISABLE_STRIPER")
	maxObjectSize, hasMaxObjectSize := parseInt64Env("CEPH_SERVER_MAX_OBJECT_SIZE")
	envPool := os.Getenv("CEPH_POOL")

	if hasAppendOnly || hasDisableStriper || hasMaxObjectSize || envPool != "" {
		if c.Repos == nil {
			c.Repos = make(map[string]*RepoConfig)
		}
		if c.Repos["default"] == nil {
			c.Repos["default"] = &RepoConfig{}
		}
		def := c.Repos["default"]
		if hasAppendOnly {
			def.AppendOnly = appendOnly
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
		configFile = os.Getenv("CEPH_SERVER_CONFIG")
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

type ServerConfigPools struct {
	Config    string `json:"config"`
	Keys      string `json:"keys"`
	Locks     string `json:"locks"`
	Snapshots string `json:"snapshots"`
	Data      string `json:"data"`
	Index     string `json:"index"`
}

type CephConfig struct {
	KeyringPath string
	ClientID    string
	CephConf    string
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

func parsePoolSpecs(specs []string) (ServerConfigPools, error) {
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
