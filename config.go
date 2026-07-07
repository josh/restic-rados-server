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
	"unicode"
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

type poolsConfig map[string][]string

type RepoConfig struct {
	Pools         poolsConfig        `json:"pools,omitempty"`
	BlobPools     *ServerConfigPools `json:"blob_pools,omitempty"`
	Access        string             `json:"access,omitempty"`
	Striper       *bool              `json:"striper,omitempty"`
	MaxObjectSize int64              `json:"max_object_size,omitempty"`

	poolSpecs []string
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
	Tailscale       *TailscaleConfig       `json:"tailscale,omitempty"`
	Repos           map[string]*RepoConfig `json:"repos,omitempty"`
}

type TailscaleConfig struct {
	Socket         string `json:"socket,omitempty"`
	HTTPS          *bool  `json:"https,omitempty"`
	Port           int    `json:"port,omitempty"`
	UpstreamSocket string `json:"upstream_socket,omitempty"`
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

func (c *Config) defaultRepo() *RepoConfig {
	if c.Repos == nil {
		c.Repos = make(map[string]*RepoConfig)
	}
	if c.Repos["default"] == nil {
		c.Repos["default"] = &RepoConfig{}
	}
	return c.Repos["default"]
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
	var striper bool
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
	fs.Var(&poolSpecs, "pool", "Pool specification: 'pool[/namespace][//lowerpool[/namespace]][:types]' where types is '*' or comma-separated list (repeatable, or semicolon-separated)")
	fs.StringVar(&cephConf, "ceph-conf", "", "path to ceph.conf file")
	fs.BoolVar(&striper, "striper", true, "enable librados striper for large objects")
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

	if set["pool"] || set["access"] || set["striper"] || set["max-object-size"] {
		def := c.defaultRepo()
		if set["pool"] {
			def.poolSpecs = poolSpecs
			def.Pools = nil
			def.BlobPools = nil
		}
		if set["access"] {
			def.Access = access
		}
		if set["striper"] {
			def.Striper = &striper
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
	striper, hasStriper := parseBoolEnv("STRIPER")
	maxObjectSize, hasMaxObjectSize := parseInt64Env("MAX_OBJECT_SIZE")
	envPool := getEnv("POOL")

	if envAccess != "" || hasStriper || hasMaxObjectSize || envPool != "" {
		def := c.defaultRepo()
		if envAccess != "" {
			def.Access = envAccess
		}
		if hasStriper {
			def.Striper = &striper
		}
		if envPool != "" {
			var specs []string
			for _, spec := range strings.Split(envPool, ";") {
				spec = strings.TrimSpace(spec)
				if spec != "" {
					specs = append(specs, spec)
				}
			}
			def.poolSpecs = specs
			def.Pools = nil
			def.BlobPools = nil
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

	if config.Tailscale != nil && (config.Tailscale.Port < 0 || config.Tailscale.Port > 65535) {
		return Config{}, false, fmt.Errorf("tailscale port must be between 0 and 65535, got %d", config.Tailscale.Port)
	}

	if err := config.normalizeRepos(); err != nil {
		return Config{}, false, err
	}

	return config, false, nil
}

func isReservedRepoName(name string) bool {
	switch name {
	case "keys", "locks", "snapshots", "data", "index", "config", "healthz", "readyz":
		return true
	default:
		return false
	}
}

func (c *Config) normalizeRepos() error {
	for name, repo := range c.Repos {
		if name == "" || strings.ContainsAny(name, "/{} ") || strings.ContainsFunc(name, unicode.IsControl) {
			return fmt.Errorf("invalid repo name %q (must not be empty or contain '/', '{', '}', spaces, or control characters)", name)
		}
		if name == "." || name == ".." {
			return fmt.Errorf("invalid repo name %q (must not be \".\" or \"..\")", name)
		}
		if name != "default" && isReservedRepoName(name) {
			return fmt.Errorf("reserved repo name %q (conflicts with server path)", name)
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

		if repo.BlobPools == nil && len(repo.poolSpecs) > 0 {
			pools, err := poolSpecsToPoolsConfig(repo.poolSpecs)
			if err != nil {
				return fmt.Errorf("repo %q: invalid pool configuration: %v", name, err)
			}
			repo.Pools = pools
		}

		if repo.BlobPools == nil && len(repo.Pools) > 0 {
			pools, err := parsePoolsConfig(repo.Pools)
			if err != nil {
				return fmt.Errorf("repo %q: invalid pool configuration: %v", name, err)
			}
			repo.BlobPools = &pools
		} else if repo.BlobPools != nil {
			if err := repo.BlobPools.normalizeLayers(); err != nil {
				return fmt.Errorf("repo %q: %v", name, err)
			}
		}

		if repo.MaxObjectSize < 0 {
			return fmt.Errorf("repo %q: max-object-size cannot be negative, got %d", name, repo.MaxObjectSize)
		}
	}
	return nil
}

type LayerPoolConfig struct {
	Pool          string `json:"pool"`
	Namespace     string `json:"namespace,omitempty"`
	Striped       *bool  `json:"striped,omitempty"`
	MaxObjectSize *int64 `json:"max_object_size,omitempty"`
}

type BlobPoolConfig struct {
	Pool          string           `json:"pool"`
	Namespace     string           `json:"namespace,omitempty"`
	Striped       *bool            `json:"striped,omitempty"`
	MaxObjectSize *int64           `json:"max_object_size,omitempty"`
	Upper         *LayerPoolConfig `json:"upper,omitempty"`
	Lower         *LayerPoolConfig `json:"lower,omitempty"`
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
	Lower         *BlobPool
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

func (p *ServerConfigPools) normalizeLayers() error {
	fields := []*BlobPoolConfig{&p.Config, &p.Keys, &p.Locks, &p.Snapshots, &p.Data, &p.Index}
	for i, bt := range AllBlobTypes {
		bpc := fields[i]
		if bpc.Upper == nil && bpc.Lower == nil {
			continue
		}
		if bpc.Upper == nil {
			return fmt.Errorf("blob type %q: lower layer requires an explicit upper layer", bt)
		}
		if bpc.Lower == nil {
			return fmt.Errorf("blob type %q: upper layer requires a lower layer (use the flat pool form for a single layer)", bt)
		}
		if bpc.Pool != "" || bpc.Namespace != "" || bpc.Striped != nil || bpc.MaxObjectSize != nil {
			return fmt.Errorf("blob type %q: cannot combine pool with upper/lower layers", bt)
		}
		if bpc.Upper.Pool == "" {
			return fmt.Errorf("blob type %q: upper pool name cannot be empty", bt)
		}
		if bpc.Lower.Pool == "" {
			return fmt.Errorf("blob type %q: lower pool name cannot be empty", bt)
		}
		if bpc.Upper.Pool == bpc.Lower.Pool && bpc.Upper.Namespace == bpc.Lower.Namespace {
			return fmt.Errorf("blob type %q: lower layer must differ from upper layer", bt)
		}

		bpc.Pool = bpc.Upper.Pool
		bpc.Namespace = bpc.Upper.Namespace
		bpc.Striped = bpc.Upper.Striped
		bpc.MaxObjectSize = bpc.Upper.MaxObjectSize
		bpc.Upper = nil
	}

	for i, bt := range AllBlobTypes {
		bpc := fields[i]
		if bpc.MaxObjectSize != nil && *bpc.MaxObjectSize <= 0 {
			return fmt.Errorf("blob type %q: max_object_size must be positive, got %d", bt, *bpc.MaxObjectSize)
		}
		if bpc.Lower != nil && bpc.Lower.MaxObjectSize != nil && *bpc.Lower.MaxObjectSize <= 0 {
			return fmt.Errorf("blob type %q: lower layer max_object_size must be positive, got %d", bt, *bpc.Lower.MaxObjectSize)
		}
	}
	return nil
}

func poolSpecsToPoolsConfig(specs []string) (poolsConfig, error) {
	result := make(poolsConfig)
	for _, spec := range specs {
		key, types, err := parsePoolSpec(spec)
		if err != nil {
			return nil, err
		}
		for _, t := range types {
			if t == "*" {
				if len(types) > 1 {
					return nil, fmt.Errorf("pool %q: wildcard '*' cannot be mixed with explicit types", key)
				}
				continue
			}
			if !isValidBlobTypeForMapping(BlobType(t)) {
				return nil, fmt.Errorf("pool %q: unknown blob type %q", key, t)
			}
		}
		for _, t := range types {
			if !slices.Contains(result[key], t) {
				result[key] = append(result[key], t)
			}
		}
		if len(result[key]) > 1 && slices.Contains(result[key], "*") {
			result[key] = []string{"*"}
		}
	}
	return result, nil
}

func splitPoolKey(key string) (pool, namespace string) {
	if idx := strings.Index(key, "/"); idx != -1 {
		return key[:idx], key[idx+1:]
	}
	return key, ""
}

func parsePoolKey(key string) (BlobPoolConfig, error) {
	upperPart, lowerPart, hasLower := strings.Cut(key, "//")

	poolName, namespace := splitPoolKey(upperPart)
	if poolName == "" {
		return BlobPoolConfig{}, fmt.Errorf("empty pool name in specification: %q", key)
	}
	if strings.Contains(upperPart, "/") && namespace == "" {
		return BlobPoolConfig{}, fmt.Errorf("empty namespace in specification: %q", key)
	}
	if strings.HasPrefix(namespace, "/") {
		return BlobPoolConfig{}, fmt.Errorf("namespace cannot start with '/' in specification: %q", key)
	}
	if strings.Contains(namespace, "/") {
		return BlobPoolConfig{}, fmt.Errorf("namespace cannot contain '/' in specification: %q", key)
	}
	bpc := BlobPoolConfig{Pool: poolName, Namespace: namespace}

	if hasLower {
		lowerPool, lowerNamespace := splitPoolKey(lowerPart)
		if lowerPool == "" {
			return BlobPoolConfig{}, fmt.Errorf("empty lower pool name in specification: %q", key)
		}
		if strings.Contains(lowerPart, "/") && lowerNamespace == "" {
			return BlobPoolConfig{}, fmt.Errorf("empty namespace in specification: %q", key)
		}
		if strings.HasPrefix(lowerNamespace, "/") {
			return BlobPoolConfig{}, fmt.Errorf("namespace cannot start with '/' in specification: %q", key)
		}
		if strings.Contains(lowerNamespace, "/") {
			return BlobPoolConfig{}, fmt.Errorf("namespace cannot contain '/' in specification: %q", key)
		}
		if lowerPool == poolName && lowerNamespace == namespace {
			return BlobPoolConfig{}, fmt.Errorf("lower layer must differ from upper layer in specification: %q", key)
		}
		bpc.Lower = &LayerPoolConfig{Pool: lowerPool, Namespace: lowerNamespace}
	}

	return bpc, nil
}

func parsePoolsConfig(pc poolsConfig) (ServerConfigPools, error) {
	if len(pc) == 0 {
		return ServerConfigPools{}, errors.New("no pool specifications provided")
	}

	typeToConfig := make(map[BlobType]BlobPoolConfig)
	typeToKey := make(map[BlobType]string)
	var catchAll *BlobPoolConfig

	for key, types := range pc {
		bpc, err := parsePoolKey(key)
		if err != nil {
			return ServerConfigPools{}, err
		}
		poolName := bpc.Pool

		if len(types) == 0 || types == nil {
			return ServerConfigPools{}, fmt.Errorf("invalid pool specification: %q", key)
		}

		if len(types) == 1 && types[0] == "*" {
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
			if existingKey, ok := typeToKey[blobType]; ok {
				if existingKey == key {
					continue
				}
				return ServerConfigPools{}, fmt.Errorf("blob type %q assigned to multiple pools: %q and %q", t, existingKey, key)
			}
			typeToKey[blobType] = key
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

func parsePoolSpec(spec string) (key string, types []string, err error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "", nil, errors.New("empty pool specification")
	}

	colonIdx := strings.Index(spec, ":")
	poolPart := spec
	var typesPart string
	if colonIdx != -1 {
		poolPart = spec[:colonIdx]
		typesPart = strings.TrimSpace(spec[colonIdx+1:])
	}

	poolPart = strings.TrimSpace(poolPart)

	upperPart, lowerPart, hasLower := strings.Cut(poolPart, "//")
	if err := validatePoolLayer(upperPart, spec); err != nil {
		return "", nil, err
	}
	if hasLower {
		if lowerPart == "" {
			return "", nil, fmt.Errorf("empty lower pool name in specification: %q", spec)
		}
		if err := validatePoolLayer(lowerPart, spec); err != nil {
			return "", nil, err
		}
		if lowerPart == upperPart {
			return "", nil, fmt.Errorf("lower layer must differ from upper layer in specification: %q", spec)
		}
	}

	if colonIdx == -1 {
		return poolPart, []string{"*"}, nil
	}

	if typesPart == "" {
		return "", nil, fmt.Errorf("empty types list in specification: %q", spec)
	}

	if typesPart == "*" {
		return poolPart, []string{"*"}, nil
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

	return poolPart, types, nil
}

func validatePoolLayer(layer, spec string) error {
	poolName, namespace := splitPoolKey(layer)
	if poolName == "" {
		return fmt.Errorf("empty pool name in specification: %q", spec)
	}
	if strings.Contains(layer, "/") && namespace == "" {
		return fmt.Errorf("empty namespace in specification: %q", spec)
	}
	if strings.HasPrefix(namespace, "/") {
		return fmt.Errorf("namespace cannot start with '/' in specification: %q", spec)
	}
	if strings.Contains(namespace, "/") {
		return fmt.Errorf("namespace cannot contain '/' in specification: %q", spec)
	}
	return nil
}

func isValidBlobTypeForMapping(bt BlobType) bool {
	for _, valid := range AllBlobTypes {
		if bt == valid {
			return true
		}
	}
	return false
}
