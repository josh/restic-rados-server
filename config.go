package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	defaultBufferSize      = 16 * 1024 * 1024
	defaultShutdownTimeout = 60 * time.Second
	repoToken              = "{repo}"
	repoMatchToken         = "{repo_match}"
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
	BlobTypeConfig,
	BlobTypeKeys,
	BlobTypeLocks,
	BlobTypeSnapshots,
	BlobTypeData,
	BlobTypeIndex,
}

type configDuration time.Duration

func (d *configDuration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	*d = configDuration(parsed)
	return nil
}

type CephConfig struct {
	KeyringPath     string
	ClientID        string
	CephConf        string
	WriteBufferSize int64
}

type TailscaleConfig struct {
	Socket         string `json:"socket,omitempty"`
	UpstreamSocket string `json:"upstream_socket,omitempty"`
	HTTPS          *bool  `json:"https,omitempty"`
	Port           int    `json:"port,omitempty"`
}

type Config struct {
	Verbose         bool
	LogFile         string
	Listeners       listenerFlags
	Stdio           bool
	ShutdownTimeout configDuration
	MaxIdleTime     configDuration
	ReadBufferSize  int64
	WriteBufferSize int64
	Keyring         string
	ClientID        string
	CephConf        string
	Tailscale       *TailscaleConfig
	Repos           map[string]*RepoConfig
}

type BlobPool struct {
	Pool          string
	Namespace     string
	Prefix        string
	Striped       bool
	Alignment     uint64
	MaxObjectSize int64
	Lower         *BlobPool
}

func (p *BlobPool) forRepo(repo, match string) *BlobPool {
	if p == nil {
		return nil
	}
	resolved := *p
	resolved.Namespace = expandRepoTokens(resolved.Namespace, repo, match)
	resolved.Prefix = expandRepoTokens(resolved.Prefix, repo, match)
	resolved.Lower = p.Lower.forRepo(repo, match)
	return &resolved
}

type BlobPoolConfig struct {
	Pool          string
	Namespace     string
	Prefix        string
	Striped       *bool
	MaxObjectSize *int64
	Lower         *BlobPoolConfig

	poolSet          bool
	namespaceSet     bool
	prefixSet        bool
	stripedSet       bool
	maxObjectSizeSet bool
	upper            *BlobPoolConfig
	lower            *BlobPoolConfig
}

type blobPoolLayerJSON struct {
	Pool          *string `json:"pool"`
	Namespace     *string `json:"namespace"`
	Prefix        *string `json:"prefix"`
	Striped       *bool   `json:"striped"`
	MaxObjectSize *int64  `json:"max_object_size"`
}

func (l *blobPoolLayerJSON) UnmarshalJSON(data []byte) error {
	type layer blobPoolLayerJSON
	var decoded layer
	if err := decodeStrict(data, &decoded); err != nil {
		return err
	}
	*l = blobPoolLayerJSON(decoded)
	return nil
}

func layerConfig(layer *blobPoolLayerJSON) *BlobPoolConfig {
	if layer == nil {
		return nil
	}
	config := &BlobPoolConfig{}
	if layer.Pool != nil {
		config.Pool = *layer.Pool
		config.poolSet = true
	}
	if layer.Namespace != nil {
		config.Namespace = *layer.Namespace
		config.namespaceSet = true
	}
	if layer.Prefix != nil {
		config.Prefix = *layer.Prefix
		config.prefixSet = true
	}
	if layer.Striped != nil {
		value := *layer.Striped
		config.Striped = &value
		config.stripedSet = true
	}
	if layer.MaxObjectSize != nil {
		value := *layer.MaxObjectSize
		config.MaxObjectSize = &value
		config.maxObjectSizeSet = true
	}
	return config
}

func (p *BlobPoolConfig) UnmarshalJSON(data []byte) error {
	var decoded struct {
		Pool          *string            `json:"pool"`
		Namespace     *string            `json:"namespace"`
		Prefix        *string            `json:"prefix"`
		Striped       *bool              `json:"striped"`
		MaxObjectSize *int64             `json:"max_object_size"`
		Upper         *blobPoolLayerJSON `json:"upper"`
		Lower         *blobPoolLayerJSON `json:"lower"`
	}
	if err := decodeStrict(data, &decoded); err != nil {
		return err
	}
	*p = BlobPoolConfig{}
	if decoded.Pool != nil {
		p.Pool = *decoded.Pool
		p.poolSet = true
	}
	if decoded.Namespace != nil {
		p.Namespace = *decoded.Namespace
		p.namespaceSet = true
	}
	if decoded.Prefix != nil {
		p.Prefix = *decoded.Prefix
		p.prefixSet = true
	}
	if decoded.Striped != nil {
		value := *decoded.Striped
		p.Striped = &value
		p.stripedSet = true
	}
	if decoded.MaxObjectSize != nil {
		value := *decoded.MaxObjectSize
		p.MaxObjectSize = &value
		p.maxObjectSizeSet = true
	}
	p.upper = layerConfig(decoded.Upper)
	p.lower = layerConfig(decoded.Lower)
	return nil
}

type BlobPools struct {
	Config    BlobPoolConfig
	Keys      BlobPoolConfig
	Locks     BlobPoolConfig
	Snapshots BlobPoolConfig
	Data      BlobPoolConfig
	Index     BlobPoolConfig

	present map[BlobType]bool
}

func (p *BlobPools) UnmarshalJSON(data []byte) error {
	var decoded struct {
		Config    *BlobPoolConfig `json:"config"`
		Keys      *BlobPoolConfig `json:"keys"`
		Locks     *BlobPoolConfig `json:"locks"`
		Snapshots *BlobPoolConfig `json:"snapshots"`
		Data      *BlobPoolConfig `json:"data"`
		Index     *BlobPoolConfig `json:"index"`
	}
	if err := decodeStrict(data, &decoded); err != nil {
		return err
	}
	*p = BlobPools{present: make(map[BlobType]bool)}
	entries := []struct {
		blobType BlobType
		value    *BlobPoolConfig
	}{
		{BlobTypeConfig, decoded.Config},
		{BlobTypeKeys, decoded.Keys},
		{BlobTypeLocks, decoded.Locks},
		{BlobTypeSnapshots, decoded.Snapshots},
		{BlobTypeData, decoded.Data},
		{BlobTypeIndex, decoded.Index},
	}
	for _, entry := range entries {
		if entry.value == nil {
			continue
		}
		p.setPoolForType(entry.blobType, *entry.value)
		p.present[entry.blobType] = true
	}
	return nil
}

func (p *BlobPools) getPoolForType(blobType BlobType) BlobPoolConfig {
	if p == nil {
		return BlobPoolConfig{}
	}
	switch blobType {
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
		return BlobPoolConfig{}
	}
}

func (p *BlobPools) setPoolForType(blobType BlobType, config BlobPoolConfig) {
	switch blobType {
	case BlobTypeConfig:
		p.Config = config
	case BlobTypeKeys:
		p.Keys = config
	case BlobTypeLocks:
		p.Locks = config
	case BlobTypeSnapshots:
		p.Snapshots = config
	case BlobTypeData:
		p.Data = config
	case BlobTypeIndex:
		p.Index = config
	}
}

type RepoConfig struct {
	Pools         map[string][]string
	BlobPools     *BlobPools
	Access        string
	Striper       *bool
	MaxObjectSize int64

	poolsSet         bool
	blobPoolsSet     bool
	accessSet        bool
	striperSet       bool
	maxObjectSizeSet bool
}

func (r *RepoConfig) UnmarshalJSON(data []byte) error {
	var decoded struct {
		Pools         *map[string][]string `json:"pools"`
		BlobPools     *BlobPools           `json:"blob_pools"`
		Access        *string              `json:"access"`
		Striper       *bool                `json:"striper"`
		MaxObjectSize *int64               `json:"max_object_size"`
	}
	if err := decodeStrict(data, &decoded); err != nil {
		return err
	}
	*r = RepoConfig{}
	if decoded.Pools != nil {
		r.Pools = *decoded.Pools
		r.poolsSet = true
	}
	if decoded.BlobPools != nil {
		r.BlobPools = decoded.BlobPools
		r.blobPoolsSet = true
	}
	if decoded.Access != nil {
		r.Access = *decoded.Access
		r.accessSet = true
	}
	if decoded.Striper != nil {
		value := *decoded.Striper
		r.Striper = &value
		r.striperSet = true
	}
	if decoded.MaxObjectSize != nil {
		r.MaxObjectSize = *decoded.MaxObjectSize
		r.maxObjectSizeSet = true
	}
	return nil
}

type configFile struct {
	Verbose         *bool                   `json:"verbose"`
	LogFile         *string                 `json:"log_file"`
	Listen          *listenerFlags          `json:"listen"`
	Stdio           *bool                   `json:"stdio"`
	ShutdownTimeout *configDuration         `json:"shutdown_timeout"`
	MaxIdleTime     *configDuration         `json:"max_idle_time"`
	ReadBufferSize  *int64                  `json:"read_buffer_size"`
	WriteBufferSize *int64                  `json:"write_buffer_size"`
	Keyring         *string                 `json:"keyring"`
	ClientID        *string                 `json:"client_id"`
	CephConf        *string                 `json:"ceph_conf"`
	Tailscale       *TailscaleConfig        `json:"tailscale"`
	Repos           *map[string]*RepoConfig `json:"repos"`
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

type stringListFlag []string

func (values *stringListFlag) String() string {
	return strings.Join(*values, ";")
}

func (values *stringListFlag) Set(value string) error {
	*values = append(*values, strings.Split(value, ";")...)
	return nil
}

type commandLineConfig struct {
	configPath      string
	verbose         bool
	logFile         string
	listeners       listenerFlags
	stdio           bool
	shutdownTimeout time.Duration
	maxIdleTime     time.Duration
	readBufferSize  int64
	writeBufferSize int64
	keyring         string
	clientID        string
	cephConf        string
	pools           stringListFlag
	access          string
	striper         bool
	maxObjectSize   int64
	showVersion     bool
	set             map[string]bool
}

func parseCommandLine(args []string) (*commandLineConfig, error) {
	if len(args) >= 2 && args[0] == "serve" && args[1] == "restic" {
		args = args[2:]
	}
	for len(args) > 0 && args[len(args)-1] == "" {
		args = args[:len(args)-1]
	}
	parsed := &commandLineConfig{}
	flags := flag.NewFlagSet("restic-rados-server", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.Usage = func() {
		fmt.Fprint(flags.Output(), "Usage: restic-rados-server [options]\n\nOptions:\n")
		flags.PrintDefaults()
	}
	flags.StringVar(&parsed.configPath, "config", "", "path to JSON configuration file")
	flags.BoolVar(&parsed.verbose, "verbose", false, "enable verbose logging")
	flags.BoolVar(&parsed.verbose, "v", false, "enable verbose logging")
	flags.StringVar(&parsed.logFile, "log-file", "", "path to log file (default: stderr)")
	flags.Var(&parsed.listeners, "listen", "Address or Unix socket path to listen on, repeatable")
	flags.BoolVar(&parsed.stdio, "stdio", false, "use HTTP/2 over stdin/stdout (default when no listeners specified)")
	flags.DurationVar(&parsed.shutdownTimeout, "shutdown-timeout", defaultShutdownTimeout, "graceful shutdown timeout for listeners")
	flags.DurationVar(&parsed.maxIdleTime, "max-idle-time", 0, "exit after duration with no active connections (e.g., 30s, 5m; 0 = disabled)")
	flags.Int64Var(&parsed.readBufferSize, "read-buffer-size", defaultBufferSize, "buffer size for reading objects in bytes")
	flags.Int64Var(&parsed.writeBufferSize, "write-buffer-size", defaultBufferSize, "buffer size for writing objects in bytes")
	flags.StringVar(&parsed.keyring, "keyring", "", "path to Ceph keyring file")
	flags.StringVar(&parsed.clientID, "id", "", "Ceph client ID (e.g., 'restic' for client.restic)")
	flags.StringVar(&parsed.clientID, "client-id", "", "Ceph client ID (e.g., 'restic' for client.restic)")
	flags.StringVar(&parsed.cephConf, "ceph-conf", "", "path to ceph.conf file")
	flags.Var(&parsed.pools, "pool", "Pool specification: 'pool[/namespace][//lowerpool[/namespace]][:types]' where types is '*' or comma-separated list (repeatable, or semicolon-separated)")
	flags.StringVar(&parsed.access, "access", "", "access level: r/read-only, ra/read-append, rw/read-write")
	flags.BoolVar(&parsed.striper, "striper", true, "enable librados striper for large objects")
	flags.Int64Var(&parsed.maxObjectSize, "max-object-size", 0, "max object size override (0 = use cluster config or 128MB default)")
	flags.BoolVar(&parsed.showVersion, "version", false, "print version and exit")
	flags.Bool("b2-hard-delete", false, "accepted for Restic compatibility and ignored")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			flags.SetOutput(os.Stderr)
			flags.Usage()
		}
		return nil, err
	}
	if flags.NArg() != 0 {
		return nil, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	parsed.set = make(map[string]bool)
	flags.Visit(func(f *flag.Flag) {
		parsed.set[f.Name] = true
	})
	if parsed.set["client-id"] {
		parsed.set["id"] = true
	}
	if parsed.set["v"] {
		parsed.set["verbose"] = true
	}
	return parsed, nil
}

func defaultConfig() *Config {
	return &Config{
		ShutdownTimeout: configDuration(defaultShutdownTimeout),
		ReadBufferSize:  defaultBufferSize,
		WriteBufferSize: defaultBufferSize,
		Repos:           make(map[string]*RepoConfig),
	}
}

var environmentPrefixes = []string{
	"RESTIC_RADOS_SERVER_",
	"RESTIC_CEPH_SERVER_",
	"CEPH_RESTIC_SERVER_",
	"RADOS_RESTIC_SERVER_",
}

func environmentValue(suffix string) (string, bool) {
	for _, prefix := range environmentPrefixes {
		if value := os.Getenv(prefix + suffix); value != "" {
			return value, true
		}
	}
	return "", false
}

func parseEnvironmentBool(suffix string) (bool, bool, error) {
	value, ok := environmentValue(suffix)
	if !ok {
		return false, false, nil
	}
	switch strings.ToLower(value) {
	case "yes", "on":
		return true, true, nil
	case "no", "off":
		return false, true, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, true, fmt.Errorf("invalid %s value %q: %w", suffix, value, err)
	}
	return parsed, true, nil
}

func parseEnvironmentInt64(suffix string) (int64, bool, error) {
	value, ok := environmentValue(suffix)
	if !ok {
		return 0, false, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, true, fmt.Errorf("invalid %s value %q: %w", suffix, value, err)
	}
	return parsed, true, nil
}

func parseEnvironmentDuration(suffix string) (time.Duration, bool, error) {
	value, ok := environmentValue(suffix)
	if !ok {
		return 0, false, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, true, fmt.Errorf("invalid %s value %q: %w", suffix, value, err)
	}
	return parsed, true, nil
}

func defaultRepo(config *Config) *RepoConfig {
	repo := config.Repos["default"]
	if repo == nil {
		repo = &RepoConfig{Access: "rw"}
		config.Repos["default"] = repo
	}
	if repo.Access == "" && !repo.accessSet {
		repo.Access = "rw"
	}
	return repo
}

func applyEnvironment(config *Config) error {
	if value, ok, err := parseEnvironmentBool("VERBOSE"); err != nil {
		return err
	} else if ok {
		config.Verbose = value
	}
	if value, ok := environmentValue("LOG_FILE"); ok {
		config.LogFile = value
	}
	if value, ok, err := parseEnvironmentBool("STDIO"); err != nil {
		return err
	} else if ok {
		config.Stdio = value
	}
	if value, ok, err := parseEnvironmentDuration("SHUTDOWN_TIMEOUT"); err != nil {
		return err
	} else if ok {
		config.ShutdownTimeout = configDuration(value)
	}
	if value, ok, err := parseEnvironmentDuration("MAX_IDLE_TIME"); err != nil {
		return err
	} else if ok {
		config.MaxIdleTime = configDuration(value)
	}
	if value, ok, err := parseEnvironmentInt64("READ_BUFFER_SIZE"); err != nil {
		return err
	} else if ok {
		config.ReadBufferSize = value
	}
	if value, ok, err := parseEnvironmentInt64("WRITE_BUFFER_SIZE"); err != nil {
		return err
	} else if ok {
		config.WriteBufferSize = value
	}
	if value, ok := environmentValue("KEYRING"); ok {
		config.Keyring = value
	} else if value := os.Getenv("CEPH_KEYRING"); value != "" {
		config.Keyring = value
	}
	if value, ok := environmentValue("ID"); ok {
		config.ClientID = value
	} else if value := os.Getenv("CEPH_ID"); value != "" {
		config.ClientID = value
	}
	if value, ok := environmentValue("CEPH_CONF"); ok {
		config.CephConf = value
	} else if value := os.Getenv("CEPH_CONF"); value != "" {
		config.CephConf = value
	}
	if value, ok := environmentValue("LISTEN"); ok {
		var listeners listenerFlags
		for _, spec := range strings.Split(value, ";") {
			if err := listeners.Set(spec); err != nil {
				return err
			}
		}
		config.Listeners = listeners
	}
	if value, ok := environmentValue("POOL"); ok {
		pools, err := parsePoolSpecifications(strings.Split(value, ";"))
		if err != nil {
			return err
		}
		repo := defaultRepo(config)
		repo.Pools = nil
		repo.BlobPools = pools
		repo.poolsSet = false
		repo.blobPoolsSet = true
	}
	if value, ok := environmentValue("ACCESS"); ok {
		repo := defaultRepo(config)
		repo.Access = value
		repo.accessSet = true
	}
	if value, ok, err := parseEnvironmentBool("STRIPER"); err != nil {
		return err
	} else if ok {
		repo := defaultRepo(config)
		repo.Striper = &value
		repo.striperSet = true
	}
	if value, ok, err := parseEnvironmentInt64("MAX_OBJECT_SIZE"); err != nil {
		return err
	} else if ok {
		repo := defaultRepo(config)
		repo.MaxObjectSize = value
		repo.maxObjectSizeSet = true
	}
	return nil
}

func mergeRepo(base, overlay *RepoConfig) *RepoConfig {
	if base == nil {
		base = &RepoConfig{Access: "rw"}
	}
	merged := *base
	if overlay == nil {
		return &merged
	}
	if overlay.poolsSet {
		merged.Pools = overlay.Pools
		merged.BlobPools = nil
		merged.poolsSet = true
		merged.blobPoolsSet = false
	}
	if overlay.blobPoolsSet {
		merged.Pools = nil
		merged.BlobPools = overlay.BlobPools
		merged.poolsSet = false
		merged.blobPoolsSet = true
	}
	if overlay.accessSet {
		merged.Access = overlay.Access
		merged.accessSet = true
	}
	if overlay.striperSet {
		merged.Striper = overlay.Striper
		merged.striperSet = true
	}
	if overlay.maxObjectSizeSet {
		merged.MaxObjectSize = overlay.MaxObjectSize
		merged.maxObjectSizeSet = true
	}
	return &merged
}

func applyConfigFile(config *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to load config file %q: %w", path, err)
	}
	var file configFile
	if err := decodeStrict(data, &file); err != nil {
		return fmt.Errorf("failed to load config file %q: %w", path, err)
	}
	if file.Verbose != nil {
		config.Verbose = *file.Verbose
	}
	if file.LogFile != nil {
		config.LogFile = *file.LogFile
	}
	if file.Listen != nil {
		config.Listeners = *file.Listen
	}
	if file.Stdio != nil {
		config.Stdio = *file.Stdio
	}
	if file.ShutdownTimeout != nil {
		config.ShutdownTimeout = *file.ShutdownTimeout
	}
	if file.MaxIdleTime != nil {
		config.MaxIdleTime = *file.MaxIdleTime
	}
	if file.ReadBufferSize != nil {
		config.ReadBufferSize = *file.ReadBufferSize
	}
	if file.WriteBufferSize != nil {
		config.WriteBufferSize = *file.WriteBufferSize
	}
	if file.Keyring != nil {
		config.Keyring = *file.Keyring
	}
	if file.ClientID != nil {
		config.ClientID = *file.ClientID
	}
	if file.CephConf != nil {
		config.CephConf = *file.CephConf
	}
	if file.Tailscale != nil {
		config.Tailscale = file.Tailscale
	}
	if file.Repos != nil {
		repos := make(map[string]*RepoConfig, len(*file.Repos))
		for name, repo := range *file.Repos {
			if repo == nil {
				repos[name] = nil
				continue
			}
			if repo.poolsSet && repo.blobPoolsSet {
				return fmt.Errorf("repo %q: cannot combine pools with blob_pools", name)
			}
			var base *RepoConfig
			if name == "default" {
				base = config.Repos[name]
			}
			repos[name] = mergeRepo(base, repo)
		}
		config.Repos = repos
	}
	return nil
}

func applyCommandLine(config *Config, commandLine *commandLineConfig) error {
	set := commandLine.set
	if set["verbose"] {
		config.Verbose = commandLine.verbose
	}
	if set["log-file"] {
		config.LogFile = commandLine.logFile
	}
	if set["listen"] {
		config.Listeners = commandLine.listeners
	}
	if set["stdio"] {
		config.Stdio = commandLine.stdio
	}
	if set["shutdown-timeout"] {
		config.ShutdownTimeout = configDuration(commandLine.shutdownTimeout)
	}
	if set["max-idle-time"] {
		config.MaxIdleTime = configDuration(commandLine.maxIdleTime)
	}
	if set["read-buffer-size"] {
		config.ReadBufferSize = commandLine.readBufferSize
	}
	if set["write-buffer-size"] {
		config.WriteBufferSize = commandLine.writeBufferSize
	}
	if set["keyring"] {
		config.Keyring = commandLine.keyring
	}
	if set["id"] {
		config.ClientID = commandLine.clientID
	}
	if set["ceph-conf"] {
		config.CephConf = commandLine.cephConf
	}
	if set["pool"] {
		pools, err := parsePoolSpecifications(commandLine.pools)
		if err != nil {
			return err
		}
		repo := defaultRepo(config)
		repo.Pools = nil
		repo.BlobPools = pools
		repo.poolsSet = false
		repo.blobPoolsSet = true
	}
	if set["access"] {
		repo := defaultRepo(config)
		repo.Access = commandLine.access
		repo.accessSet = true
	}
	if set["striper"] {
		repo := defaultRepo(config)
		repo.Striper = &commandLine.striper
		repo.striperSet = true
	}
	if set["max-object-size"] {
		repo := defaultRepo(config)
		repo.MaxObjectSize = commandLine.maxObjectSize
		repo.maxObjectSizeSet = true
	}
	return nil
}

func loadConfig(args []string) (*Config, bool, error) {
	commandLine, err := parseCommandLine(args)
	if err != nil {
		return nil, false, err
	}
	if commandLine.showVersion {
		return nil, true, nil
	}
	config := defaultConfig()
	configPath := commandLine.configPath
	if !commandLine.set["config"] {
		if value, ok := environmentValue("CONFIG"); ok {
			configPath = value
		}
	}
	if configPath != "" {
		if err := applyConfigFile(config, configPath); err != nil {
			return nil, false, err
		}
	}
	if err := applyEnvironment(config); err != nil {
		return nil, false, err
	}
	if err := applyCommandLine(config, commandLine); err != nil {
		return nil, false, err
	}
	if err := normalizeAndValidateConfig(config); err != nil {
		return nil, false, err
	}
	return config, commandLine.showVersion, nil
}

type parsedPoolSpecification struct {
	raw      string
	config   BlobPoolConfig
	types    []BlobType
	catchAll bool
}

func parsePoolLayer(raw string, lower bool) (BlobPoolConfig, error) {
	pool, namespace, hasNamespace := strings.Cut(raw, "/")
	if pool == "" {
		if lower {
			return BlobPoolConfig{}, fmt.Errorf("empty lower pool name")
		}
		return BlobPoolConfig{}, fmt.Errorf("empty pool name")
	}
	if hasNamespace {
		if namespace == "" {
			return BlobPoolConfig{}, fmt.Errorf("empty namespace")
		}
		if strings.HasPrefix(namespace, "/") {
			return BlobPoolConfig{}, fmt.Errorf("namespace cannot start with \"/\"")
		}
		if strings.Contains(namespace, "/") {
			return BlobPoolConfig{}, fmt.Errorf("namespace cannot contain \"/\"")
		}
	}
	return BlobPoolConfig{Pool: pool, Namespace: namespace, poolSet: true, namespaceSet: hasNamespace}, nil
}

func parsePoolSpecification(raw string) (parsedPoolSpecification, error) {
	storage, typesValue, hasTypes := strings.Cut(raw, ":")
	storage = strings.TrimSpace(storage)
	if !hasTypes {
		typesValue = "*"
	}
	if hasTypes && typesValue == "" {
		return parsedPoolSpecification{raw: raw}, fmt.Errorf("empty types list")
	}
	return parsePoolSpecificationParts(raw, storage, strings.Split(typesValue, ","))
}

func parsePoolSpecificationParts(raw, storage string, rawTypes []string) (parsedPoolSpecification, error) {
	result := parsedPoolSpecification{raw: raw}
	if storage == "" {
		return result, fmt.Errorf("empty pool name")
	}
	upperRaw, lowerRaw, hasLower := strings.Cut(storage, "//")
	upper, err := parsePoolLayer(upperRaw, false)
	if err != nil {
		return result, err
	}
	if hasLower {
		lower, err := parsePoolLayer(lowerRaw, true)
		if err != nil {
			return result, err
		}
		if sameStorageConfig(upper, lower) {
			return result, fmt.Errorf("lower layer must differ from upper layer")
		}
		upper.Lower = &lower
	}
	seen := make(map[BlobType]bool)
	for _, rawType := range rawTypes {
		blobType := BlobType(strings.TrimSpace(rawType))
		if blobType == "" {
			continue
		}
		if blobType == "*" {
			result.catchAll = true
			continue
		}
		if !validBlobType(blobType) {
			return result, fmt.Errorf("unknown blob type %q", blobType)
		}
		if !seen[blobType] {
			seen[blobType] = true
			result.types = append(result.types, blobType)
		}
	}
	if result.catchAll && len(result.types) != 0 {
		return result, fmt.Errorf("catch-all type \"*\" cannot be mixed with explicit types")
	}
	result.config = upper
	return result, nil
}

func parsePoolSpecifications(rawSpecifications []string) (*BlobPools, error) {
	var parsed []parsedPoolSpecification
	for _, raw := range rawSpecifications {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		specification, err := parsePoolSpecification(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid pool configuration: %w in specification: %q", err, raw)
		}
		parsed = append(parsed, specification)
	}
	return combinePoolSpecifications(parsed)
}

func parseLegacyPoolSpecifications(rawPools map[string][]string) (*BlobPools, error) {
	keys := make([]string, 0, len(rawPools))
	for key := range rawPools {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parsed := make([]parsedPoolSpecification, 0, len(keys))
	for _, key := range keys {
		types := rawPools[key]
		raw := key + ":" + strings.Join(types, ",")
		if len(types) == 0 {
			return nil, fmt.Errorf("invalid pool configuration: empty types list in specification: %q", raw)
		}
		specification, err := parsePoolSpecificationParts(raw, key, types)
		if err != nil {
			return nil, fmt.Errorf("invalid pool configuration: %w in specification: %q", err, raw)
		}
		parsed = append(parsed, specification)
	}
	return combinePoolSpecifications(parsed)
}

func combinePoolSpecifications(parsed []parsedPoolSpecification) (*BlobPools, error) {
	var catchAll *parsedPoolSpecification
	assignments := make(map[BlobType]parsedPoolSpecification)
	for _, specification := range parsed {
		if specification.catchAll {
			if catchAll == nil {
				copy := specification
				catchAll = &copy
			} else if !samePoolConfig(catchAll.config, specification.config) {
				return nil, fmt.Errorf("multiple catch-all pools specified: %q and %q", catchAll.raw, specification.raw)
			}
			continue
		}
		for _, blobType := range specification.types {
			previous, exists := assignments[blobType]
			if exists && !samePoolConfig(previous.config, specification.config) {
				return nil, fmt.Errorf("blob type %q assigned to multiple pools: %q and %q", blobType, previous.raw, specification.raw)
			}
			if !exists {
				assignments[blobType] = specification
			}
		}
	}
	pools := &BlobPools{present: make(map[BlobType]bool)}
	for _, blobType := range AllBlobTypes {
		specification, ok := assignments[blobType]
		if !ok && catchAll != nil {
			specification = *catchAll
			ok = true
		}
		if ok {
			pools.setPoolForType(blobType, clonePoolConfig(specification.config))
			pools.present[blobType] = true
		}
	}
	return pools, nil
}

func clonePoolConfig(config BlobPoolConfig) BlobPoolConfig {
	cloned := config
	if config.Striped != nil {
		value := *config.Striped
		cloned.Striped = &value
	}
	if config.MaxObjectSize != nil {
		value := *config.MaxObjectSize
		cloned.MaxObjectSize = &value
	}
	if config.Lower != nil {
		lower := clonePoolConfig(*config.Lower)
		cloned.Lower = &lower
	}
	return cloned
}

func validBlobType(blobType BlobType) bool {
	for _, candidate := range AllBlobTypes {
		if blobType == candidate {
			return true
		}
	}
	return false
}

func sameOptionalBool(a, b *bool) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func sameOptionalInt64(a, b *int64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func sameStorageConfig(a, b BlobPoolConfig) bool {
	return a.Pool == b.Pool && a.Namespace == b.Namespace && a.Prefix == b.Prefix
}

func samePoolConfig(a, b BlobPoolConfig) bool {
	if !sameStorageConfig(a, b) || !sameOptionalBool(a.Striped, b.Striped) || !sameOptionalInt64(a.MaxObjectSize, b.MaxObjectSize) {
		return false
	}
	if a.Lower == nil || b.Lower == nil {
		return a.Lower == nil && b.Lower == nil
	}
	return samePoolConfig(*a.Lower, *b.Lower)
}

func normalizePoolEntry(blobType BlobType, entry BlobPoolConfig) (BlobPoolConfig, error) {
	hasLayers := entry.upper != nil || entry.lower != nil
	hasFlat := entry.poolSet || entry.namespaceSet || entry.prefixSet || entry.stripedSet || entry.maxObjectSizeSet
	if hasLayers && hasFlat {
		return BlobPoolConfig{}, fmt.Errorf("blob type %q: cannot combine pool with upper/lower layers", blobType)
	}
	if entry.lower != nil && entry.upper == nil {
		return BlobPoolConfig{}, fmt.Errorf("blob type %q: lower layer requires an explicit upper layer", blobType)
	}
	if entry.upper != nil && entry.lower == nil {
		return BlobPoolConfig{}, fmt.Errorf("blob type %q: upper layer requires a lower layer", blobType)
	}
	if !hasLayers {
		if err := validatePoolLayer(blobType, entry, "", "", false); err != nil {
			return BlobPoolConfig{}, err
		}
		entry.upper = nil
		entry.lower = nil
		return entry, nil
	}
	if err := validatePoolLayer(blobType, *entry.upper, "upper ", "upper layer ", true); err != nil {
		return BlobPoolConfig{}, err
	}
	if err := validatePoolLayer(blobType, *entry.lower, "lower ", "lower layer ", true); err != nil {
		return BlobPoolConfig{}, err
	}
	if sameStorageConfig(*entry.upper, *entry.lower) {
		return BlobPoolConfig{}, fmt.Errorf("blob type %q: lower layer must differ from upper layer", blobType)
	}
	upper := clonePoolConfig(*entry.upper)
	lower := clonePoolConfig(*entry.lower)
	upper.Lower = &lower
	return upper, nil
}

func validatePoolLayer(blobType BlobType, layer BlobPoolConfig, poolLabel, fieldLabel string, requirePool bool) error {
	if layer.Pool == "" && (requirePool || layer.poolSet) {
		return fmt.Errorf("blob type %q: %spool name cannot be empty", blobType, poolLabel)
	}
	if layer.MaxObjectSize != nil && *layer.MaxObjectSize <= 0 {
		return fmt.Errorf("blob type %q: %smax_object_size must be positive, got %d", blobType, fieldLabel, *layer.MaxObjectSize)
	}
	if containsControl(layer.Prefix) {
		return fmt.Errorf("blob type %q: %sprefix cannot contain control characters", blobType, fieldLabel)
	}
	return nil
}

func containsControl(value string) bool {
	return strings.ContainsFunc(value, unicode.IsControl)
}

func normalizeRepoPools(repo *RepoConfig) error {
	if repo.poolsSet && repo.blobPoolsSet {
		return fmt.Errorf("cannot combine pools with blob_pools")
	}
	if repo.poolsSet {
		pools, err := parseLegacyPoolSpecifications(repo.Pools)
		if err != nil {
			return err
		}
		repo.BlobPools = pools
	}
	if repo.BlobPools == nil {
		return nil
	}
	if repo.BlobPools.present == nil {
		repo.BlobPools.present = make(map[BlobType]bool)
		for _, blobType := range AllBlobTypes {
			if repo.BlobPools.getPoolForType(blobType).Pool != "" {
				repo.BlobPools.present[blobType] = true
			}
		}
	}
	for _, blobType := range AllBlobTypes {
		if !repo.BlobPools.present[blobType] {
			continue
		}
		normalized, err := normalizePoolEntry(blobType, repo.BlobPools.getPoolForType(blobType))
		if err != nil {
			return err
		}
		repo.BlobPools.setPoolForType(blobType, normalized)
	}
	return nil
}

func validRepoAccess(access string) bool {
	switch access {
	case "r", "read-only", "ra", "read-append", "rw", "read-write":
		return true
	default:
		return false
	}
}

func normalizeAndValidateConfig(config *Config) error {
	if config.ReadBufferSize <= 0 {
		return fmt.Errorf("read-buffer-size must be positive, got %d", config.ReadBufferSize)
	}
	if config.WriteBufferSize <= 0 {
		return fmt.Errorf("write-buffer-size must be positive, got %d", config.WriteBufferSize)
	}
	if time.Duration(config.ShutdownTimeout) < 0 {
		return fmt.Errorf("shutdown-timeout cannot be negative")
	}
	if time.Duration(config.MaxIdleTime) < 0 {
		return fmt.Errorf("max-idle-time cannot be negative")
	}
	if config.Tailscale != nil && (config.Tailscale.Port < 0 || config.Tailscale.Port > 65535) {
		return fmt.Errorf("tailscale port must be between 1 and 65535, got %d", config.Tailscale.Port)
	}
	names := make([]string, 0, len(config.Repos))
	for name := range config.Repos {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		repo := config.Repos[name]
		if repo == nil {
			return fmt.Errorf("repo %q: configuration cannot be null", name)
		}
		if err := validateRepoName(name); err != nil {
			return err
		}
		if repo.Access == "" && !repo.accessSet {
			repo.Access = "rw"
		}
		if !validRepoAccess(repo.Access) {
			return fmt.Errorf("repo %q: invalid access %q (must be r, ra, or rw)", name, repo.Access)
		}
		if repo.MaxObjectSize < 0 {
			return fmt.Errorf("repo %q: max-object-size cannot be negative, got %d", name, repo.MaxObjectSize)
		}
		if err := normalizeRepoPools(repo); err != nil {
			return fmt.Errorf("repo %q: %w", name, err)
		}
	}
	if err := validateRepoStorage(config.Repos); err != nil {
		return err
	}
	return nil
}

func isRepoNameCharacter(r rune) bool {
	if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
		return true
	}
	return strings.ContainsRune("-._~!$&'()+,;=:@", r)
}

func isValidRepoName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	for _, r := range name {
		if r > unicode.MaxASCII || !isRepoNameCharacter(r) {
			return false
		}
	}
	return true
}

func isReservedRepoName(name string) bool {
	switch name {
	case "config", "keys", "locks", "snapshots", "data", "index", "healthz", "readyz":
		return true
	default:
		return false
	}
}

func validateRepoName(name string) error {
	starCount := strings.Count(name, "*")
	if starCount > 1 {
		return fmt.Errorf("invalid repo pattern %q (may contain only one '*')", name)
	}
	plain := strings.ReplaceAll(name, "*", "a")
	if !isValidRepoName(plain) {
		if name == "." || name == ".." {
			return fmt.Errorf("invalid repo name %q (must not be \".\" or \"..\")", name)
		}
		return fmt.Errorf("invalid repo name %q", name)
	}
	if starCount == 0 && isReservedRepoName(name) {
		return fmt.Errorf("reserved repo name %q", name)
	}
	return nil
}

type repoPattern struct {
	key    string
	prefix string
	suffix string
}

func (p repoPattern) match(repo string) (string, bool) {
	if len(repo) <= len(p.prefix)+len(p.suffix) || !strings.HasPrefix(repo, p.prefix) || !strings.HasSuffix(repo, p.suffix) {
		return "", false
	}
	return repo[len(p.prefix) : len(repo)-len(p.suffix)], true
}

func compareRepoPatterns(a, b repoPattern) int {
	aLiteral := len(a.prefix) + len(a.suffix)
	bLiteral := len(b.prefix) + len(b.suffix)
	if aLiteral != bLiteral {
		return bLiteral - aLiteral
	}
	if len(a.prefix) != len(b.prefix) {
		return len(b.prefix) - len(a.prefix)
	}
	if len(a.suffix) != len(b.suffix) {
		return len(b.suffix) - len(a.suffix)
	}
	return strings.Compare(a.key, b.key)
}

func compileRepoPatterns(repos map[string]*RepoConfig) []repoPattern {
	patterns := make([]repoPattern, 0)
	for key := range repos {
		prefix, suffix, found := strings.Cut(key, "*")
		if !found {
			continue
		}
		patterns = append(patterns, repoPattern{key: key, prefix: prefix, suffix: suffix})
	}
	sort.Slice(patterns, func(i, j int) bool {
		return compareRepoPatterns(patterns[i], patterns[j]) < 0
	})
	return patterns
}

func expandRepoTokens(value, repo, match string) string {
	value = strings.ReplaceAll(value, repoToken, repo)
	return strings.ReplaceAll(value, repoMatchToken, match)
}

func configLayers(config BlobPoolConfig) []BlobPoolConfig {
	layers := []BlobPoolConfig{config}
	if config.Lower != nil {
		layers = append(layers, *config.Lower)
	}
	return layers
}

func layerToken(layer BlobPoolConfig) string {
	hasRepo := strings.Contains(layer.Namespace, repoToken) || strings.Contains(layer.Prefix, repoToken)
	hasMatch := strings.Contains(layer.Namespace, repoMatchToken) || strings.Contains(layer.Prefix, repoMatchToken)
	switch {
	case hasRepo && hasMatch:
		return "both"
	case hasRepo:
		return repoToken
	case hasMatch:
		return repoMatchToken
	default:
		return ""
	}
}

func validatePatternLayer(repoName string, blobType BlobType, layer BlobPoolConfig, label string) error {
	if strings.Contains(layer.Pool, repoToken) || strings.Contains(layer.Pool, repoMatchToken) {
		return fmt.Errorf("repo %q: blob type %q: %spool name cannot contain %q or %q (dynamic pool names are not supported)", repoName, blobType, label, repoToken, repoMatchToken)
	}
	if layerToken(layer) == "" {
		return fmt.Errorf("repo %q: blob type %q: %snamespace or prefix must contain %q or %q so dynamic repos do not share storage", repoName, blobType, label, repoToken, repoMatchToken)
	}
	if layerToken(layer) == "both" {
		return fmt.Errorf("repo %q: blob type %q: %smust use only one of %q or %q", repoName, blobType, label, repoToken, repoMatchToken)
	}
	return nil
}

func validateStaticLayer(repoName string, blobType BlobType, layer BlobPoolConfig, label string) error {
	if strings.Contains(layer.Pool, repoToken) || strings.Contains(layer.Pool, repoMatchToken) ||
		strings.Contains(layer.Namespace, repoToken) || strings.Contains(layer.Namespace, repoMatchToken) ||
		strings.Contains(layer.Prefix, repoToken) || strings.Contains(layer.Prefix, repoMatchToken) {
		return fmt.Errorf("repo %q: blob type %q: %s%q and %q are only allowed in repo patterns", repoName, blobType, label, repoToken, repoMatchToken)
	}
	return nil
}

func validateSharedPoolTokenShape(blobType BlobType, layer BlobPoolConfig) error {
	token := layerToken(layer)
	if token == "both" || token == "" {
		return nil
	}
	if strings.Contains(layer.Namespace, token) && layer.Namespace != token {
		return fmt.Errorf("blob type %q: namespace %q must be exactly %q or %q when both layers share a pool", blobType, layer.Namespace, repoToken, repoMatchToken)
	}
	if strings.Contains(layer.Prefix, token) && !strings.HasPrefix(layer.Prefix, token+"/") {
		return fmt.Errorf("blob type %q: prefix %q must start with %q or %q followed by \"/\" when both layers share a pool", blobType, layer.Prefix, repoToken, repoMatchToken)
	}
	return nil
}

func templateLiterals(value string) []string {
	value = strings.ReplaceAll(value, repoToken, "|")
	value = strings.ReplaceAll(value, repoMatchToken, "|")
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == '|' || r == '/'
	})
	var literals []string
	for _, part := range parts {
		part = strings.Trim(part, "-._~!$&'()+,;=:@")
		if part != "" {
			literals = append(literals, part)
		}
	}
	trimmed := strings.TrimSuffix(value, "/")
	if !strings.Contains(trimmed, "|") && isValidRepoName(trimmed) {
		literals = append(literals, trimmed)
	}
	return literals
}

func patternCandidateRepos(pattern repoPattern, layers ...BlobPoolConfig) []string {
	values := []string{"", "a", "b", "x", "z", "ax", "aa", "ab", "abc", "foo", "bar", "repo", "other", "backup", "laptop", "desktop", "0"}
	for _, layer := range layers {
		values = append(values, templateLiterals(layer.Namespace)...)
		values = append(values, templateLiterals(layer.Prefix)...)
	}
	seen := make(map[string]bool)
	var repos []string
	add := func(repo string) {
		if seen[repo] || !isValidRepoName(repo) || isReservedRepoName(repo) || repo == "default" {
			return
		}
		if _, ok := pattern.match(repo); !ok {
			return
		}
		seen[repo] = true
		repos = append(repos, repo)
	}
	for _, value := range values {
		add(value)
		add(pattern.prefix + value + pattern.suffix)
	}
	return repos
}

func owningPattern(repo string, repos map[string]*RepoConfig, patterns []repoPattern) string {
	if _, ok := repos[repo]; ok {
		return ""
	}
	for _, pattern := range patterns {
		if _, ok := pattern.match(repo); ok {
			return pattern.key
		}
	}
	return ""
}

type symbolicTemplate struct {
	literal  string
	suffix   string
	variable bool
	exact    bool
}

func parseSymbolicTemplate(value, token string) symbolicTemplate {
	if !strings.Contains(value, token) {
		return symbolicTemplate{literal: value, exact: true}
	}
	if strings.Count(value, token) != 1 || !strings.HasPrefix(value, token) {
		return symbolicTemplate{}
	}
	return symbolicTemplate{suffix: strings.TrimPrefix(value, token), variable: true, exact: true}
}

type symbolicConstraints struct {
	upper      string
	lower      string
	upperSet   bool
	lowerSet   bool
	equal      bool
	impossible bool
	exact      bool
}

func (c *symbolicConstraints) setUpper(value string) {
	if c.upperSet && c.upper != value {
		c.impossible = true
		return
	}
	c.upper = value
	c.upperSet = true
}

func (c *symbolicConstraints) setLower(value string) {
	if c.lowerSet && c.lower != value {
		c.impossible = true
		return
	}
	c.lower = value
	c.lowerSet = true
}

func solveSymbolicValue(literal, suffix string) (string, bool) {
	if !strings.HasSuffix(literal, suffix) {
		return "", false
	}
	value := strings.TrimSuffix(literal, suffix)
	return value, value != "" && !strings.Contains(value, "/")
}

func (c *symbolicConstraints) add(upper, lower symbolicTemplate) {
	if c.impossible {
		return
	}
	if !upper.exact || !lower.exact {
		c.exact = false
		return
	}
	switch {
	case !upper.variable && !lower.variable:
		c.impossible = upper.literal != lower.literal
	case upper.variable && lower.variable:
		if upper.suffix != lower.suffix {
			c.impossible = true
			return
		}
		c.equal = true
	case upper.variable:
		value, ok := solveSymbolicValue(lower.literal, upper.suffix)
		if !ok {
			c.impossible = true
			return
		}
		c.setUpper(value)
	case lower.variable:
		value, ok := solveSymbolicValue(upper.literal, lower.suffix)
		if !ok {
			c.impossible = true
			return
		}
		c.setLower(value)
	}
}

func tokenValueRepo(pattern repoPattern, token, value string) (string, bool) {
	var repo string
	if token == repoToken {
		repo = value
	} else {
		repo = pattern.prefix + value + pattern.suffix
	}
	match, ok := pattern.match(repo)
	if !ok || token == repoMatchToken && match != value {
		return "", false
	}
	if !isValidRepoName(repo) || isReservedRepoName(repo) || repo == "default" {
		return "", false
	}
	return repo, true
}

func layersResolveTogether(pattern repoPattern, upper, lower BlobPoolConfig, repos map[string]*RepoConfig, patterns []repoPattern) bool {
	token := layerToken(upper)
	constraints := symbolicConstraints{exact: true}
	constraints.add(parseSymbolicTemplate(upper.Namespace, token), parseSymbolicTemplate(lower.Namespace, token))
	constraints.add(parseSymbolicTemplate(upper.Prefix, token), parseSymbolicTemplate(lower.Prefix, token))
	if constraints.impossible {
		return false
	}
	if !constraints.exact {
		return true
	}
	if constraints.equal {
		switch {
		case constraints.upperSet && constraints.lowerSet && constraints.upper != constraints.lower:
			return false
		case constraints.upperSet:
			constraints.setLower(constraints.upper)
		case constraints.lowerSet:
			constraints.setUpper(constraints.lower)
		}
	}
	if constraints.upperSet {
		repo, ok := tokenValueRepo(pattern, token, constraints.upper)
		if !ok || owningPattern(repo, repos, patterns) != pattern.key {
			return false
		}
	}
	if constraints.lowerSet {
		repo, ok := tokenValueRepo(pattern, token, constraints.lower)
		if !ok || owningPattern(repo, repos, patterns) != pattern.key {
			return false
		}
	}
	return true
}

func validateRepoStorage(repos map[string]*RepoConfig) error {
	patterns := compileRepoPatterns(repos)
	names := make([]string, 0, len(repos))
	for name := range repos {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, repoName := range names {
		repo := repos[repoName]
		if repo.BlobPools == nil {
			continue
		}
		dynamic := strings.Contains(repoName, "*")
		prefix, suffix, _ := strings.Cut(repoName, "*")
		pattern := repoPattern{key: repoName, prefix: prefix, suffix: suffix}
		for _, blobType := range AllBlobTypes {
			config := repo.BlobPools.getPoolForType(blobType)
			if config.Pool == "" {
				continue
			}
			layers := configLayers(config)
			for i, layer := range layers {
				label := ""
				if i == 1 {
					label = "lower layer "
				}
				var err error
				if dynamic {
					err = validatePatternLayer(repoName, blobType, layer, label)
				} else {
					err = validateStaticLayer(repoName, blobType, layer, label)
				}
				if err != nil {
					return err
				}
			}
			if !dynamic || config.Lower == nil || config.Pool != config.Lower.Pool {
				continue
			}
			if err := validateSharedPoolTokenShape(blobType, config); err != nil {
				return fmt.Errorf("repo %q: %w", repoName, err)
			}
			if err := validateSharedPoolTokenShape(blobType, *config.Lower); err != nil {
				return fmt.Errorf("repo %q: %w", repoName, err)
			}
			upperToken := layerToken(config)
			lowerToken := layerToken(*config.Lower)
			if upperToken != lowerToken {
				return fmt.Errorf("repo %q: blob type %q: upper and lower layers must both use %q or both use %q", repoName, blobType, repoToken, repoMatchToken)
			}
			if layersResolveTogether(pattern, config, *config.Lower, repos, patterns) {
				return fmt.Errorf("repo %q: blob type %q: lower layer may overlap upper layer across dynamic repos", repoName, blobType)
			}
		}
	}
	return nil
}

type storageCollision struct {
	repo      string
	otherRepo string
	blobTypes []string
}

type resolvedStorage struct {
	pool      string
	namespace string
	prefix    string
}

func repoCandidates(name string, repo *RepoConfig) []string {
	if !strings.Contains(name, "*") {
		return []string{name}
	}
	prefix, suffix, _ := strings.Cut(name, "*")
	pattern := repoPattern{key: name, prefix: prefix, suffix: suffix}
	var layers []BlobPoolConfig
	if repo != nil && repo.BlobPools != nil {
		for _, blobType := range AllBlobTypes {
			layers = append(layers, configLayers(repo.BlobPools.getPoolForType(blobType))...)
		}
	}
	return patternCandidateRepos(pattern, layers...)
}

func resolvedStorages(repoName, actualRepo string, config BlobPoolConfig) []resolvedStorage {
	match := ""
	if prefix, suffix, found := strings.Cut(repoName, "*"); found {
		match, _ = (repoPattern{key: repoName, prefix: prefix, suffix: suffix}).match(actualRepo)
	}
	var resolved []resolvedStorage
	for _, layer := range configLayers(config) {
		if layer.Pool == "" {
			continue
		}
		resolved = append(resolved, resolvedStorage{
			pool:      layer.Pool,
			namespace: expandRepoTokens(layer.Namespace, actualRepo, match),
			prefix:    expandRepoTokens(layer.Prefix, actualRepo, match),
		})
	}
	return resolved
}

func resolvedStoragesOverlap(a, b resolvedStorage, blobType BlobType) bool {
	if a.pool != b.pool || a.namespace != b.namespace {
		return false
	}
	if blobType == BlobTypeConfig {
		return a.prefix == b.prefix
	}
	upper := a.prefix + string(blobType) + "/"
	lower := b.prefix + string(blobType) + "/"
	return strings.HasPrefix(upper, lower) || strings.HasPrefix(lower, upper)
}

type collisionTemplate struct {
	literal  string
	prefix   string
	suffix   string
	variable bool
	exact    bool
}

func repoCollisionTemplate(repoName string) collisionTemplate {
	prefix, suffix, dynamic := strings.Cut(repoName, "*")
	if !dynamic {
		return collisionTemplate{literal: repoName, exact: true}
	}
	return collisionTemplate{prefix: prefix, suffix: suffix, variable: true, exact: true}
}

func storageCollisionTemplate(repoName string, layer BlobPoolConfig, value string) collisionTemplate {
	repoPrefix, repoSuffix, dynamic := strings.Cut(repoName, "*")
	if !dynamic {
		return collisionTemplate{literal: value, exact: true}
	}
	token := layerToken(layer)
	if token == "" || !strings.Contains(value, token) {
		return collisionTemplate{literal: value, exact: true}
	}
	if token == "both" || strings.Count(value, token) != 1 {
		return collisionTemplate{variable: true}
	}
	prefix, suffix, _ := strings.Cut(value, token)
	if token == repoToken {
		prefix += repoPrefix
		suffix = repoSuffix + suffix
	}
	return collisionTemplate{prefix: prefix, suffix: suffix, variable: true, exact: true}
}

func appendCollisionTemplate(template collisionTemplate, value string) collisionTemplate {
	if template.variable {
		template.suffix += value
	} else {
		template.literal += value
	}
	return template
}

func validCollisionVariable(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if !isRepoNameCharacter(r) {
			return false
		}
	}
	return true
}

func collisionTemplateMatchesLiteral(template collisionTemplate, literal string) bool {
	if !template.exact {
		return true
	}
	if !template.variable {
		return template.literal == literal
	}
	if len(literal) <= len(template.prefix)+len(template.suffix) ||
		!strings.HasPrefix(literal, template.prefix) ||
		!strings.HasSuffix(literal, template.suffix) {
		return false
	}
	value := literal[len(template.prefix) : len(literal)-len(template.suffix)]
	return validCollisionVariable(value)
}

func collisionFixedPrefixesCompatible(a, b string) bool {
	length := min(len(a), len(b))
	return a[:length] == b[:length]
}

func collisionFixedSuffixesCompatible(a, b string) bool {
	length := min(len(a), len(b))
	return a[len(a)-length:] == b[len(b)-length:]
}

func collisionTemplatesMayEqual(a, b collisionTemplate) bool {
	if !a.exact || !b.exact {
		return true
	}
	switch {
	case !a.variable && !b.variable:
		return a.literal == b.literal
	case !a.variable:
		return collisionTemplateMatchesLiteral(b, a.literal)
	case !b.variable:
		return collisionTemplateMatchesLiteral(a, b.literal)
	default:
		return collisionFixedPrefixesCompatible(a.prefix, b.prefix) &&
			collisionFixedSuffixesCompatible(a.suffix, b.suffix)
	}
}

func collisionTemplateMayPrefix(a, b collisionTemplate) bool {
	if !a.exact || !b.exact {
		return true
	}
	switch {
	case !a.variable && !b.variable:
		return strings.HasPrefix(b.literal, a.literal)
	case a.variable && !b.variable:
		for length := len(a.prefix) + len(a.suffix) + 1; length <= len(b.literal); length++ {
			if collisionTemplateMatchesLiteral(a, b.literal[:length]) {
				return true
			}
		}
		return false
	case !a.variable && b.variable:
		return collisionFixedPrefixesCompatible(a.literal, b.prefix)
	default:
		return collisionFixedPrefixesCompatible(a.prefix, b.prefix)
	}
}

func collisionTemplatesMayOverlap(a, b collisionTemplate) bool {
	return collisionTemplateMayPrefix(a, b) || collisionTemplateMayPrefix(b, a)
}

func collisionTemplatesEquivalent(a, b collisionTemplate) bool {
	if !a.exact || !b.exact || a.variable != b.variable {
		return false
	}
	if a.variable {
		return a.prefix == b.prefix && a.suffix == b.suffix
	}
	return a.literal == b.literal
}

func collisionForcesSameRepo(repoA, repoB, namespaceA, namespaceB, rootA, rootB collisionTemplate, blobType BlobType) bool {
	if collisionTemplatesEquivalent(namespaceA, repoA) && collisionTemplatesEquivalent(namespaceB, repoB) {
		return true
	}
	if blobType == BlobTypeConfig {
		return collisionTemplatesEquivalent(rootA, repoA) && collisionTemplatesEquivalent(rootB, repoB)
	}
	tail := "/" + string(blobType) + "/"
	return collisionTemplatesEquivalent(rootA, appendCollisionTemplate(repoA, tail)) &&
		collisionTemplatesEquivalent(rootB, appendCollisionTemplate(repoB, tail))
}

func repoBlobTypesMayCollideSymbolically(nameA string, configA BlobPoolConfig, nameB string, configB BlobPoolConfig, blobType BlobType) bool {
	repoA := repoCollisionTemplate(nameA)
	repoB := repoCollisionTemplate(nameB)
	for _, layerA := range configLayers(configA) {
		for _, layerB := range configLayers(configB) {
			if layerA.Pool == "" || layerA.Pool != layerB.Pool {
				continue
			}
			namespaceA := storageCollisionTemplate(nameA, layerA, layerA.Namespace)
			namespaceB := storageCollisionTemplate(nameB, layerB, layerB.Namespace)
			if !collisionTemplatesMayEqual(namespaceA, namespaceB) {
				continue
			}
			rootA := storageCollisionTemplate(nameA, layerA, layerA.Prefix)
			rootB := storageCollisionTemplate(nameB, layerB, layerB.Prefix)
			if blobType != BlobTypeConfig {
				rootA = appendCollisionTemplate(rootA, string(blobType)+"/")
				rootB = appendCollisionTemplate(rootB, string(blobType)+"/")
			}
			rootsOverlap := collisionTemplatesMayEqual(rootA, rootB)
			if blobType != BlobTypeConfig {
				rootsOverlap = collisionTemplatesMayOverlap(rootA, rootB)
			}
			if !rootsOverlap || collisionForcesSameRepo(repoA, repoB, namespaceA, namespaceB, rootA, rootB, blobType) {
				continue
			}
			return true
		}
	}
	return false
}

func repoBlobTypesCollide(nameA string, repoA *RepoConfig, nameB string, repoB *RepoConfig, blobType BlobType) bool {
	if repoA == nil || repoB == nil || repoA.BlobPools == nil || repoB.BlobPools == nil {
		return false
	}
	configA := repoA.BlobPools.getPoolForType(blobType)
	configB := repoB.BlobPools.getPoolForType(blobType)
	if configA.Pool == "" || configB.Pool == "" {
		return false
	}
	for _, actualA := range repoCandidates(nameA, repoA) {
		for _, actualB := range repoCandidates(nameB, repoB) {
			if actualA == actualB {
				continue
			}
			for _, storageA := range resolvedStorages(nameA, actualA, configA) {
				for _, storageB := range resolvedStorages(nameB, actualB, configB) {
					if resolvedStoragesOverlap(storageA, storageB, blobType) {
						return true
					}
				}
			}
		}
	}
	return repoBlobTypesMayCollideSymbolically(nameA, configA, nameB, configB, blobType)
}

func storageCollisions(repos map[string]*RepoConfig) []storageCollision {
	names := make([]string, 0, len(repos))
	for name := range repos {
		names = append(names, name)
	}
	sort.Strings(names)
	var collisions []storageCollision
	for i, name := range names {
		for _, otherName := range names[i+1:] {
			collision := storageCollision{repo: name, otherRepo: otherName}
			for _, blobType := range AllBlobTypes {
				if repoBlobTypesCollide(name, repos[name], otherName, repos[otherName], blobType) {
					collision.blobTypes = append(collision.blobTypes, string(blobType))
				}
			}
			if len(collision.blobTypes) != 0 {
				collisions = append(collisions, collision)
			}
		}
	}
	return collisions
}
