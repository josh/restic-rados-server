package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
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
	Listeners       ListenerConfigs        `json:"listen,omitempty"`
	Stdio           bool                   `json:"-"`
	Metrics         bool                   `json:"-"`
	ShutdownTimeout Duration               `json:"shutdown_timeout,omitempty"`
	Access          string                 `json:"access,omitempty"`
	MaxIdleTime     Duration               `json:"max_idle_time,omitempty"`
	IOTimeout       Duration               `json:"io_timeout,omitempty"`
	MaxUploads      int                    `json:"max_uploads,omitempty"`
	LogFile         string                 `json:"log_file,omitempty"`
	Keyring         string                 `json:"keyring,omitempty"`
	ClientID        string                 `json:"client_id,omitempty"`
	CephConf        string                 `json:"ceph_conf,omitempty"`
	ReadBufferSize  int64                  `json:"read_buffer_size,omitempty"`
	WriteBufferSize int64                  `json:"write_buffer_size,omitempty"`
	Repos           map[string]*RepoConfig `json:"repos,omitempty"`
}

func normalizeAccess(value string) (string, error) {
	switch value {
	case "r", "read-only":
		return "r", nil
	case "ra", "read-append":
		return "ra", nil
	case "rw", "read-write":
		return "rw", nil
	default:
		return "", fmt.Errorf("invalid access %q (must be r, ra, or rw)", value)
	}
}

func jsonObjectFields(data []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, errors.New("configuration cannot be null")
	}
	return fields, nil
}

func jsonObjectField(fields map[string]json.RawMessage, name string) (json.RawMessage, bool, error) {
	var raw json.RawMessage
	found := false
	for key, value := range fields {
		if !strings.EqualFold(key, name) {
			continue
		}
		if found {
			return nil, false, fmt.Errorf("configuration contains multiple %q fields", name)
		}
		raw = value
		found = true
	}
	return raw, found, nil
}

func parseJSONAccess(raw json.RawMessage) (string, error) {
	var value *string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	if value == nil {
		return "", errors.New("access cannot be null")
	}
	return normalizeAccess(*value)
}

func decodeStrictJSON(data []byte, value any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected data after JSON object")
		}
		return err
	}
	return nil
}

func (r *RepoConfig) UnmarshalJSON(data []byte) error {
	fields, err := jsonObjectFields(data)
	if err != nil {
		return err
	}
	accessRaw, hasAccess, err := jsonObjectField(fields, "access")
	if err != nil {
		return err
	}
	type plainRepoConfig RepoConfig
	if err := decodeStrictJSON(data, (*plainRepoConfig)(r)); err != nil {
		return err
	}

	if hasAccess {
		access, err := parseJSONAccess(accessRaw)
		if err != nil {
			return err
		}
		r.Access = access
	}
	return nil
}

func (c *Config) UnmarshalJSON(data []byte) error {
	fields, err := jsonObjectFields(data)
	if err != nil {
		return err
	}
	accessRaw, hasAccess, err := jsonObjectField(fields, "access")
	if err != nil {
		return err
	}
	type plainConfig Config
	if err := decodeStrictJSON(data, (*plainConfig)(c)); err != nil {
		return err
	}

	if hasAccess {
		access, err := parseJSONAccess(accessRaw)
		if err != nil {
			return err
		}
		c.Access = access
	}
	return nil
}

func (c *Config) loadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return decodeStrictJSON(data, c)
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

type repoFlags struct {
	listeners     ListenerConfigs
	poolSpecs     poolFlags
	striper       bool
	maxObjectSize int64
}

func bindFlags(c *Config, configFile *string, showVersion *bool, repo *repoFlags) *flag.FlagSet {
	fs := flag.NewFlagSet("restic-rados-server", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "Usage: restic-rados-server [options]\n\nOptions:\n")
		fs.PrintDefaults()
	}

	fs.BoolVar(showVersion, "version", false, "print version and exit")
	fs.StringVar(configFile, "config", "", "path to JSON configuration file")
	fs.BoolVar(&c.Verbose, "v", c.Verbose, "enable verbose logging")
	fs.BoolVar(&c.Verbose, "verbose", c.Verbose, "enable verbose logging")
	fs.Var(&repo.listeners, "listen", "Listener endpoint, repeatable")
	fs.BoolVar(&c.Stdio, "stdio", c.Stdio, "use HTTP/2 over stdin/stdout (default when no listeners specified)")
	fs.BoolVar(&c.Metrics, "metrics", c.Metrics, "serve Prometheus metrics at /metrics on every listener")
	fs.Var(&c.ShutdownTimeout, "shutdown-timeout", "graceful shutdown timeout for listeners")
	fs.StringVar(&c.Access, "access", c.Access, "maximum access level for the server: r/read-only, ra/read-append, rw/read-write")
	fs.Var(&c.MaxIdleTime, "max-idle-time", "exit after duration with no active connections (e.g., 30s, 5m; 0 = disabled)")
	fs.Var(&c.IOTimeout, "io-timeout", "longest pause allowed while reading a request body or writing a response (0 = disabled)")
	fs.IntVar(&c.MaxUploads, "max-uploads", c.MaxUploads, "uploads handled at once; the rest wait")
	fs.StringVar(&c.LogFile, "log-file", c.LogFile, "path to log file (default: stderr)")
	fs.StringVar(&c.Keyring, "keyring", c.Keyring, "path to Ceph keyring file")
	fs.StringVar(&c.ClientID, "id", c.ClientID, "Ceph client ID (e.g., 'restic' for client.restic)")
	fs.Var(&repo.poolSpecs, "pool", "Pool specification: 'pool[/namespace][:types]' where types is '*' or comma-separated list (repeatable, or semicolon-separated)")
	fs.StringVar(&c.CephConf, "ceph-conf", c.CephConf, "path to ceph.conf file")
	fs.BoolVar(&repo.striper, "striper", true, "enable librados striper for large objects")
	fs.Int64Var(&c.ReadBufferSize, "read-buffer-size", c.ReadBufferSize, "buffer size for reading objects in bytes")
	fs.Int64Var(&c.WriteBufferSize, "write-buffer-size", c.WriteBufferSize, "buffer size for writing objects in bytes")
	fs.Int64Var(&repo.maxObjectSize, "max-object-size", 0, "max object size override (0 = use cluster config or 128MB default)")
	return fs
}

func (c *Config) applyRepoFlags(fs *flag.FlagSet, repo *repoFlags) {
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "listen":
			c.Listeners = repo.listeners
		case "pool":
			def := c.defaultRepo()
			def.poolSpecs = repo.poolSpecs
			def.Pools = nil
			def.BlobPools = nil
		case "striper":
			c.defaultRepo().Striper = &repo.striper
		case "max-object-size":
			c.defaultRepo().MaxObjectSize = repo.maxObjectSize
		}
	})
}

func defaultConfig() Config {
	return Config{
		ShutdownTimeout: Duration(60 * time.Second),
		IOTimeout:       Duration(30 * time.Second),
		MaxUploads:      8,
		Access:          "rw",
		ReadBufferSize:  defaultReadBufferSize,
		WriteBufferSize: defaultWriteBufferSize,
	}
}

var envPrefixes = []string{"RESTIC_RADOS_SERVER_", "CEPH_RESTIC_SERVER_"}

func getPrefixedEnv(suffix string) string {
	for _, prefix := range envPrefixes {
		if v := os.Getenv(prefix + suffix); v != "" {
			return v
		}
	}
	return ""
}

func parseBoolEnv(suffix string) (bool, bool, error) {
	val := getPrefixedEnv(suffix)
	if val == "" {
		return false, false, nil
	}
	switch strings.ToLower(val) {
	case "true", "1", "yes", "on":
		return true, true, nil
	case "false", "0", "no", "off":
		return false, true, nil
	}
	return false, false, fmt.Errorf("invalid %s value %q", suffix, val)
}

func parseInt64Env(suffix string) (int64, bool) {
	val := getPrefixedEnv(suffix)
	if val == "" {
		return 0, false
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func (c *Config) loadFromEnv() error {
	verbose, hasVerbose, err := parseBoolEnv("VERBOSE")
	if err != nil {
		return err
	}
	if hasVerbose {
		c.Verbose = verbose
	}
	if v := getPrefixedEnv("LOG_FILE"); v != "" {
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

	if envAccess := getPrefixedEnv("ACCESS"); envAccess != "" {
		access, err := normalizeAccess(envAccess)
		if err != nil {
			return err
		}
		c.Access = access
	}
	striper, hasStriper, err := parseBoolEnv("STRIPER")
	if err != nil {
		return err
	}
	maxObjectSize, hasMaxObjectSize := parseInt64Env("MAX_OBJECT_SIZE")
	envPool := getPrefixedEnv("POOL")

	if hasStriper || hasMaxObjectSize || envPool != "" {
		def := c.defaultRepo()
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

	return nil
}

func loadConfig(args []string) (Config, bool, error) {
	var configFile string
	var showVersion bool
	scratch := defaultConfig()
	fs := bindFlags(&scratch, &configFile, &showVersion, &repoFlags{})
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(os.Stderr)
			fs.Usage()
		}
		return Config{}, false, err
	}

	if showVersion {
		return Config{}, true, nil
	}

	if configFile == "" {
		configFile = getPrefixedEnv("CONFIG")
	}

	config := defaultConfig()
	if configFile != "" {
		if err := config.loadFromFile(configFile); err != nil {
			return Config{}, false, fmt.Errorf("failed to load config file %s: %w", configFile, err)
		}
	}

	if err := config.loadFromEnv(); err != nil {
		return Config{}, false, err
	}

	var repo repoFlags
	fs = bindFlags(&config, &configFile, &showVersion, &repo)
	if err := fs.Parse(args); err != nil {
		return Config{}, false, err
	}
	config.applyRepoFlags(fs, &repo)

	if config.Metrics && config.Stdio {
		return Config{}, false, errors.New("--metrics is not supported in stdio mode")
	}

	if config.ReadBufferSize <= 0 {
		return Config{}, false, fmt.Errorf("read-buffer-size must be positive, got %d", config.ReadBufferSize)
	}

	if config.WriteBufferSize <= 0 {
		return Config{}, false, fmt.Errorf("write-buffer-size must be positive, got %d", config.WriteBufferSize)
	}

	if config.IOTimeout < 0 {
		return Config{}, false, errors.New("io-timeout cannot be negative")
	}

	if config.MaxUploads <= 0 {
		return Config{}, false, fmt.Errorf("max-uploads must be positive, got %d", config.MaxUploads)
	}

	access, err := normalizeAccess(config.Access)
	if err != nil {
		return Config{}, false, err
	}
	config.Access = access

	if err := config.normalizeRepos(); err != nil {
		return Config{}, false, err
	}

	if err := config.validateMetricsPaths(); err != nil {
		return Config{}, false, err
	}

	return config, false, nil
}

func (c *Config) validateMetricsPaths() error {
	if c.Metrics {
		if err := c.validateMetricsPathRepos(defaultMetricsPath); err != nil {
			return err
		}
	}
	for _, listener := range c.Listeners {
		if listener.metricsPath == "" {
			continue
		}
		if err := c.validateMetricsPathRepos(listener.metricsPath); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) validateMetricsPathRepos(path string) error {
	segment, _, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	if c.Repos[segment] != nil && !strings.Contains(segment, "*") {
		return fmt.Errorf("repo name %q conflicts with metrics path %q", segment, path)
	}
	return nil
}

func isReservedRepoName(name string) bool {
	switch name {
	case "keys", "locks", "snapshots", "data", "index", "config", "healthz", "readyz":
		return true
	default:
		return false
	}
}

const (
	repoNameToken  = "{repo}"
	repoMatchToken = "{repo_match}"
)

func containsRepoToken(s string) bool {
	return strings.Contains(s, repoNameToken) || strings.Contains(s, repoMatchToken)
}

var (
	repoNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	repoKeyPattern  = regexp.MustCompile(`^[A-Za-z0-9._*-]+$`)
)

func isValidRepoName(name string) bool {
	return name != "." && name != ".." && repoNamePattern.MatchString(name)
}

type repoPattern struct {
	key    string
	prefix string
	suffix string
}

func (p repoPattern) match(name string) (string, bool) {
	if len(name) <= len(p.prefix)+len(p.suffix) ||
		!strings.HasPrefix(name, p.prefix) || !strings.HasSuffix(name, p.suffix) {
		return "", false
	}
	return name[len(p.prefix) : len(name)-len(p.suffix)], true
}

func compareRepoPatterns(a, b repoPattern) int {
	if d := (len(b.prefix) + len(b.suffix)) - (len(a.prefix) + len(a.suffix)); d != 0 {
		return d
	}
	if d := len(b.prefix) - len(a.prefix); d != 0 {
		return d
	}
	return strings.Compare(a.key, b.key)
}

func compileRepoPatterns(repos map[string]*RepoConfig) []repoPattern {
	var patterns []repoPattern
	for key := range repos {
		if before, after, ok := strings.Cut(key, "*"); ok {
			patterns = append(patterns, repoPattern{key: key, prefix: before, suffix: after})
		}
	}
	slices.SortFunc(patterns, compareRepoPatterns)
	return patterns
}

func expandRepoTokens(s, repo, match string) string {
	s = strings.ReplaceAll(s, repoNameToken, repo)
	return strings.ReplaceAll(s, repoMatchToken, match)
}

type storageLocation struct {
	pool      string
	namespace string
}

func blobStorageLocations(bpc BlobPoolConfig) []storageLocation {
	if bpc.Pool == "" {
		return nil
	}
	locations := []storageLocation{{pool: bpc.Pool, namespace: bpc.Namespace}}
	if bpc.Lower != nil {
		locations = append(locations, storageLocation{pool: bpc.Lower.Pool, namespace: bpc.Lower.Namespace})
	}
	return locations
}

func (loc storageLocation) tokenFree() bool {
	return !containsRepoToken(loc.pool) &&
		!containsRepoToken(loc.namespace)
}

type storageCollision struct {
	repo      string
	otherRepo string
	blobTypes []string
}

func storageCollisions(repos map[string]*RepoConfig) []storageCollision {
	names := make([]string, 0, len(repos))
	for name, repo := range repos {
		if repo.BlobPools != nil {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	var collisions []storageCollision
	for i, a := range names {
		for _, b := range names[i+1:] {
			var blobTypes []string
			for _, bt := range AllBlobTypes {
				ta := blobStorageLocations(repos[a].BlobPools.getPoolForType(bt))
				tb := blobStorageLocations(repos[b].BlobPools.getPoolForType(bt))
				collides := false
				for _, x := range ta {
					for _, y := range tb {
						if x.tokenFree() && y.tokenFree() && x == y {
							collides = true
						}
					}
				}
				if collides {
					blobTypes = append(blobTypes, string(bt))
				}
			}
			if len(blobTypes) > 0 {
				collisions = append(collisions, storageCollision{repo: a, otherRepo: b, blobTypes: blobTypes})
			}
		}
	}
	return collisions
}

func (bp *BlobPool) forRepo(repo, match string) *BlobPool {
	out := *bp
	out.Namespace = expandRepoTokens(out.Namespace, repo, match)
	if bp.Lower != nil {
		lower := *bp.Lower
		lower.Namespace = expandRepoTokens(lower.Namespace, repo, match)
		out.Lower = &lower
	}
	return &out
}

func (c *Config) normalizeRepos() error {
	for name, repo := range c.Repos {
		if !repoKeyPattern.MatchString(name) {
			return fmt.Errorf("invalid repo name %q (may contain only letters, digits, '.', '_', '-', and one '*')", name)
		}
		if name == "." || name == ".." {
			return fmt.Errorf("invalid repo name %q (must not be \".\" or \"..\")", name)
		}
		if name != "default" && isReservedRepoName(name) {
			return fmt.Errorf("reserved repo name %q (conflicts with server path)", name)
		}
		if strings.Count(name, "*") > 1 {
			return fmt.Errorf("invalid repo pattern %q (may contain only one '*')", name)
		}
		if repo == nil {
			return fmt.Errorf("repo %q: configuration cannot be null", name)
		}

		if repo.BlobPools != nil && len(repo.Pools) > 0 {
			return fmt.Errorf("repo %q: cannot combine pools with blob_pools", name)
		}

		if repo.Access == "" {
			repo.Access = "rw"
		} else {
			access, err := normalizeAccess(repo.Access)
			if err != nil {
				return fmt.Errorf("repo %q: %w", name, err)
			}
			repo.Access = access
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

		if repo.BlobPools != nil {
			if err := repo.BlobPools.validateRepoTokens(name); err != nil {
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
	KeyringPath     string
	ClientID        string
	CephConf        string
	WriteBufferSize int64
	OSDOpTimeout    time.Duration
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

func (p *ServerConfigPools) validateRepoTokens(name string) error {
	_, _, dynamic := strings.Cut(name, "*")
	fields := []*BlobPoolConfig{&p.Config, &p.Keys, &p.Locks, &p.Snapshots, &p.Data, &p.Index}
	for i, bt := range AllBlobTypes {
		bpc := fields[i]
		if bpc.Pool == "" {
			continue
		}
		if containsRepoToken(bpc.Pool) || (bpc.Lower != nil && containsRepoToken(bpc.Lower.Pool)) {
			return fmt.Errorf("blob type %q: pool name cannot contain %q or %q (dynamic pool names are not supported)", bt, repoNameToken, repoMatchToken)
		}
		if !dynamic {
			if containsRepoToken(bpc.Namespace) || (bpc.Lower != nil && containsRepoToken(bpc.Lower.Namespace)) {
				return fmt.Errorf("blob type %q: %q and %q are only allowed in repo patterns", bt, repoNameToken, repoMatchToken)
			}
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

func parsePoolTarget(target, spec string) (pool, namespace string, err error) {
	pool, namespace, hasNamespace := strings.Cut(target, "/")
	if pool == "" {
		return "", "", fmt.Errorf("empty pool name in specification: %q", spec)
	}
	if hasNamespace && namespace == "" {
		return "", "", fmt.Errorf("empty namespace in specification: %q", spec)
	}
	if strings.Contains(namespace, "/") {
		return "", "", fmt.Errorf("namespace cannot contain '/' in specification: %q", spec)
	}
	return pool, namespace, nil
}

func parsePoolsConfig(pc poolsConfig) (ServerConfigPools, error) {
	if len(pc) == 0 {
		return ServerConfigPools{}, errors.New("no pool specifications provided")
	}

	typeToConfig := make(map[BlobType]BlobPoolConfig)
	typeToKey := make(map[BlobType]string)
	var catchAll *BlobPoolConfig

	for key, types := range pc {
		poolName, namespace, err := parsePoolTarget(key, key)
		if err != nil {
			return ServerConfigPools{}, err
		}
		bpc := BlobPoolConfig{Pool: poolName, Namespace: namespace}

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

	if _, _, err := parsePoolTarget(poolPart, spec); err != nil {
		return "", nil, err
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

func isValidBlobTypeForMapping(bt BlobType) bool {
	for _, valid := range AllBlobTypes {
		if bt == valid {
			return true
		}
	}
	return false
}
