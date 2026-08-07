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
	Access          string                 `json:"access,omitempty"`
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

type commandLineConfig struct {
	showVersion     bool
	configFile      string
	verbose         bool
	listeners       listenerFlags
	useStdio        bool
	shutdownTimeout time.Duration
	access          string
	maxIdleTime     time.Duration
	logFile         string
	keyringPath     string
	clientID        string
	poolSpecs       poolFlags
	cephConf        string
	striper         bool
	readBufferSize  int64
	writeBufferSize int64
	maxObjectSize   int64
	set             map[string]bool
}

func parseCommandLine(args []string) (commandLineConfig, error) {
	fs := flag.NewFlagSet("restic-rados-server", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "Usage: restic-rados-server [options]\n\nOptions:\n")
		fs.PrintDefaults()
	}

	var parsed commandLineConfig
	fs.BoolVar(&parsed.showVersion, "version", false, "print version and exit")
	fs.StringVar(&parsed.configFile, "config", "", "path to JSON configuration file")
	fs.BoolVar(&parsed.verbose, "v", false, "enable verbose logging")
	fs.BoolVar(&parsed.verbose, "verbose", false, "enable verbose logging")
	fs.Var(&parsed.listeners, "listen", "Address or Unix socket path to listen on, repeatable")
	fs.BoolVar(&parsed.useStdio, "stdio", false, "use HTTP/2 over stdin/stdout (default when no listeners specified)")
	fs.DurationVar(&parsed.shutdownTimeout, "shutdown-timeout", 60*time.Second, "graceful shutdown timeout for listeners")
	fs.StringVar(&parsed.access, "access", "", "maximum access level for the server: r/read-only, ra/read-append, rw/read-write")
	fs.DurationVar(&parsed.maxIdleTime, "max-idle-time", 0, "exit after duration with no active connections (e.g., 30s, 5m; 0 = disabled)")
	fs.StringVar(&parsed.logFile, "log-file", "", "path to log file (default: stderr)")
	fs.StringVar(&parsed.keyringPath, "keyring", "", "path to Ceph keyring file")
	fs.StringVar(&parsed.clientID, "id", "", "Ceph client ID (e.g., 'restic' for client.restic)")
	fs.Var(&parsed.poolSpecs, "pool", "Pool specification: 'pool[/namespace][:types]' where types is '*' or comma-separated list (repeatable, or semicolon-separated)")
	fs.StringVar(&parsed.cephConf, "ceph-conf", "", "path to ceph.conf file")
	fs.BoolVar(&parsed.striper, "striper", true, "enable librados striper for large objects")
	fs.Int64Var(&parsed.readBufferSize, "read-buffer-size", defaultReadBufferSize, "buffer size for reading objects in bytes")
	fs.Int64Var(&parsed.writeBufferSize, "write-buffer-size", defaultWriteBufferSize, "buffer size for writing objects in bytes")
	fs.Int64Var(&parsed.maxObjectSize, "max-object-size", 0, "max object size override (0 = use cluster config or 128MB default)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(os.Stderr)
			fs.Usage()
		}
		return commandLineConfig{}, err
	}

	parsed.set = make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		parsed.set[f.Name] = true
	})
	if parsed.set["access"] {
		access, err := normalizeAccess(parsed.access)
		if err != nil {
			return commandLineConfig{}, err
		}
		parsed.access = access
	}
	return parsed, nil
}

func (c *Config) applyCommandLine(parsed commandLineConfig) {
	if parsed.set["verbose"] || parsed.set["v"] {
		c.Verbose = parsed.verbose
	}
	if parsed.set["listen"] {
		c.Listeners = parsed.listeners
	}
	if parsed.set["stdio"] {
		c.Stdio = parsed.useStdio
	}
	if parsed.set["shutdown-timeout"] {
		c.ShutdownTimeout = Duration(parsed.shutdownTimeout)
	}
	if parsed.set["access"] {
		c.Access = parsed.access
	}
	if parsed.set["max-idle-time"] {
		c.MaxIdleTime = Duration(parsed.maxIdleTime)
	}
	if parsed.set["log-file"] {
		c.LogFile = parsed.logFile
	}
	if parsed.set["keyring"] {
		c.Keyring = parsed.keyringPath
	}
	if parsed.set["id"] {
		c.ClientID = parsed.clientID
	}
	if parsed.set["ceph-conf"] {
		c.CephConf = parsed.cephConf
	}
	if parsed.set["read-buffer-size"] {
		c.ReadBufferSize = parsed.readBufferSize
	}
	if parsed.set["write-buffer-size"] {
		c.WriteBufferSize = parsed.writeBufferSize
	}

	if parsed.set["pool"] || parsed.set["striper"] || parsed.set["max-object-size"] {
		def := c.defaultRepo()
		if parsed.set["pool"] {
			def.poolSpecs = parsed.poolSpecs
			def.Pools = nil
			def.BlobPools = nil
		}
		if parsed.set["striper"] {
			def.Striper = &parsed.striper
		}
		if parsed.set["max-object-size"] {
			def.MaxObjectSize = parsed.maxObjectSize
		}
	}
}

var envPrefixes = []string{"RESTIC_RADOS_SERVER_", "RESTIC_CEPH_SERVER_", "CEPH_RESTIC_SERVER_", "RADOS_RESTIC_SERVER_"}

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
	config := Config{
		ShutdownTimeout: Duration(60 * time.Second),
		Access:          "rw",
		ReadBufferSize:  defaultReadBufferSize,
		WriteBufferSize: defaultWriteBufferSize,
	}

	commandLine, err := parseCommandLine(args)
	if err != nil {
		return Config{}, false, err
	}

	if commandLine.showVersion {
		return Config{}, true, nil
	}

	configFile := commandLine.configFile
	if configFile == "" {
		configFile = getPrefixedEnv("CONFIG")
	}

	if configFile != "" {
		if err := config.loadFromFile(configFile); err != nil {
			return Config{}, false, fmt.Errorf("failed to load config file %s: %w", configFile, err)
		}
	}

	if err := config.loadFromEnv(); err != nil {
		return Config{}, false, err
	}
	config.applyCommandLine(commandLine)

	if config.ReadBufferSize <= 0 {
		return Config{}, false, fmt.Errorf("read-buffer-size must be positive, got %d", config.ReadBufferSize)
	}

	if config.WriteBufferSize <= 0 {
		return Config{}, false, fmt.Errorf("write-buffer-size must be positive, got %d", config.WriteBufferSize)
	}

	if config.Tailscale != nil && (config.Tailscale.Port < 0 || config.Tailscale.Port > 65535) {
		return Config{}, false, fmt.Errorf("tailscale port must be between 0 and 65535, got %d", config.Tailscale.Port)
	}

	access, err := normalizeAccess(config.Access)
	if err != nil {
		return Config{}, false, err
	}
	config.Access = access

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

const (
	repoNameToken  = "{repo}"
	repoMatchToken = "{repo_match}"
)

func containsRepoToken(s string) bool {
	return strings.Contains(s, repoNameToken) || strings.Contains(s, repoMatchToken)
}

func isValidRepoName(name string) bool {
	return name != "" && name != "." && name != ".." &&
		!strings.ContainsAny(name, "/{}* ") &&
		!strings.ContainsFunc(name, unicode.IsControl)
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
	prefix    string
}

func blobStorageLocations(bpc BlobPoolConfig) []storageLocation {
	if bpc.Pool == "" {
		return nil
	}
	locations := []storageLocation{{pool: bpc.Pool, namespace: bpc.Namespace, prefix: bpc.Prefix}}
	if bpc.Lower != nil {
		locations = append(locations, storageLocation{pool: bpc.Lower.Pool, namespace: bpc.Lower.Namespace, prefix: bpc.Lower.Prefix})
	}
	return locations
}

func (loc storageLocation) tokenFree() bool {
	return !containsRepoToken(loc.pool) &&
		!containsRepoToken(loc.namespace) &&
		!containsRepoToken(loc.prefix)
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
	out.Prefix = expandRepoTokens(out.Prefix, repo, match)
	if bp.Lower != nil {
		lower := *bp.Lower
		lower.Namespace = expandRepoTokens(lower.Namespace, repo, match)
		lower.Prefix = expandRepoTokens(lower.Prefix, repo, match)
		out.Lower = &lower
	}
	return &out
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
	Prefix        string `json:"prefix,omitempty"`
	Striped       *bool  `json:"striped,omitempty"`
	MaxObjectSize *int64 `json:"max_object_size,omitempty"`
}

type BlobPoolConfig struct {
	Pool          string           `json:"pool"`
	Namespace     string           `json:"namespace,omitempty"`
	Prefix        string           `json:"prefix,omitempty"`
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
		if bpc.Pool != "" || bpc.Namespace != "" || bpc.Prefix != "" || bpc.Striped != nil || bpc.MaxObjectSize != nil {
			return fmt.Errorf("blob type %q: cannot combine pool with upper/lower layers", bt)
		}
		if bpc.Upper.Pool == "" {
			return fmt.Errorf("blob type %q: upper pool name cannot be empty", bt)
		}
		if bpc.Lower.Pool == "" {
			return fmt.Errorf("blob type %q: lower pool name cannot be empty", bt)
		}
		if bpc.Upper.Pool == bpc.Lower.Pool && bpc.Upper.Namespace == bpc.Lower.Namespace && bpc.Upper.Prefix == bpc.Lower.Prefix {
			return fmt.Errorf("blob type %q: lower layer must differ from upper layer", bt)
		}

		bpc.Pool = bpc.Upper.Pool
		bpc.Namespace = bpc.Upper.Namespace
		bpc.Prefix = bpc.Upper.Prefix
		bpc.Striped = bpc.Upper.Striped
		bpc.MaxObjectSize = bpc.Upper.MaxObjectSize
		bpc.Upper = nil
	}

	for i, bt := range AllBlobTypes {
		bpc := fields[i]
		if strings.ContainsFunc(bpc.Prefix, unicode.IsControl) {
			return fmt.Errorf("blob type %q: prefix cannot contain control characters", bt)
		}
		if bpc.Lower != nil && strings.ContainsFunc(bpc.Lower.Prefix, unicode.IsControl) {
			return fmt.Errorf("blob type %q: lower layer prefix cannot contain control characters", bt)
		}
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
			if containsRepoToken(bpc.Namespace) || containsRepoToken(bpc.Prefix) ||
				(bpc.Lower != nil && (containsRepoToken(bpc.Lower.Namespace) || containsRepoToken(bpc.Lower.Prefix))) {
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
