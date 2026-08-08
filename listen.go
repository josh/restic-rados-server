package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/http/httpguts"
	"golang.org/x/net/http2"
	"tailscale.com/client/local"
	"tailscale.com/ipn"
	"tailscale.com/paths"
	"tailscale.com/tailcfg"
)

type PolicyOrigin uint8

const (
	PolicyOriginConfigured PolicyOrigin = iota
	PolicyOriginTrustedHeader
	PolicyOriginTailscale
)

type PolicyDocument struct {
	Origin     PolicyOrigin
	Capability string
	Value      json.RawMessage
}

type PolicyReducer func(context.Context, []PolicyDocument) (context.Context, error)

type PolicyReducerFactory func(ListenerInfo) (PolicyReducer, error)

type ListenerInfo struct {
	Endpoint             string
	Policy               json.RawMessage
	TrustedAppCapsHeader string
	AcceptAppCaps        []string
}

type ListenOptions struct {
	ShutdownTimeout       time.Duration
	TailscaleDrainTimeout time.Duration
	RuntimeName           string
	ConnState             func(net.Conn, http.ConnState)
}

type listenerKind uint8

const (
	listenerKindUnix listenerKind = iota
	listenerKindTCP
	listenerKindSystemd
	listenerKindTailscale
)

const (
	systemdListenFDStart         = 3
	listenerMaxUnixSocketPathLen = 107
	tailscaleAppCapsHeader       = "Tailscale-App-Capabilities"
	defaultTailscaleDrainTimeout = 10 * time.Second
)

type tailscaleListenerOptions struct {
	acceptAppCaps  []string
	socket         string
	https          *bool
	port           int
	upstreamSocket string
}

type listenerConfig struct {
	kind                 listenerKind
	endpoint             string
	address              string
	serviceName          string
	policy               json.RawMessage
	trustedAppCapsHeader string
	tailscale            tailscaleListenerOptions
	systemdFile          *os.File
	raw                  string
}

type ListenerConfigs []listenerConfig

type listenerSpec struct {
	Endpoint             string          `json:"endpoint"`
	Policy               json.RawMessage `json:"policy"`
	TrustedAppCapsHeader json.RawMessage `json:"trusted_app_caps_header"`
	Options              json.RawMessage `json:"options"`
}

type tailscaleListenerOptionsSpec struct {
	AcceptAppCaps  json.RawMessage `json:"accept_app_caps"`
	Socket         json.RawMessage `json:"socket"`
	HTTPS          json.RawMessage `json:"https"`
	Port           json.RawMessage `json:"port"`
	UpstreamSocket json.RawMessage `json:"upstream_socket"`
}

func decodeListenerJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err == nil {
		return errors.New("unexpected data after JSON value")
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func parseOptionalListenerString(raw json.RawMessage, field string) (string, bool, error) {
	if raw == nil {
		return "", false, nil
	}
	var value *string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false, fmt.Errorf("%s must be a string", field)
	}
	if value == nil {
		return "", false, fmt.Errorf("%s cannot be null", field)
	}
	if *value == "" {
		return "", false, fmt.Errorf("%s cannot be empty", field)
	}
	return *value, true, nil
}

func parseTailscaleListenerOptions(raw json.RawMessage) (tailscaleListenerOptions, error) {
	if raw == nil {
		return tailscaleListenerOptions{}, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return tailscaleListenerOptions{}, errors.New("options cannot be null")
	}
	var spec tailscaleListenerOptionsSpec
	if err := decodeListenerJSON(raw, &spec); err != nil {
		return tailscaleListenerOptions{}, fmt.Errorf("options: %w", err)
	}
	options := tailscaleListenerOptions{}
	if spec.AcceptAppCaps != nil {
		if bytes.Equal(bytes.TrimSpace(spec.AcceptAppCaps), []byte("null")) {
			return tailscaleListenerOptions{}, errors.New("options.accept_app_caps cannot be null")
		}
		if err := json.Unmarshal(spec.AcceptAppCaps, &options.acceptAppCaps); err != nil {
			return tailscaleListenerOptions{}, errors.New("options.accept_app_caps must be an array of strings")
		}
		seen := make(map[string]bool, len(options.acceptAppCaps))
		for _, capability := range options.acceptAppCaps {
			if capability == "" || strings.TrimSpace(capability) != capability {
				return tailscaleListenerOptions{}, errors.New("options.accept_app_caps entries must be non-empty strings without surrounding whitespace")
			}
			if seen[capability] {
				return tailscaleListenerOptions{}, fmt.Errorf("options.accept_app_caps contains duplicate capability %q", capability)
			}
			seen[capability] = true
		}
	}
	var err error
	if options.socket, _, err = parseOptionalListenerString(spec.Socket, "options.socket"); err != nil {
		return tailscaleListenerOptions{}, err
	}
	if options.upstreamSocket, _, err = parseOptionalListenerString(spec.UpstreamSocket, "options.upstream_socket"); err != nil {
		return tailscaleListenerOptions{}, err
	}
	if spec.HTTPS != nil {
		var value *bool
		if err := json.Unmarshal(spec.HTTPS, &value); err != nil {
			return tailscaleListenerOptions{}, errors.New("options.https must be a boolean")
		}
		if value == nil {
			return tailscaleListenerOptions{}, errors.New("options.https cannot be null")
		}
		options.https = value
	}
	if spec.Port != nil {
		var value *int
		if err := json.Unmarshal(spec.Port, &value); err != nil {
			return tailscaleListenerOptions{}, errors.New("options.port must be an integer")
		}
		if value == nil {
			return tailscaleListenerOptions{}, errors.New("options.port cannot be null")
		}
		if *value < 1 || *value > 65535 {
			return tailscaleListenerOptions{}, errors.New("options.port must be between 1 and 65535")
		}
		options.port = *value
	}
	return options, nil
}

func setListenerTCPAddress(config *listenerConfig, value, original string) error {
	if !strings.Contains(value, ":") {
		return fmt.Errorf("invalid --listen value %q: TCP listeners must specify host:port", original)
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("invalid --listen value %q: %w", original, err)
	}
	if port == "" {
		return fmt.Errorf("invalid --listen value %q: missing port", original)
	}
	config.address = net.JoinHostPort(host, port)
	return nil
}

func listenerUsesQueryParameters(value string) bool {
	_, query, found := strings.Cut(value, "?")
	return found && slices.ContainsFunc(strings.Split(query, "&"), func(part string) bool {
		key, _, hasValue := strings.Cut(part, "=")
		return hasValue && key != "" || key == "access" || strings.HasPrefix(key, "trusted-")
	})
}

func listenerQueryError(value string) error {
	return fmt.Errorf("invalid --listen value %q: listener query parameters are not supported; use a listener object in JSON configuration", value)
}

func parseListenerEndpoint(value string) (listenerConfig, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return listenerConfig{}, fmt.Errorf("invalid --listen value %q: empty endpoint", value)
	}
	config := listenerConfig{endpoint: trimmed, raw: trimmed}
	working := trimmed
	lower := strings.ToLower(working)
	if strings.HasPrefix(lower, "tailscale+svc:") {
		if strings.Contains(working, "?") {
			return listenerConfig{}, listenerQueryError(value)
		}
		label := working[len("tailscale+svc:"):]
		if label == "" {
			return listenerConfig{}, fmt.Errorf("invalid --listen value %q: missing Tailscale service name", value)
		}
		service := tailcfg.AsServiceName("svc:" + label)
		if service == "" {
			return listenerConfig{}, fmt.Errorf("invalid --listen value %q: Tailscale service name must be a DNS label", value)
		}
		config.kind = listenerKindTailscale
		config.serviceName = service.String()
		return config, nil
	}
	if strings.HasPrefix(lower, "systemd:") {
		if strings.Contains(working, "?") {
			return listenerConfig{}, listenerQueryError(value)
		}
		name := working[len("systemd:"):]
		if name == "" {
			return listenerConfig{}, fmt.Errorf("invalid --listen value %q: missing systemd socket name", value)
		}
		if strings.Contains(name, ":") {
			return listenerConfig{}, fmt.Errorf("invalid --listen value %q: systemd socket names cannot contain colons", value)
		}
		config.kind = listenerKindSystemd
		config.address = name
		return config, nil
	}
	switch {
	case strings.HasPrefix(lower, "unix:"):
		working = strings.TrimPrefix(working[len("unix:"):], "//")
		config.kind = listenerKindUnix
	case strings.HasPrefix(lower, "tcp:"):
		working = strings.TrimPrefix(working[len("tcp:"):], "//")
		config.kind = listenerKindTCP
	default:
		if strings.Contains(working, "/") || strings.HasPrefix(working, "@") || !strings.Contains(working, ":") {
			config.kind = listenerKindUnix
		} else {
			config.kind = listenerKindTCP
		}
	}
	if config.kind == listenerKindUnix {
		if working == "" {
			return listenerConfig{}, fmt.Errorf("invalid --listen value %q: missing Unix socket path", value)
		}
		if listenerUsesQueryParameters(working) {
			return listenerConfig{}, listenerQueryError(value)
		}
		config.address = working
		return config, nil
	}
	if strings.Contains(working, "?") {
		return listenerConfig{}, listenerQueryError(value)
	}
	if err := setListenerTCPAddress(&config, working, value); err != nil {
		return listenerConfig{}, err
	}
	return config, nil
}

func parseStructuredListener(data []byte) (listenerConfig, error) {
	var spec listenerSpec
	if err := decodeListenerJSON(data, &spec); err != nil {
		return listenerConfig{}, err
	}
	if spec.Endpoint == "" {
		return listenerConfig{}, errors.New("listener endpoint is required")
	}
	config, err := parseListenerEndpoint(spec.Endpoint)
	if err != nil {
		return listenerConfig{}, err
	}
	config.raw = string(bytes.TrimSpace(data))
	if spec.Policy != nil {
		config.policy = slices.Clone(spec.Policy)
	}
	header, _, err := parseOptionalListenerString(spec.TrustedAppCapsHeader, "trusted_app_caps_header")
	if err != nil {
		return listenerConfig{}, err
	}
	if header != "" {
		if !httpguts.ValidHeaderFieldName(header) {
			return listenerConfig{}, fmt.Errorf("trusted_app_caps_header %q is not a valid HTTP header name", header)
		}
		if strings.EqualFold(header, tailscaleAppCapsHeader) {
			return listenerConfig{}, fmt.Errorf("trusted_app_caps_header %q is reserved for Tailscale app capabilities", header)
		}
		if config.kind == listenerKindTailscale {
			return listenerConfig{}, errors.New("trusted_app_caps_header is not supported for tailscale+svc endpoints; use options.accept_app_caps")
		}
		config.trustedAppCapsHeader = header
	}
	options, err := parseTailscaleListenerOptions(spec.Options)
	if err != nil {
		return listenerConfig{}, err
	}
	if config.kind != listenerKindTailscale && (options.socket != "" || options.upstreamSocket != "" || options.https != nil || options.port != 0) {
		return listenerConfig{}, errors.New("socket, https, port, and upstream_socket options are only supported for tailscale+svc endpoints")
	}
	config.tailscale = options
	return config, nil
}

func (configs *ListenerConfigs) Set(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("invalid --listen value %q: empty specification", value)
	}
	if strings.HasPrefix(trimmed, "{") {
		return errors.New("--listen accepts endpoint strings only; put listener objects in the JSON configuration")
	}
	config, err := parseListenerEndpoint(trimmed)
	if err != nil {
		return err
	}
	*configs = append(*configs, config)
	return nil
}

func (configs ListenerConfigs) String() string {
	parts := make([]string, len(configs))
	for i := range configs {
		parts[i] = configs[i].raw
	}
	return strings.Join(parts, ",")
}

func (configs *ListenerConfigs) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.New("listen must be an array")
	}
	var entries []json.RawMessage
	if err := decodeListenerJSON(data, &entries); err != nil {
		return err
	}
	parsed := make(ListenerConfigs, 0, len(entries))
	for _, entry := range entries {
		trimmed := bytes.TrimSpace(entry)
		if trimmed[0] == '{' {
			config, err := parseStructuredListener(trimmed)
			if err != nil {
				return fmt.Errorf("invalid listen entry %s: %w", entry, err)
			}
			parsed = append(parsed, config)
			continue
		}
		var endpoint string
		if err := json.Unmarshal(trimmed, &endpoint); err != nil {
			return fmt.Errorf("invalid listen entry %s: must be a string or object", entry)
		}
		config, err := parseListenerEndpoint(endpoint)
		if err != nil {
			return err
		}
		parsed = append(parsed, config)
	}
	*configs = parsed
	return nil
}

func listenerInfo(config listenerConfig) ListenerInfo {
	return ListenerInfo{
		Endpoint:             config.endpoint,
		Policy:               slices.Clone(config.policy),
		TrustedAppCapsHeader: config.trustedAppCapsHeader,
		AcceptAppCaps:        slices.Clone(config.tailscale.acceptAppCaps),
	}
}

type preparedListener struct {
	config       listenerConfig
	listener     net.Listener
	reducer      PolicyReducer
	registration *tailscaleRegistration
}

type PreparedListeners struct {
	listeners    []preparedListener
	options      ListenOptions
	mu           sync.Mutex
	servers      []*http.Server
	serving      bool
	closed       bool
	closeNotify  chan struct{}
	withdrawOnce sync.Once
	withdrawErr  error
	closeOnce    sync.Once
	closeErr     error
}

func (prepared *PreparedListeners) Len() int {
	if prepared == nil {
		return 0
	}
	return len(prepared.listeners)
}

func defaultListenerRuntimeName() string {
	executable, err := os.Executable()
	if err == nil {
		if name := filepath.Base(executable); name != "" && name != "." {
			return name
		}
	}
	if name := filepath.Base(os.Args[0]); name != "" && name != "." {
		return name
	}
	return "server"
}

func normalizeListenOptions(options ListenOptions) (ListenOptions, error) {
	if options.ShutdownTimeout < 0 {
		return ListenOptions{}, errors.New("shutdown timeout cannot be negative")
	}
	if options.TailscaleDrainTimeout < 0 {
		return ListenOptions{}, errors.New("tailscale drain timeout cannot be negative")
	}
	if options.TailscaleDrainTimeout == 0 {
		options.TailscaleDrainTimeout = defaultTailscaleDrainTimeout
	}
	if options.RuntimeName == "" {
		options.RuntimeName = defaultListenerRuntimeName()
	}
	if options.RuntimeName == "." || options.RuntimeName == ".." || filepath.Base(options.RuntimeName) != options.RuntimeName || strings.ContainsAny(options.RuntimeName, `/\`) {
		return ListenOptions{}, fmt.Errorf("invalid runtime name %q", options.RuntimeName)
	}
	return options, nil
}

func systemdListenerConfigs() []listenerConfig {
	defer func() {
		_ = os.Unsetenv("LISTEN_PID")
		_ = os.Unsetenv("LISTEN_FDS")
		_ = os.Unsetenv("LISTEN_FDNAMES")
	}()
	pid, err := strconv.Atoi(os.Getenv("LISTEN_PID"))
	if err != nil || pid != os.Getpid() {
		return nil
	}
	count, err := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if err != nil || count <= 0 {
		return nil
	}
	names := strings.Split(os.Getenv("LISTEN_FDNAMES"), ":")
	configs := make([]listenerConfig, 0, count)
	for offset := 0; offset < count; offset++ {
		fd := systemdListenFDStart + offset
		syscall.CloseOnExec(fd)
		name := "LISTEN_FD_" + strconv.Itoa(fd)
		if offset < len(names) && names[offset] != "" {
			name = names[offset]
		}
		file := os.NewFile(uintptr(fd), name)
		configs = append(configs, listenerConfig{
			kind:        listenerKindSystemd,
			endpoint:    "systemd:" + name,
			address:     name,
			raw:         "systemd:" + name,
			systemdFile: file,
		})
	}
	return configs
}

func closeSystemdFiles(configs []listenerConfig) {
	for i := range configs {
		if configs[i].systemdFile != nil {
			_ = configs[i].systemdFile.Close()
			configs[i].systemdFile = nil
		}
	}
}

func resolveSystemdListeners(configured ListenerConfigs) ([]listenerConfig, error) {
	bindings := make(map[string]listenerConfig)
	result := make([]listenerConfig, 0, len(configured))
	for _, config := range configured {
		if config.kind != listenerKindSystemd {
			result = append(result, config)
			continue
		}
		if _, exists := bindings[config.address]; exists {
			return nil, fmt.Errorf("duplicate systemd listener configuration for %q", config.endpoint)
		}
		bindings[config.address] = config
	}
	inherited := systemdListenerConfigs()
	matched := make(map[string]bool, len(bindings))
	for i := range inherited {
		if binding, ok := bindings[inherited[i].address]; ok {
			inherited[i].policy = slices.Clone(binding.policy)
			inherited[i].trustedAppCapsHeader = binding.trustedAppCapsHeader
			inherited[i].tailscale = binding.tailscale
			inherited[i].tailscale.acceptAppCaps = slices.Clone(binding.tailscale.acceptAppCaps)
			inherited[i].raw = binding.raw
			matched[inherited[i].address] = true
		}
		result = append(result, inherited[i])
	}
	for name := range bindings {
		if !matched[name] {
			closeSystemdFiles(inherited)
			return nil, fmt.Errorf("systemd listener configuration %q has no matching inherited socket", name)
		}
	}
	return result, nil
}

func prepareListenerUnixSocketPath(path string) error {
	if strings.HasPrefix(path, "@") {
		return nil
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		if _, linkErr := os.Lstat(path); linkErr == nil {
			if removeErr := os.Remove(path); removeErr != nil {
				return fmt.Errorf("failed to remove dangling symlink %q: %w", path, removeErr)
			}
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to stat socket path %q: %w", path, err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to remove %q: not a socket", path)
	}
	connection, dialErr := net.DialTimeout("unix", path, time.Second)
	if dialErr == nil {
		_ = connection.Close()
		return fmt.Errorf("refusing to bind %q: another server is already listening", path)
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("refusing to remove %q: cannot verify socket is stale: %w", path, dialErr)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to remove stale socket %q: %w", path, err)
	}
	return nil
}

func warnIfTrustedHeaderIsExposed(config listenerConfig, listener net.Listener) {
	address, isTCP := listener.Addr().(*net.TCPAddr)
	trustsDynamicPolicy := config.trustedAppCapsHeader != "" || len(config.tailscale.acceptAppCaps) != 0
	if trustsDynamicPolicy && isTCP && !address.IP.IsLoopback() {
		slog.Warn("capability-trusting listener bound to a non-loopback address; only trust app capability headers behind a proxy that replaces them", "address", listener.Addr().String(), "trusted_app_caps_header", config.trustedAppCapsHeader, "accept_app_caps", config.tailscale.acceptAppCaps)
	}
}

func listenerRuntimeBaseDirectory() string {
	if directory := os.Getenv("RUNTIME_DIRECTORY"); directory != "" {
		return directory
	}
	if directory := os.Getenv("XDG_RUNTIME_DIR"); directory != "" {
		return directory
	}
	return os.TempDir()
}

func ensureListenerPrivateDirectory(directory string) error {
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return err
		}
		return os.Chmod(directory, 0o700)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("upstream socket directory %s is not a directory", directory)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && int(stat.Uid) != os.Getuid() {
		return fmt.Errorf("upstream socket directory %s is owned by uid %d", directory, stat.Uid)
	}
	return os.Chmod(directory, 0o700)
}

func tailscaleUpstreamPath(config listenerConfig, runtimeName string) (string, error) {
	path := config.tailscale.upstreamSocket
	if path == "" {
		directory := filepath.Join(listenerRuntimeBaseDirectory(), fmt.Sprintf("%s.%d", runtimeName, os.Getuid()))
		path = filepath.Join(directory, strings.TrimPrefix(config.serviceName, "svc:")+".sock")
	}
	if len(path) > listenerMaxUnixSocketPathLen {
		return "", fmt.Errorf("upstream_socket path too long (%d > %d bytes): %s", len(path), listenerMaxUnixSocketPathLen, path)
	}
	return path, nil
}

func prepareTailscaleUpstreamPath(config listenerConfig, path string) error {
	if config.tailscale.upstreamSocket != "" {
		return os.MkdirAll(filepath.Dir(path), 0o700)
	}
	return ensureListenerPrivateDirectory(filepath.Dir(path))
}

func setServiceAdvertised(ctx context.Context, client *local.Client, service string, advertise bool) (bool, error) {
	preferences, err := client.GetPrefs(ctx)
	if err != nil {
		return false, err
	}
	hasService := slices.Contains(preferences.AdvertiseServices, service)
	if advertise == hasService {
		return false, nil
	}
	updated := slices.Clone(preferences.AdvertiseServices)
	if advertise {
		updated = append(updated, service)
	} else {
		updated = slices.DeleteFunc(updated, func(current string) bool { return current == service })
	}
	_, err = client.EditPrefs(ctx, &ipn.MaskedPrefs{
		AdvertiseServicesSet: true,
		Prefs:                ipn.Prefs{AdvertiseServices: updated},
	})
	return true, err
}

func restoreTailscaleService(ctx context.Context, client *local.Client, service string, previous, installed *ipn.ServiceConfig, hadPrevious bool) error {
	config, err := client.GetServeConfig(ctx)
	if err != nil {
		return err
	}
	if config == nil {
		if !hadPrevious {
			return nil
		}
		return fmt.Errorf("serve configuration changed while %s was running", service)
	}
	serviceName := tailcfg.ServiceName(service)
	current, exists := config.Services[serviceName]
	switch {
	case hadPrevious && exists && reflect.DeepEqual(current, previous):
		return nil
	case !hadPrevious && !exists:
		return nil
	case !exists || !reflect.DeepEqual(current, installed):
		return fmt.Errorf("serve configuration changed while %s was running", service)
	}
	if hadPrevious {
		config.Services[serviceName] = previous.Clone()
	} else {
		delete(config.Services, serviceName)
	}
	return client.SetServeConfig(ctx, config)
}

type tailscaleRegistration struct {
	client         *local.Client
	service        string
	previous       *ipn.ServiceConfig
	installed      *ipn.ServiceConfig
	hadPrevious    bool
	addedAdvertise bool
	once           sync.Once
	err            error
}

func (registration *tailscaleRegistration) withdraw(ctx context.Context) error {
	registration.once.Do(func() {
		var errs []error
		if registration.addedAdvertise {
			if _, err := setServiceAdvertised(ctx, registration.client, registration.service, false); err != nil {
				errs = append(errs, fmt.Errorf("withdraw %s: %w", registration.service, err))
			}
		}
		if err := restoreTailscaleService(ctx, registration.client, registration.service, registration.previous, registration.installed, registration.hadPrevious); err != nil {
			errs = append(errs, fmt.Errorf("restore serve handler %s: %w", registration.service, err))
		}
		registration.err = errors.Join(errs...)
	})
	return registration.err
}

func withdrawTailscaleRegistration(registration *tailscaleRegistration, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return registration.withdraw(ctx)
}

func tailscaleSocket(socket string) string {
	if socket == "" {
		socket = paths.DefaultTailscaledSocket()
	}
	socket = filepath.Clean(socket)
	if absolute, err := filepath.Abs(socket); err == nil {
		socket = filepath.Clean(absolute)
	}
	if resolved, err := filepath.EvalSymlinks(socket); err == nil {
		socket = filepath.Clean(resolved)
	}
	return socket
}

func prepareTailscaleListener(ctx context.Context, config listenerConfig, runtimeName string, tailscaleDrainTimeout time.Duration) (net.Listener, *tailscaleRegistration, error) {
	upstream, err := tailscaleUpstreamPath(config, runtimeName)
	if err != nil {
		return nil, nil, fmt.Errorf("tailscale %s: %w", config.serviceName, err)
	}
	if err := prepareTailscaleUpstreamPath(config, upstream); err != nil {
		return nil, nil, fmt.Errorf("tailscale %s: prepare upstream socket path: %w", config.serviceName, err)
	}
	if err := prepareListenerUnixSocketPath(upstream); err != nil {
		return nil, nil, fmt.Errorf("tailscale %s: %w", config.serviceName, err)
	}
	listener, err := net.Listen("unix", upstream)
	if err != nil {
		return nil, nil, fmt.Errorf("tailscale %s: bind upstream socket: %w", config.serviceName, err)
	}
	var registration *tailscaleRegistration
	fail := func(err error) (net.Listener, *tailscaleRegistration, error) {
		if registration != nil {
			err = errors.Join(err, withdrawTailscaleRegistration(registration, tailscaleDrainTimeout))
		}
		_ = listener.Close()
		return nil, nil, err
	}
	if err := os.Chmod(upstream, 0o600); err != nil {
		return fail(fmt.Errorf("tailscale %s: restrict upstream socket permissions: %w", config.serviceName, err))
	}
	client := &local.Client{Socket: tailscaleSocket(config.tailscale.socket)}
	status, err := client.StatusWithoutPeers(ctx)
	if err != nil {
		return fail(fmt.Errorf("tailscale %s: cannot reach tailscaled: %w", config.serviceName, err))
	}
	if status.CurrentTailnet == nil {
		return fail(fmt.Errorf("tailscale %s: node is not connected to a tailnet", config.serviceName))
	}
	useHTTPS := config.tailscale.https == nil || *config.tailscale.https
	port := 80
	if useHTTPS {
		port = 443
	}
	if config.tailscale.port != 0 {
		port = config.tailscale.port
	}
	proxy, err := ipn.ExpandProxyTargetValue("unix:"+upstream, []string{"unix"}, "http")
	if err != nil {
		return fail(fmt.Errorf("tailscale %s: %w", config.serviceName, err))
	}
	handler := &ipn.HTTPHandler{Proxy: proxy}
	for _, capability := range config.tailscale.acceptAppCaps {
		handler.AcceptAppCaps = append(handler.AcceptAppCaps, tailcfg.PeerCapability(capability))
	}
	serveConfig, err := client.GetServeConfig(ctx)
	if err != nil {
		return fail(fmt.Errorf("tailscale %s: get serve config: %w", config.serviceName, err))
	}
	if serveConfig == nil {
		serveConfig = new(ipn.ServeConfig)
	}
	serviceName := tailcfg.ServiceName(config.serviceName)
	previous, hadPrevious := serveConfig.Services[serviceName]
	previous = previous.Clone()
	serveConfig.SetWebHandler(handler, config.serviceName, uint16(port), "/", useHTTPS, status.CurrentTailnet.MagicDNSSuffix)
	installed := serveConfig.Services[serviceName].Clone()
	registration = &tailscaleRegistration{
		client:      client,
		service:     config.serviceName,
		previous:    previous,
		installed:   installed,
		hadPrevious: hadPrevious,
	}
	if err := client.SetServeConfig(ctx, serveConfig); err != nil {
		return fail(fmt.Errorf("tailscale %s: set serve config: %w", config.serviceName, err))
	}
	addedAdvertise, err := setServiceAdvertised(ctx, client, config.serviceName, true)
	registration.addedAdvertise = addedAdvertise
	if err != nil {
		return fail(fmt.Errorf("tailscale %s: advertise service: %w", config.serviceName, err))
	}
	slog.Info("advertising tailscale service", "service", config.serviceName, "port", port, "upstream", upstream)
	return listener, registration, nil
}

func bindListener(ctx context.Context, config listenerConfig, runtimeName string, tailscaleDrainTimeout time.Duration) (net.Listener, *tailscaleRegistration, error) {
	switch config.kind {
	case listenerKindUnix:
		if err := prepareListenerUnixSocketPath(config.address); err != nil {
			return nil, nil, err
		}
		listener, err := net.Listen("unix", config.address)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create Unix socket listener: %w", err)
		}
		return listener, nil, nil
	case listenerKindTCP:
		listener, err := net.Listen("tcp", config.address)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create TCP listener: %w", err)
		}
		warnIfTrustedHeaderIsExposed(config, listener)
		return listener, nil, nil
	case listenerKindSystemd:
		if config.systemdFile == nil {
			return nil, nil, fmt.Errorf("systemd listener %s unavailable", config.endpoint)
		}
		listener, err := net.FileListener(config.systemdFile)
		_ = config.systemdFile.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("systemd listener %s unavailable: %w", config.endpoint, err)
		}
		warnIfTrustedHeaderIsExposed(config, listener)
		return listener, nil, nil
	case listenerKindTailscale:
		return prepareTailscaleListener(ctx, config, runtimeName, tailscaleDrainTimeout)
	default:
		return nil, nil, fmt.Errorf("unsupported listener endpoint %q", config.endpoint)
	}
}

func preflightTailscale(configs []listenerConfig, runtimeName string) error {
	services := make(map[string]bool)
	upstreams := make(map[string]string)
	for _, config := range configs {
		if config.kind != listenerKindTailscale {
			continue
		}
		if services[config.serviceName] {
			return fmt.Errorf("duplicate tailscale service %q", config.serviceName)
		}
		services[config.serviceName] = true
		path, err := tailscaleUpstreamPath(config, runtimeName)
		if err != nil {
			return fmt.Errorf("tailscale %s: %w", config.serviceName, err)
		}
		if previous, exists := upstreams[path]; exists {
			return fmt.Errorf("tailscale upstream_socket %q cannot be shared by multiple tailscale services %s and %s", path, previous, config.serviceName)
		}
		upstreams[path] = config.serviceName
	}
	return nil
}

func PrepareListeners(ctx context.Context, configs ListenerConfigs, factory PolicyReducerFactory, options ListenOptions) (_ *PreparedListeners, err error) {
	if factory == nil {
		return nil, errors.New("policy reducer factory is required")
	}
	options, err = normalizeListenOptions(options)
	if err != nil {
		return nil, err
	}
	resolved, err := resolveSystemdListeners(configs)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			closeSystemdFiles(resolved)
		}
	}()
	if err := preflightTailscale(resolved, options.RuntimeName); err != nil {
		return nil, err
	}
	listeners := make([]preparedListener, 0, len(resolved))
	for _, config := range resolved {
		reducer, factoryErr := factory(listenerInfo(config))
		if factoryErr != nil {
			return nil, fmt.Errorf("listener %s policy: %w", config.endpoint, factoryErr)
		}
		if reducer == nil {
			return nil, fmt.Errorf("listener %s policy reducer is nil", config.endpoint)
		}
		listeners = append(listeners, preparedListener{config: config, reducer: reducer})
	}
	prepared := &PreparedListeners{listeners: listeners, options: options}
	defer func() {
		if err != nil {
			err = errors.Join(err, prepared.Close())
		}
	}()
	for i := range prepared.listeners {
		candidate := &prepared.listeners[i]
		listener, registration, bindErr := bindListener(ctx, candidate.config, options.RuntimeName, options.TailscaleDrainTimeout)
		if bindErr != nil {
			return nil, fmt.Errorf("listener %s: %w", candidate.config.endpoint, bindErr)
		}
		candidate.listener = listener
		candidate.registration = registration
	}
	return prepared, nil
}

func takeListenerHeader(header http.Header, name string) []string {
	var values []string
	for key, current := range header {
		if strings.EqualFold(key, name) {
			values = append(values, current...)
			delete(header, key)
		}
	}
	return values
}

func listenerHeaderValue(value string) ([]byte, error) {
	trimmed := bytes.TrimSpace([]byte(value))
	if !json.Valid(trimmed) {
		return nil, errors.New("header is not valid JSON")
	}
	return slices.Clone(trimmed), nil
}

func decodeTailscaleHeaderValue(value string) ([]byte, error) {
	if strings.HasPrefix(strings.TrimSpace(value), "=?") {
		decoded, err := new(mime.WordDecoder).DecodeHeader(value)
		if err != nil {
			return nil, err
		}
		value = decoded
	}
	return listenerHeaderValue(value)
}

func listenerPolicyDocuments(request *http.Request, config listenerConfig) ([]PolicyDocument, error) {
	documents := make([]PolicyDocument, 0, 1)
	if config.policy != nil {
		documents = append(documents, PolicyDocument{Origin: PolicyOriginConfigured, Value: slices.Clone(config.policy)})
	}
	var trustedValues []string
	if config.trustedAppCapsHeader != "" {
		trustedValues = takeListenerHeader(request.Header, config.trustedAppCapsHeader)
	}
	tailscaleValues := takeListenerHeader(request.Header, tailscaleAppCapsHeader)
	if len(trustedValues) > 1 {
		return nil, fmt.Errorf("trusted app capabilities header %q appears more than once", config.trustedAppCapsHeader)
	}
	if len(trustedValues) == 1 {
		value, err := listenerHeaderValue(trustedValues[0])
		if err != nil {
			return nil, fmt.Errorf("trusted app capabilities header %q: %w", config.trustedAppCapsHeader, err)
		}
		documents = append(documents, PolicyDocument{Origin: PolicyOriginTrustedHeader, Value: value})
	}
	if len(config.tailscale.acceptAppCaps) == 0 {
		return documents, nil
	}
	if len(tailscaleValues) > 1 {
		return nil, errors.New("Tailscale app capabilities header appears more than once")
	}
	if len(tailscaleValues) == 0 {
		return documents, nil
	}
	value, err := decodeTailscaleHeaderValue(tailscaleValues[0])
	if err != nil {
		return nil, fmt.Errorf("Tailscale app capabilities header: %w", err)
	}
	if len(value) == 0 || value[0] != '{' {
		return nil, errors.New("Tailscale app capabilities header must contain a JSON object")
	}
	var capabilities tailcfg.PeerCapMap
	if err := json.Unmarshal(value, &capabilities); err != nil {
		return nil, fmt.Errorf("Tailscale app capabilities header: %w", err)
	}
	for _, accepted := range config.tailscale.acceptAppCaps {
		values, ok := capabilities[tailcfg.PeerCapability(accepted)]
		if !ok {
			continue
		}
		if len(values) == 0 {
			documents = append(documents, PolicyDocument{Origin: PolicyOriginTailscale, Capability: accepted})
			continue
		}
		for _, raw := range values {
			documents = append(documents, PolicyDocument{
				Origin:     PolicyOriginTailscale,
				Capability: accepted,
				Value:      json.RawMessage(raw),
			})
		}
	}
	return documents, nil
}

func listenerPolicyHandler(config listenerConfig, reducer PolicyReducer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		documents, err := listenerPolicyDocuments(request, config)
		if err != nil {
			http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		reduced, err := reducer(request.Context(), documents)
		if err != nil || reduced == nil {
			http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		next.ServeHTTP(response, request.WithContext(reduced))
	})
}

func (prepared *PreparedListeners) withdraw() error {
	if prepared == nil {
		return nil
	}
	prepared.withdrawOnce.Do(func() {
		groups := make(map[string][]*tailscaleRegistration)
		for i := range prepared.listeners {
			registration := prepared.listeners[i].registration
			if registration != nil {
				groups[registration.client.Socket] = append(groups[registration.client.Socket], registration)
			}
		}
		errorChannel := make(chan error, len(prepared.listeners))
		var wait sync.WaitGroup
		for _, registrations := range groups {
			registrations := registrations
			wait.Go(func() {
				for _, registration := range registrations {
					if err := withdrawTailscaleRegistration(registration, prepared.options.TailscaleDrainTimeout); err != nil {
						errorChannel <- err
					}
				}
			})
		}
		wait.Wait()
		close(errorChannel)
		var errs []error
		for err := range errorChannel {
			errs = append(errs, err)
		}
		prepared.withdrawErr = errors.Join(errs...)
	})
	return prepared.withdrawErr
}

func (prepared *PreparedListeners) Close() error {
	if prepared == nil {
		return nil
	}
	prepared.closeOnce.Do(func() {
		prepared.mu.Lock()
		prepared.closed = true
		if prepared.closeNotify == nil {
			prepared.closeNotify = make(chan struct{})
		}
		close(prepared.closeNotify)
		servers := slices.Clone(prepared.servers)
		prepared.mu.Unlock()
		errs := []error{prepared.withdraw()}
		for _, server := range servers {
			if err := server.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		for i := range prepared.listeners {
			listener := prepared.listeners[i].listener
			if listener == nil {
				continue
			}
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		prepared.closeErr = errors.Join(errs...)
	})
	return prepared.closeErr
}

func (prepared *PreparedListeners) isClosed() bool {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	return prepared.closed
}

func shutdownListenerServers(servers []*http.Server, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var wait sync.WaitGroup
	errorChannel := make(chan error, len(servers))
	for _, server := range servers {
		wait.Go(func() {
			if err := server.Shutdown(ctx); err != nil {
				errorChannel <- err
				_ = server.Close()
			}
		})
	}
	wait.Wait()
	close(errorChannel)
	var errs []error
	for err := range errorChannel {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func recordListenerShutdownError(target *error, normal bool, message string, err error) {
	if err == nil {
		return
	}
	if normal {
		slog.Warn(message, "error", err)
	} else if *target == nil {
		*target = err
	}
}

func (prepared *PreparedListeners) Serve(ctx context.Context, handler http.Handler) error {
	if prepared == nil {
		return errors.New("prepared listeners are nil")
	}
	if handler == nil {
		return errors.New("listener handler is nil")
	}
	requestContext := context.WithoutCancel(ctx)
	servers := make([]*http.Server, 0, len(prepared.listeners))
	for i := range prepared.listeners {
		listener := prepared.listeners[i]
		server := &http.Server{
			Handler:   listenerPolicyHandler(listener.config, listener.reducer, handler),
			ConnState: prepared.options.ConnState,
			BaseContext: func(net.Listener) context.Context {
				return requestContext
			},
		}
		if err := http2.ConfigureServer(server, &http2.Server{}); err != nil {
			return fmt.Errorf("configure HTTP/2 for %s: %w", listener.config.endpoint, err)
		}
		servers = append(servers, server)
	}
	prepared.mu.Lock()
	if prepared.closed {
		prepared.mu.Unlock()
		return errors.New("prepared listeners are closed")
	}
	if prepared.serving {
		prepared.mu.Unlock()
		return errors.New("prepared listeners are already serving")
	}
	prepared.serving = true
	prepared.servers = servers
	if prepared.closeNotify == nil {
		prepared.closeNotify = make(chan struct{})
	}
	closeNotify := prepared.closeNotify
	prepared.mu.Unlock()
	if len(servers) == 0 {
		select {
		case <-ctx.Done():
		case <-closeNotify:
		}
		if err := prepared.Close(); err != nil {
			slog.Warn("listener close failed during shutdown", "error", err)
		}
		return nil
	}
	type result struct {
		endpoint string
		err      error
	}
	results := make(chan result, len(servers))
	for i := range servers {
		server := servers[i]
		listener := prepared.listeners[i]
		slog.Info("listening", "address", listener.config.endpoint)
		go func() {
			err := server.Serve(listener.listener)
			results <- result{endpoint: listener.config.endpoint, err: err}
		}()
	}
	remaining := len(servers)
	var firstErr error
	normalShutdown := false
	select {
	case <-ctx.Done():
		normalShutdown = true
	case result := <-results:
		remaining--
		closed := prepared.isClosed()
		if result.err != nil && !errors.Is(result.err, http.ErrServerClosed) && !closed {
			firstErr = fmt.Errorf("listener %s error: %w", result.endpoint, result.err)
		} else if !closed {
			firstErr = fmt.Errorf("listener %s stopped unexpectedly", result.endpoint)
		} else {
			normalShutdown = true
		}
	}
	recordListenerShutdownError(&firstErr, normalShutdown, "listener withdrawal failed during shutdown", prepared.withdraw())
	recordListenerShutdownError(&firstErr, normalShutdown, "listener shutdown did not complete cleanly", shutdownListenerServers(servers, prepared.options.ShutdownTimeout))
	for remaining > 0 {
		<-results
		remaining--
	}
	recordListenerShutdownError(&firstErr, normalShutdown, "listener close failed during shutdown", prepared.Close())
	if normalShutdown {
		return nil
	}
	return firstErr
}
