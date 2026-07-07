package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/http2"
)

type listenerType string

const (
	listenerTypeStdio   listenerType = "stdio"
	listenerTypeUnix    listenerType = "unix"
	listenerTypeTCP     listenerType = "tcp"
	listenerTypeSystemd listenerType = "systemd"
)

const listenFdsStart = 3

type listenerConfig struct {
	kind                 listenerType
	address              string
	raw                  string
	file                 *os.File
	trustedCapsHeader    string
	trustedTailscaleCaps string
}

func (cfg listenerConfig) trustsCaps() bool {
	return cfg.trustedCapsHeader != "" || cfg.trustedTailscaleCaps != ""
}

func (cfg listenerConfig) Close() {
	if cfg.file != nil {
		_ = cfg.file.Close()
	}
}

func prepareUnixSocketPath(path string) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		if _, lerr := os.Lstat(path); lerr == nil {
			if rerr := os.Remove(path); rerr != nil {
				return fmt.Errorf("failed to remove dangling symlink %q: %w", path, rerr)
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
	conn, dialErr := net.DialTimeout("unix", path, time.Second)
	if dialErr == nil {
		_ = conn.Close()
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

func listenerIsNonLoopback(l net.Listener) bool {
	tcpAddr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return false
	}
	return !tcpAddr.IP.IsLoopback()
}

func warnIfUntrustedCapsBind(cfg listenerConfig, l net.Listener) {
	if cfg.trustsCaps() && listenerIsNonLoopback(l) {
		slog.Warn("capability-trusting listener bound to a non-loopback address; only enable trusted caps headers behind a trusted proxy that sets them", "address", l.Addr().String())
	}
}

func (cfg *listenerConfig) setTCPAddress(value string, rawInput string) error {
	if !strings.Contains(value, ":") {
		return fmt.Errorf("invalid --listen value %q: TCP listeners must specify host:port", rawInput)
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("invalid --listen value %q: %w", rawInput, err)
	}
	if port == "" {
		return fmt.Errorf("invalid --listen value %q: missing port", rawInput)
	}
	cfg.address = net.JoinHostPort(host, port)
	return nil
}

type listenerFlags []listenerConfig

func (l *listenerFlags) String() string {
	parts := make([]string, len(*l))
	for i, cfg := range *l {
		parts[i] = cfg.raw
	}
	return strings.Join(parts, ",")
}

type listenerSpec struct {
	Address              string `json:"address,omitempty"`
	Systemd              string `json:"systemd,omitempty"`
	TrustedCapsHeader    string `json:"trusted_caps_header,omitempty"`
	TrustedTailscaleCaps string `json:"trusted_tailscale_caps,omitempty"`
}

func (l *listenerFlags) UnmarshalJSON(data []byte) error {
	var entries []json.RawMessage
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	for _, entry := range entries {
		if bytes.HasPrefix(bytes.TrimSpace(entry), []byte("{")) {
			var spec listenerSpec
			dec := json.NewDecoder(bytes.NewReader(entry))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&spec); err != nil {
				return err
			}
			if spec.Address == "" && spec.Systemd == "" {
				return fmt.Errorf("invalid listen entry %s: one of address or systemd is required", entry)
			}
			if spec.Address != "" && spec.Systemd != "" {
				return fmt.Errorf("invalid listen entry %s: address and systemd are mutually exclusive", entry)
			}
			if spec.Systemd != "" {
				if spec.TrustedCapsHeader != "" && spec.TrustedTailscaleCaps != "" {
					return fmt.Errorf("invalid listen entry %s: only one of trusted_caps_header or trusted_tailscale_caps may be set", entry)
				}
				*l = append(*l, listenerConfig{
					kind:                 listenerTypeSystemd,
					address:              spec.Systemd,
					raw:                  "systemd:" + spec.Systemd,
					trustedCapsHeader:    spec.TrustedCapsHeader,
					trustedTailscaleCaps: spec.TrustedTailscaleCaps,
				})
				continue
			}
			if err := l.Set(spec.Address); err != nil {
				return err
			}
			if spec.TrustedCapsHeader != "" {
				(*l)[len(*l)-1].trustedCapsHeader = spec.TrustedCapsHeader
			}
			if spec.TrustedTailscaleCaps != "" {
				(*l)[len(*l)-1].trustedTailscaleCaps = spec.TrustedTailscaleCaps
			}
			if last := &(*l)[len(*l)-1]; last.trustedCapsHeader != "" && last.trustedTailscaleCaps != "" {
				return fmt.Errorf("invalid listen entry %s: only one of trusted_caps_header or trusted_tailscale_caps may be set", entry)
			}
			continue
		}
		var s string
		if err := json.Unmarshal(entry, &s); err != nil {
			return err
		}
		if err := l.Set(s); err != nil {
			return err
		}
	}
	return nil
}

func parseListenerQuery(query string) (trustedCapsHeader, trustedTailscaleCaps string, isCapsQuery bool, err error) {
	parts := strings.Split(query, "&")
	for _, part := range parts {
		if key, _, _ := strings.Cut(part, "="); strings.HasPrefix(key, "trusted-") {
			isCapsQuery = true
		}
	}
	if !isCapsQuery {
		return "", "", false, nil
	}
	for _, part := range parts {
		if part == "" {
			continue
		}
		key, val, hasVal := strings.Cut(part, "=")
		switch key {
		case "trusted-caps-header":
			if !hasVal || val == "" {
				return "", "", true, fmt.Errorf("--listen query %q requires a header name", key)
			}
			trustedCapsHeader = val
		case "trusted-ts-caps":
			if !hasVal || val == "" {
				return "", "", true, fmt.Errorf("--listen query %q requires a capability name", key)
			}
			trustedTailscaleCaps = val
		default:
			return "", "", true, fmt.Errorf("unknown --listen query parameter %q", key)
		}
	}
	if trustedCapsHeader != "" && trustedTailscaleCaps != "" {
		return "", "", true, fmt.Errorf("--listen may set only one of trusted-caps-header or trusted-ts-caps")
	}
	return trustedCapsHeader, trustedTailscaleCaps, true, nil
}

func (l *listenerFlags) Set(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("invalid --listen value %q: empty specification", value)
	}

	cfg := listenerConfig{raw: trimmed}
	spec := trimmed
	if i := strings.IndexByte(trimmed, '?'); i != -1 {
		trustedCapsHeader, trustedTailscaleCaps, isCapsQuery, err := parseListenerQuery(trimmed[i+1:])
		if err != nil {
			return err
		}
		if isCapsQuery {
			cfg.trustedCapsHeader = trustedCapsHeader
			cfg.trustedTailscaleCaps = trustedTailscaleCaps
			spec = trimmed[:i]
		}
	}
	if spec == "" {
		return fmt.Errorf("invalid --listen value %q: empty specification", value)
	}

	working := spec
	lower := strings.ToLower(working)

	switch {
	case strings.HasPrefix(lower, "unix://"):
		working = working[len("unix://"):]
		cfg.kind = listenerTypeUnix
	case strings.HasPrefix(lower, "unix:"):
		working = working[len("unix:"):]
		cfg.kind = listenerTypeUnix
	case strings.HasPrefix(lower, "tcp://"):
		working = working[len("tcp://"):]
		cfg.kind = listenerTypeTCP
	case strings.HasPrefix(lower, "tcp:"):
		working = working[len("tcp:"):]
		cfg.kind = listenerTypeTCP
	}

	if cfg.kind == listenerTypeUnix {
		if working == "" {
			return fmt.Errorf("invalid --listen value %q: missing Unix socket path", value)
		}
		cfg.address = working
		*l = append(*l, cfg)
		return nil
	}

	if cfg.kind == listenerTypeTCP {
		if err := cfg.setTCPAddress(working, value); err != nil {
			return err
		}
		*l = append(*l, cfg)
		return nil
	}

	if strings.Contains(working, "/") || strings.HasPrefix(working, "@") {
		cfg.kind = listenerTypeUnix
		cfg.address = working
		*l = append(*l, cfg)
		return nil
	}

	if !strings.Contains(working, ":") {
		cfg.kind = listenerTypeUnix
		cfg.address = working
		*l = append(*l, cfg)
		return nil
	}

	cfg.kind = listenerTypeTCP
	if err := cfg.setTCPAddress(working, value); err != nil {
		return err
	}
	*l = append(*l, cfg)
	return nil
}

func (cfg listenerConfig) description() string {
	switch cfg.kind {
	case listenerTypeUnix:
		return fmt.Sprintf("unix://%s", cfg.address)
	case listenerTypeTCP:
		return cfg.address
	case listenerTypeSystemd:
		if cfg.address != "" {
			return fmt.Sprintf("systemd:%s", cfg.address)
		}
		return "systemd"
	case listenerTypeStdio:
		return "stdio"
	default:
		return "unknown"
	}
}

func systemdListeners() []listenerConfig {
	defer func() {
		_ = os.Unsetenv("LISTEN_PID")
		_ = os.Unsetenv("LISTEN_FDS")
		_ = os.Unsetenv("LISTEN_FDNAMES")
	}()

	pid, err := strconv.Atoi(os.Getenv("LISTEN_PID"))
	if err != nil || pid != os.Getpid() {
		return nil
	}

	nfds, err := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if err != nil || nfds <= 0 {
		return nil
	}

	names := strings.Split(os.Getenv("LISTEN_FDNAMES"), ":")
	configs := make([]listenerConfig, 0, nfds)
	for offset := 0; offset < nfds; offset++ {
		fd := listenFdsStart + offset
		syscall.CloseOnExec(fd)

		name := "LISTEN_FD_" + strconv.Itoa(fd)
		if offset < len(names) && len(names[offset]) > 0 {
			name = names[offset]
		}

		file := os.NewFile(uintptr(fd), name)
		configs = append(configs, listenerConfig{
			kind:    listenerTypeSystemd,
			address: file.Name(),
			raw:     fmt.Sprintf("systemd:%s", file.Name()),
			file:    file,
		})
	}

	return configs
}

func resolveSystemdListeners(configured []listenerConfig) ([]listenerConfig, error) {
	policies := make(map[string]listenerConfig)
	bound := configured[:0]
	for _, l := range configured {
		if l.kind == listenerTypeSystemd && l.file == nil {
			if _, dup := policies[l.address]; dup {
				return nil, fmt.Errorf("duplicate systemd caps policy for %q", l.address)
			}
			policies[l.address] = l
			continue
		}
		bound = append(bound, l)
	}

	result := bound
	matched := make(map[string]bool)
	for _, sl := range systemdListeners() {
		if p, ok := policies[sl.address]; ok {
			sl.trustedCapsHeader = p.trustedCapsHeader
			sl.trustedTailscaleCaps = p.trustedTailscaleCaps
			matched[sl.address] = true
		}
		result = append(result, sl)
	}
	for name := range policies {
		if !matched[name] {
			return nil, fmt.Errorf("systemd caps policy %q has no matching inherited socket", name)
		}
	}
	return result, nil
}

func (cfg listenerConfig) Serve(ctx context.Context, handler http.Handler, shutdownTimeout time.Duration, monitor *idleMonitor) error {
	switch cfg.kind {
	case listenerTypeStdio:
		server := &http2.Server{}
		stdioConn := &StdioConn{
			stdin:  os.Stdin,
			stdout: os.Stdout,
		}
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-ctx.Done():
				_ = stdioConn.Close()
			case <-done:
			}
		}()
		server.ServeConn(stdioConn, &http2.ServeConnOpts{
			Context: ctx,
			Handler: handler,
		})
		return nil
	case listenerTypeUnix:
		if err := prepareUnixSocketPath(cfg.address); err != nil {
			return err
		}
		listener, err := net.Listen("unix", cfg.address)
		if err != nil {
			return fmt.Errorf("failed to create Unix socket listener: %w", err)
		}
		defer func() { _ = listener.Close() }()
		warnIfUntrustedCapsBind(cfg, listener)
		return serveListener(ctx, listener, handler, shutdownTimeout, monitor)
	case listenerTypeTCP:
		listener, err := net.Listen("tcp", cfg.address)
		if err != nil {
			return fmt.Errorf("failed to create TCP listener: %w", err)
		}
		defer func() { _ = listener.Close() }()
		warnIfUntrustedCapsBind(cfg, listener)
		return serveListener(ctx, listener, handler, shutdownTimeout, monitor)
	case listenerTypeSystemd:
		if cfg.file == nil {
			return fmt.Errorf("systemd listener %s unavailable", cfg.description())
		}
		listener, err := net.FileListener(cfg.file)
		if err != nil {
			return fmt.Errorf("systemd listener %s unavailable: %w", cfg.description(), err)
		}
		_ = cfg.file.Close()
		defer func() {
			_ = listener.Close()
		}()
		warnIfUntrustedCapsBind(cfg, listener)
		return serveListener(ctx, listener, handler, shutdownTimeout, monitor)
	default:
		return fmt.Errorf("unsupported listener type %s", cfg.kind)
	}
}

func serveAllListeners(ctx context.Context, cancel context.CancelFunc, listeners []listenerConfig, handler http.Handler, shutdownTimeout time.Duration, monitor *idleMonitor) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(listeners))

	for _, cfg := range listeners {
		cfg := cfg
		listenerHandler := handler
		if cfg.trustsCaps() {
			listenerHandler = enforceCaps(cfg.trustedCapsHeader, cfg.trustedTailscaleCaps, handler)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("listening", "address", cfg.description())
			if err := cfg.Serve(ctx, listenerHandler, shutdownTimeout, monitor); err != nil && ctx.Err() == nil {
				select {
				case errChan <- fmt.Errorf("listener %s error: %w", cfg.description(), err):
				default:
				}
				cancel()
			}
		}()
	}

	wg.Wait()
	close(errChan)

	if err := <-errChan; err != nil {
		return err
	}
	return nil
}

func serveListener(ctx context.Context, listener net.Listener, handler http.Handler, shutdownTimeout time.Duration, monitor *idleMonitor) error {
	server := &http.Server{
		Handler: handler,
	}

	if monitor != nil {
		server.ConnState = func(conn net.Conn, state http.ConnState) {
			switch state {
			case http.StateNew:
				monitor.Incr()
			case http.StateClosed, http.StateHijacked:
				monitor.Decr()
			}
		}
	}

	_ = http2.ConfigureServer(server, &http2.Server{})

	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("server shutdown error", "error", err)
		}
		return nil
	case err := <-errChan:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	}
}
