package main

import (
	"context"
	"encoding/json"
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
	kind    listenerType
	address string
	raw     string
	file    *os.File
}

func (cfg listenerConfig) Close() {
	if cfg.file != nil {
		_ = cfg.file.Close()
	}

	if cfg.kind == listenerTypeUnix {
		if cfg.address != "" {
			_ = os.Remove(cfg.address)
		}
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

func (l *listenerFlags) UnmarshalJSON(data []byte) error {
	var ss []string
	if err := json.Unmarshal(data, &ss); err != nil {
		return err
	}
	for _, s := range ss {
		if err := l.Set(s); err != nil {
			return err
		}
	}
	return nil
}

func (l *listenerFlags) Set(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("invalid --listen value %q: empty specification", value)
	}

	cfg := listenerConfig{raw: trimmed}
	working := trimmed
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

func systemdListeners() ([]listenerConfig, error) {
	defer func() {
		_ = os.Unsetenv("LISTEN_PID")
		_ = os.Unsetenv("LISTEN_FDS")
		_ = os.Unsetenv("LISTEN_FDNAMES")
	}()

	pid, err := strconv.Atoi(os.Getenv("LISTEN_PID"))
	if err != nil || pid != os.Getpid() {
		return nil, nil
	}

	nfds, err := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if err != nil || nfds <= 0 {
		return nil, nil
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

	return configs, nil
}

func (cfg listenerConfig) Serve(ctx context.Context, handler http.Handler, shutdownTimeout time.Duration, monitor *idleMonitor) error {
	switch cfg.kind {
	case listenerTypeStdio:
		server := &http2.Server{}
		stdioConn := &StdioConn{
			stdin:  os.Stdin,
			stdout: os.Stdout,
		}
		server.ServeConn(stdioConn, &http2.ServeConnOpts{
			Context: ctx,
			Handler: handler,
		})
		return nil
	case listenerTypeUnix:
		if err := os.Remove(cfg.address); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove existing socket: %w", err)
		}
		listener, err := net.Listen("unix", cfg.address)
		if err != nil {
			return fmt.Errorf("failed to create Unix socket listener: %w", err)
		}
		defer func() {
			_ = listener.Close()
			if cfg.address != "" {
				_ = os.Remove(cfg.address)
			}
		}()
		return serveListener(ctx, listener, handler, shutdownTimeout, monitor)
	case listenerTypeTCP:
		listener, err := net.Listen("tcp", cfg.address)
		if err != nil {
			return fmt.Errorf("failed to create TCP listener: %w", err)
		}
		defer func() { _ = listener.Close() }()
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("listening", "address", cfg.description())
			if err := cfg.Serve(ctx, handler, shutdownTimeout, monitor); err != nil && ctx.Err() == nil {
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
		return ctx.Err()
	case err := <-errChan:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	}
}
