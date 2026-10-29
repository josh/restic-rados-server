package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var logger *slog.Logger

var version = "1.0.2"

func serveStdio(ctx context.Context, handler http.Handler) error {
	listener := &stdioListener{conn: make(chan net.Conn, 1)}
	listener.conn <- &StdioConn{stdin: os.Stdin, stdout: os.Stdout}
	server := &http.Server{
		Handler:   handler,
		Protocols: new(http.Protocols),
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
	server.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			_ = server.Close()
		}
	}
	server.Protocols.SetUnencryptedHTTP2(true)
	stop := context.AfterFunc(ctx, func() { _ = server.Close() })
	defer stop()
	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

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

func main() {
	config, showVersion, err := loadConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	if showVersion {
		fmt.Println(version)
		os.Exit(0)
	}
	if err := validateResticListenerPolicies(config.Listeners); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err := initLogger(config.Verbose, config.LogFile); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	serverMetrics.recordBuildInfo(version)

	if len(config.Repos) == 0 {
		fmt.Fprintln(os.Stderr, "pool not set (use --pool, RESTIC_RADOS_SERVER_POOL, or config file)")
		os.Exit(1)
	}
	for name, repo := range config.Repos {
		if repo.BlobPools == nil {
			fmt.Fprintf(os.Stderr, "repo %q: no pools configured\n", name)
			os.Exit(1)
		}
		if repo.BlobPools.Config.Pool == "" {
			fmt.Fprintf(os.Stderr, "repo %q: config pool must be specified (use 'poolname' or 'poolname:config,...')\n", name)
			os.Exit(1)
		}
	}

	if config.Stdio && len(config.Listeners) > 0 {
		slog.Error("--stdio cannot be combined with --listen")
		os.Exit(1)
	}
	if config.Stdio && time.Duration(config.MaxIdleTime) > 0 {
		slog.Error("--max-idle-time is not supported in stdio mode")
		os.Exit(1)
	}

	cephConfig := CephConfig{
		KeyringPath:     config.Keyring,
		ClientID:        config.ClientID,
		CephConf:        config.CephConf,
		WriteBufferSize: config.WriteBufferSize,
		OSDOpTimeout:    time.Duration(config.IOTimeout),
	}

	connMgr := NewConnectionManager(cephConfig)
	defer connMgr.Shutdown()

	if err := connMgr.InitializeAllPoolConfigs(config.Repos); err != nil {
		slog.Error("failed to initialize pool configs", "error", err)
		os.Exit(1)
	}

	if config.Stdio && !connMgr.Ready() {
		slog.Error("failed to initialize pool configs", "error", errConnectionUnavailable)
		os.Exit(1)
	}

	readPool := NewBufferPool(config.ReadBufferSize)
	writePool := NewBufferPool(config.WriteBufferSize)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mux := http.NewServeMux()
	setupAllRoutes(mux, connMgr, config.Repos, ParseAccess(config.Access), readPool, writePool, time.Duration(config.IOTimeout), config.MaxUploads)

	var monitor *idleMonitor
	var connState func(net.Conn, http.ConnState)
	if time.Duration(config.MaxIdleTime) > 0 {
		monitor = newIdleMonitor(time.Duration(config.MaxIdleTime))
		defer monitor.Stop()
		connState = func(_ net.Conn, state http.ConnState) {
			switch state {
			case http.StateNew:
				monitor.Incr()
			case http.StateClosed, http.StateHijacked:
				monitor.Decr()
			}
		}
		go func() {
			select {
			case <-monitor.Done():
				cancel()
			case <-ctx.Done():
				monitor.Stop()
			}
		}()
	}

	metricsPath := ""
	if config.Metrics {
		metricsPath = defaultMetricsPath
	}
	listeners, err := PrepareListeners(config.Listeners, newResticPolicyReducer, ListenOptions{
		ShutdownTimeout: time.Duration(config.ShutdownTimeout),
		IOTimeout:       time.Duration(config.IOTimeout),
		ConnState:       connState,
		MetricsHandler:  newMetricsHandler(),
		MetricsPath:     metricsPath,
	})
	if err != nil {
		slog.Error("failed to prepare listeners", "error", err)
		os.Exit(1)
	}
	closeListeners := func() {
		if err := listeners.Close(); err != nil {
			slog.Error("failed to close listeners", "error", err)
		}
	}
	defer closeListeners()

	if config.Stdio && listeners.Len() > 0 {
		closeListeners()
		slog.Error("--stdio cannot be combined with inherited systemd listeners")
		os.Exit(1)
	}
	if config.Metrics && listeners.Len() == 0 {
		closeListeners()
		fmt.Fprintln(os.Stderr, "--metrics requires listeners")
		os.Exit(1)
	}
	if !config.Stdio && listeners.Len() == 0 {
		config.Stdio = true
	}
	if config.Stdio && time.Duration(config.MaxIdleTime) > 0 {
		closeListeners()
		slog.Error("--max-idle-time is not supported in stdio mode")
		os.Exit(1)
	}

	if config.Stdio {
		if !connMgr.Ready() {
			closeListeners()
			slog.Error("failed to initialize pool configs", "error", errConnectionUnavailable)
			os.Exit(1)
		}
		if err := serveStdio(ctx, mux); err != nil && ctx.Err() == nil {
			closeListeners()
			slog.Error("stdio server error", "error", err)
			os.Exit(1)
		}
		return
	}

	if err := listeners.Serve(ctx, mux); err != nil {
		closeListeners()
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
