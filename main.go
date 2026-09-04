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

	"golang.org/x/net/http2"
)

var logger *slog.Logger

var version = "1.0.0"

const tailscaleDrainTimeout = 10 * time.Second

func serveStdio(ctx context.Context, handler http.Handler) error {
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
	setupAllRoutes(mux, connMgr, config.Repos, ParseAccess(config.Access), readPool, writePool)

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
	listeners, err := PrepareListeners(ctx, config.Listeners, newResticPolicyReducer, ListenOptions{
		ShutdownTimeout:       time.Duration(config.ShutdownTimeout),
		TailscaleDrainTimeout: tailscaleDrainTimeout,
		RuntimeName:           "restic-rados-server",
		ConnState:             connState,
		MetricsHandler:        newMetricsHandler(),
		MetricsPath:           metricsPath,
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
