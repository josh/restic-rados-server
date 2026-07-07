package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var logger *slog.Logger

var version = "0.7.0"

const tailscaleDrainTimeout = 10 * time.Second

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

	if err := initLogger(config.Verbose, config.LogFile); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

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

	cephConfig := CephConfig{
		KeyringPath: config.Keyring,
		ClientID:    config.ClientID,
		CephConf:    config.CephConf,
	}

	connMgr := NewConnectionManager(cephConfig)
	defer connMgr.Shutdown()

	if err := connMgr.InitializeAllPoolConfigs(config.Repos); err != nil {
		slog.Error("failed to initialize pool configs", "error", err)
		os.Exit(1)
	}

	maxWriteSize, err := connMgr.GetMaxWriteSize()
	if err != nil {
		slog.Warn("failed to get max write size for validation", "error", err)
	} else if config.WriteBufferSize > maxWriteSize {
		slog.Warn("write buffer size exceeds cluster max write size, writes may be chunked or fail",
			"write_buffer_size", config.WriteBufferSize,
			"cluster_max_write_size", maxWriteSize)
	}

	readPool := NewBufferPool(config.ReadBufferSize)
	writePool := NewBufferPool(config.WriteBufferSize)

	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	resolvedListeners, err := resolveSystemdListeners(config.Listeners)
	if err != nil {
		slog.Error("failed to resolve listeners", "error", err)
		os.Exit(1)
	}
	config.Listeners = resolvedListeners
	if config.Stdio && len(config.Listeners) > 0 {
		slog.Error("--stdio cannot be combined with --listen")
		os.Exit(1)
	}
	hasConfiguredListeners := len(config.Listeners) > 0

	if !config.Stdio && !hasConfiguredListeners {
		config.Stdio = true
	}

	mux := http.NewServeMux()
	setupAllRoutes(mux, connMgr, config.Repos, readPool, writePool)

	if config.Stdio && time.Duration(config.MaxIdleTime) > 0 {
		slog.Error("--max-idle-time is not supported in stdio mode")
		os.Exit(1)
	}

	var monitor *idleMonitor
	if time.Duration(config.MaxIdleTime) > 0 {
		monitor = newIdleMonitor(time.Duration(config.MaxIdleTime))
		defer monitor.Stop()
		go func() {
			select {
			case <-monitor.Done():
				cancel()
			case <-ctx.Done():
				monitor.Stop()
			}
		}()
	}

	if config.Stdio {
		for _, cfg := range config.Listeners {
			cfg.Close()
		}

		stdioCfg := listenerConfig{
			kind: listenerTypeStdio,
			raw:  "stdio",
		}
		if err := stdioCfg.Serve(ctx, mux, time.Duration(config.ShutdownTimeout), monitor); err != nil && ctx.Err() == nil {
			slog.Error("stdio server error", "error", err)
			os.Exit(1)
		}
	} else {
		tailscaleCount := 0
		for _, l := range config.Listeners {
			if l.kind == listenerTypeTailscaleService {
				tailscaleCount++
			}
		}
		if tailscaleCount > 1 && config.Tailscale != nil && config.Tailscale.UpstreamSocket != "" {
			slog.Error("tailscale.upstream_socket cannot be shared by multiple tailscale services")
			os.Exit(1)
		}

		var withdrawals []func(context.Context)
		var withdrawOnce sync.Once
		withdrawAll := func() {
			withdrawOnce.Do(func() {
				if len(withdrawals) == 0 {
					return
				}
				drainCtx, drainCancel := context.WithTimeout(context.Background(), tailscaleDrainTimeout)
				defer drainCancel()
				for _, withdraw := range withdrawals {
					withdraw(drainCtx)
				}
			})
		}

		for i := range config.Listeners {
			if config.Listeners[i].kind == listenerTypeTailscaleService {
				upstream, withdraw, err := setupTailscaleService(ctx, config.Listeners[i], config.Tailscale)
				if err != nil {
					withdrawAll()
					slog.Error("failed to set up tailscale service", "error", err)
					os.Exit(1)
				}
				config.Listeners[i] = upstream
				withdrawals = append(withdrawals, withdraw)
			}
		}
		if len(withdrawals) > 0 {
			serveCtx, serveCancel := context.WithCancel(context.Background())
			go func() {
				<-ctx.Done()
				withdrawAll()
				serveCancel()
			}()
			if err := serveAllListeners(serveCtx, serveCancel, config.Listeners, mux, time.Duration(config.ShutdownTimeout), monitor); err != nil {
				withdrawAll()
				slog.Error("server error", "error", err)
				os.Exit(1)
			}
			withdrawAll()
		} else if err := serveAllListeners(ctx, cancel, config.Listeners, mux, time.Duration(config.ShutdownTimeout), monitor); err != nil {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}
}
