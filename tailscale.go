package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/ipn"
	"tailscale.com/tailcfg"
)

func tailscaleUpstreamSocketPath(configured, service string) (string, error) {
	path := configured
	if path == "" {
		name := tailcfg.ServiceName(service).WithoutPrefix() + ".sock"
		if dir, _, _ := strings.Cut(os.Getenv("RUNTIME_DIRECTORY"), ":"); dir != "" {
			path = filepath.Join(dir, name)
		} else {
			base := os.Getenv("XDG_RUNTIME_DIR")
			if base == "" {
				base = os.TempDir()
			}
			dir := filepath.Join(base, "restic-rados-server")
			if err := prepareOwnedSocketDir(dir); err != nil {
				return "", err
			}
			path = filepath.Join(dir, name)
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("failed to resolve tailscale upstream socket path %q: %w", path, err)
	}
	return abs, nil
}

func prepareOwnedSocketDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("failed to create tailscale upstream socket directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("failed to stat tailscale upstream socket directory %q: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("refusing to use tailscale upstream socket directory %q: not a directory", dir)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && int(stat.Uid) != os.Getuid() {
		return fmt.Errorf("refusing to use tailscale upstream socket directory %q: not owned by the current user", dir)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("failed to restrict tailscale upstream socket directory %q: %w", dir, err)
	}
	return nil
}

func listenUpstreamSocket(path string) (net.Listener, error) {
	if err := prepareUnixSocketPath(path); err != nil {
		return nil, err
	}
	oldUmask := syscall.Umask(0o077)
	listener, err := net.Listen("unix", path)
	syscall.Umask(oldUmask)
	if err != nil {
		return nil, fmt.Errorf("failed to create tailscale upstream socket listener: %w", err)
	}
	return listener, nil
}

func setupTailscaleListeners(ctx context.Context, config *Config) (err error) {
	var serviceListeners []*listenerConfig
	for i := range config.Listeners {
		if config.Listeners[i].kind == listenerTypeTailscaleService {
			serviceListeners = append(serviceListeners, &config.Listeners[i])
		}
	}
	if len(serviceListeners) == 0 {
		return nil
	}

	lc := &local.Client{Socket: config.tailscaleSocket()}

	var advertised []string
	defer func() {
		if err == nil {
			return
		}
		drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, service := range advertised {
			if drainErr := setServiceAdvertised(drainCtx, lc, service, false); drainErr != nil {
				slog.Error("failed to drain tailscale service during startup rollback", "service", service, "error", drainErr)
			}
		}
	}()

	st, err := lc.StatusWithoutPeers(ctx)
	if err != nil {
		return fmt.Errorf("cannot reach tailscaled: %w (is tailscaled running?)", err)
	}
	if st.CurrentTailnet == nil {
		return fmt.Errorf("tailscale is not connected to a tailnet (backend state: %s)", st.BackendState)
	}
	if st.Self == nil || st.Self.Tags == nil || st.Self.Tags.Len() == 0 {
		slog.Warn("tailscale service hosts must be tagged nodes; the service will not receive traffic until this node is tagged")
	}
	magicDNSSuffix := st.CurrentTailnet.MagicDNSSuffix

	useTLS := config.tailscaleUseTLS()
	port := config.tailscalePort()

	if useTLS && st.Self != nil && !st.Self.HasCap(tailcfg.CapabilityHTTPS) {
		slog.Warn("tailnet HTTPS certificates are not enabled; TLS connections will fail until HTTPS is enabled for the tailnet or tailscale.https is set to false")
	}

	sc, err := lc.GetServeConfig(ctx)
	if err != nil {
		return fmt.Errorf("get tailscale serve config: %w", err)
	}
	if sc == nil {
		sc = new(ipn.ServeConfig)
	}

	for _, cfg := range serviceListeners {
		path, err := tailscaleUpstreamSocketPath(config.tailscaleUpstreamSocket(), cfg.address)
		if err != nil {
			return err
		}
		listener, err := listenUpstreamSocket(path)
		if err != nil {
			return err
		}
		cfg.listener = listener

		proxyURL, err := ipn.ExpandProxyTargetValue("unix:"+path, []string{"unix"}, "unix")
		if err != nil {
			return fmt.Errorf("invalid tailscale upstream socket %q: %w", path, err)
		}

		handler := &ipn.HTTPHandler{Proxy: proxyURL}
		if config.TailscaleCapability != "" {
			handler.AcceptAppCaps = []tailcfg.PeerCapability{tailcfg.PeerCapability(config.TailscaleCapability)}
		}
		delete(sc.Services, tailcfg.ServiceName(cfg.address))
		sc.SetWebHandler(handler, cfg.address, port, "/", useTLS, magicDNSSuffix)
	}

	if err := lc.SetServeConfig(ctx, sc); err != nil {
		if local.IsAccessDeniedError(err) {
			return fmt.Errorf("set tailscale serve config: %w (run as root, or grant operator access: tailscale set --operator=$USER)", err)
		}
		return fmt.Errorf("set tailscale serve config: %w", err)
	}

	applied, err := lc.GetServeConfig(ctx)
	if err != nil {
		return fmt.Errorf("verify tailscale serve config: %w", err)
	}

	for _, cfg := range serviceListeners {
		service := cfg.address
		var appliedService *ipn.ServiceConfig
		if applied != nil {
			appliedService = applied.Services[tailcfg.ServiceName(service)]
		}
		if appliedService == nil {
			return fmt.Errorf("tailscaled dropped the serve config for %s (Tailscale Services require tailscaled v1.86 or later)", service)
		}
		if config.TailscaleCapability != "" && !serviceAcceptsAppCap(appliedService, config.TailscaleCapability) {
			return fmt.Errorf("tailscaled dropped the app capability filter for %s (forwarding app capabilities requires a newer tailscaled)", service)
		}

		if err := setServiceAdvertised(ctx, lc, service, true); err != nil {
			return fmt.Errorf("advertise %s: %w", service, err)
		}
		advertised = append(advertised, service)

		scheme := "https"
		if !useTLS {
			scheme = "http"
		}
		slog.Info("advertising tailscale service",
			"service", service,
			"url", fmt.Sprintf("%s://%s.%s:%d/", scheme, tailcfg.ServiceName(service).WithoutPrefix(), magicDNSSuffix, port))

		var drainOnce sync.Once
		cfg.drain = func() {
			drainOnce.Do(func() {
				drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := setServiceAdvertised(drainCtx, lc, service, false); err != nil {
					slog.Error("failed to drain tailscale service", "service", service, "error", err)
				}
			})
		}
	}

	return nil
}

func serviceAcceptsAppCap(svc *ipn.ServiceConfig, capability string) bool {
	for _, web := range svc.Web {
		if web == nil {
			continue
		}
		for _, handler := range web.Handlers {
			if handler != nil && slices.Contains(handler.AcceptAppCaps, tailcfg.PeerCapability(capability)) {
				return true
			}
		}
	}
	return false
}

var advertiseServicesMu sync.Mutex

func setServiceAdvertised(ctx context.Context, lc *local.Client, service string, advertise bool) error {
	advertiseServicesMu.Lock()
	defer advertiseServicesMu.Unlock()

	prefs, err := lc.GetPrefs(ctx)
	if err != nil {
		return fmt.Errorf("get prefs: %w", err)
	}

	var updated []string
	if advertise {
		if slices.Contains(prefs.AdvertiseServices, service) {
			return nil
		}
		updated = append(slices.Clone(prefs.AdvertiseServices), service)
	} else {
		updated = slices.DeleteFunc(slices.Clone(prefs.AdvertiseServices), func(s string) bool { return s == service })
		if len(updated) == len(prefs.AdvertiseServices) {
			return nil
		}
	}

	_, err = lc.EditPrefs(ctx, &ipn.MaskedPrefs{
		AdvertiseServicesSet: true,
		Prefs:                ipn.Prefs{AdvertiseServices: updated},
	})
	return err
}
