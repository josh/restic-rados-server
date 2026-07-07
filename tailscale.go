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

	"tailscale.com/client/local"
	"tailscale.com/ipn"
	"tailscale.com/tailcfg"
)

var tailscaleMu sync.Mutex

const maxUnixSocketPathLen = 107

func tailscaleUpstreamBaseDir() string {
	if d := os.Getenv("RUNTIME_DIRECTORY"); d != "" {
		return d
	}
	if d := os.Getenv("XDG_RUNTIME_DIR"); d != "" {
		return d
	}
	return os.TempDir()
}

func prepareTailscaleUpstreamPath(tsCfg *TailscaleConfig, service string) (string, error) {
	if tsCfg != nil && tsCfg.UpstreamSocket != "" {
		if len(tsCfg.UpstreamSocket) > maxUnixSocketPathLen {
			return "", fmt.Errorf("upstream_socket path too long (%d > %d bytes): %s", len(tsCfg.UpstreamSocket), maxUnixSocketPathLen, tsCfg.UpstreamSocket)
		}
		return tsCfg.UpstreamSocket, nil
	}
	dir := filepath.Join(tailscaleUpstreamBaseDir(), "restic-rados-server")
	name := strings.NewReplacer("/", "-", " ", "-", ":", "-").Replace(strings.TrimPrefix(service, "svc:"))
	path := filepath.Join(dir, name+".sock")
	if len(path) > maxUnixSocketPathLen {
		return "", fmt.Errorf("derived upstream socket path too long (%d > %d bytes): %s", len(path), maxUnixSocketPathLen, path)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", err
	}
	return path, nil
}

func removeTailscaleServeHandler(ctx context.Context, lc *local.Client, service string) {
	tailscaleMu.Lock()
	defer tailscaleMu.Unlock()

	sc, err := lc.GetServeConfig(ctx)
	if err != nil || sc == nil {
		return
	}
	if _, ok := sc.Services[tailcfg.ServiceName(service)]; !ok {
		return
	}
	delete(sc.Services, tailcfg.ServiceName(service))
	if err := lc.SetServeConfig(ctx, sc); err != nil {
		slog.Error("failed to remove tailscale serve config", "service", service, "error", err)
	}
}

func setServiceAdvertised(ctx context.Context, lc *local.Client, service string, advertise bool) error {
	tailscaleMu.Lock()
	defer tailscaleMu.Unlock()

	prefs, err := lc.GetPrefs(ctx)
	if err != nil {
		return err
	}
	has := slices.Contains(prefs.AdvertiseServices, service)
	var updated []string
	switch {
	case advertise && has:
		return nil
	case advertise:
		updated = append(append(updated, prefs.AdvertiseServices...), service)
	case !advertise && !has:
		return nil
	default:
		for _, s := range prefs.AdvertiseServices {
			if s != service {
				updated = append(updated, s)
			}
		}
	}
	_, err = lc.EditPrefs(ctx, &ipn.MaskedPrefs{
		AdvertiseServicesSet: true,
		Prefs: ipn.Prefs{
			AdvertiseServices: updated,
		},
	})
	return err
}

func setupTailscaleService(ctx context.Context, cfg listenerConfig, tsCfg *TailscaleConfig) (listenerConfig, func(context.Context), error) {
	service := cfg.serviceName
	appCapability := cfg.trustedTailscaleCaps

	upstream, err := prepareTailscaleUpstreamPath(tsCfg, service)
	if err != nil {
		return listenerConfig{}, nil, fmt.Errorf("tailscale %s: %w", service, err)
	}

	lc := &local.Client{}
	if tsCfg != nil && tsCfg.Socket != "" {
		lc.Socket = tsCfg.Socket
	}

	st, err := lc.StatusWithoutPeers(ctx)
	if err != nil {
		return listenerConfig{}, nil, fmt.Errorf("tailscale %s: cannot reach tailscaled: %w", service, err)
	}
	if st.CurrentTailnet == nil {
		return listenerConfig{}, nil, fmt.Errorf("tailscale %s: node is not connected to a tailnet", service)
	}
	mds := st.CurrentTailnet.MagicDNSSuffix

	useTLS := tsCfg == nil || tsCfg.HTTPS == nil || *tsCfg.HTTPS
	port := 80
	if useTLS {
		port = 443
	}
	if tsCfg != nil && tsCfg.Port != 0 {
		port = tsCfg.Port
	}

	if err := prepareUnixSocketPath(upstream); err != nil {
		return listenerConfig{}, nil, fmt.Errorf("tailscale %s: %w", service, err)
	}
	listener, err := net.Listen("unix", upstream)
	if err != nil {
		return listenerConfig{}, nil, fmt.Errorf("tailscale %s: bind upstream socket: %w", service, err)
	}

	proxyValue, err := ipn.ExpandProxyTargetValue("unix:"+upstream, []string{"unix"}, "http")
	if err != nil {
		_ = listener.Close()
		return listenerConfig{}, nil, fmt.Errorf("tailscale %s: %w", service, err)
	}

	handler := &ipn.HTTPHandler{Proxy: proxyValue}
	if appCapability != "" {
		handler.AcceptAppCaps = []tailcfg.PeerCapability{tailcfg.PeerCapability(appCapability)}
	}

	sc, err := lc.GetServeConfig(ctx)
	if err != nil {
		_ = listener.Close()
		return listenerConfig{}, nil, fmt.Errorf("tailscale %s: get serve config: %w", service, err)
	}
	if sc == nil {
		sc = new(ipn.ServeConfig)
	}
	sc.SetWebHandler(handler, service, uint16(port), "/", useTLS, mds)
	if err := lc.SetServeConfig(ctx, sc); err != nil {
		_ = listener.Close()
		return listenerConfig{}, nil, fmt.Errorf("tailscale %s: set serve config: %w", service, err)
	}

	if err := setServiceAdvertised(ctx, lc, service, true); err != nil {
		removeTailscaleServeHandler(ctx, lc, service)
		_ = listener.Close()
		return listenerConfig{}, nil, fmt.Errorf("tailscale %s: advertise service: %w", service, err)
	}

	slog.Info("advertising tailscale service", "service", service, "port", port, "upstream", upstream)

	withdraw := func(drainCtx context.Context) {
		if err := setServiceAdvertised(drainCtx, lc, service, false); err != nil {
			slog.Error("failed to withdraw tailscale service", "service", service, "error", err)
		}
		removeTailscaleServeHandler(drainCtx, lc, service)
	}

	return listenerConfig{
		kind:                 listenerTypeUnix,
		address:              upstream,
		raw:                  cfg.raw,
		trustedTailscaleCaps: appCapability,
		bound:                listener,
	}, withdraw, nil
}
