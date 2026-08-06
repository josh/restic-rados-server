package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"mime"
	"net/http"
	"strings"
)

const tailscaleCapHeader = "Tailscale-App-Capabilities"

type Access int

const (
	AccessNone       Access = 0
	AccessRead       Access = 1
	AccessReadAppend Access = 2
	AccessReadWrite  Access = 3
)

func ParseAccess(s string) Access {
	switch s {
	case "r", "read-only":
		return AccessRead
	case "ra", "read-append":
		return AccessReadAppend
	case "rw", "read-write":
		return AccessReadWrite
	default:
		return AccessNone
	}
}

type capGrant map[string]Access

type grantContextKey struct{}
type listenerAccessContextKey struct{}

func withGrant(ctx context.Context, grant capGrant) context.Context {
	return context.WithValue(ctx, grantContextKey{}, grant)
}

func withListenerAccess(ctx context.Context, access Access) context.Context {
	return context.WithValue(ctx, listenerAccessContextKey{}, access)
}

func listenerAccessForRequest(ctx context.Context) Access {
	access, ok := ctx.Value(listenerAccessContextKey{}).(Access)
	if !ok {
		return AccessReadWrite
	}
	return access
}

func grantForRepo(ctx context.Context, repo string) Access {
	grant, ok := ctx.Value(grantContextKey{}).(capGrant)
	if !ok {
		return AccessReadWrite
	}
	if access, ok := grant[repo]; ok {
		return access
	}
	var best *repoPattern
	var bestAccess Access
	for key, access := range grant {
		before, after, found := strings.Cut(key, "*")
		if !found {
			continue
		}
		p := repoPattern{key: key, prefix: before, suffix: after}
		if _, ok := p.match(repo); !ok {
			continue
		}
		if best == nil || compareRepoPatterns(p, *best) < 0 {
			best = &p
			bestAccess = access
		}
	}
	if best != nil {
		return bestAccess
	}
	return AccessNone
}

func mergeGrantAccess(grant capGrant, repo string, access Access) {
	current, ok := grant[repo]
	if !ok || access > current {
		grant[repo] = access
	}
}

func mergeGrantObject(grant capGrant, obj map[string]json.RawMessage) {
	for repo, raw := range obj {
		if strings.Count(repo, "*") > 1 {
			slog.Debug("ignoring capability key with multiple wildcards", "repo", repo)
			continue
		}
		var accessStr string
		if err := json.Unmarshal(raw, &accessStr); err != nil {
			slog.Debug("denying capability key with non-string access", "repo", repo)
			mergeGrantAccess(grant, repo, AccessNone)
			continue
		}
		access := ParseAccess(accessStr)
		if access == AccessNone && accessStr != "" && accessStr != "none" {
			slog.Debug("denying capability key with unrecognized access token", "repo", repo, "access", accessStr)
		}
		mergeGrantAccess(grant, repo, access)
	}
}

func mergeGrantList(grant capGrant, raw []byte) {
	var rules []json.RawMessage
	if err := json.Unmarshal(raw, &rules); err != nil {
		return
	}
	for _, rule := range rules {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(rule, &obj); err != nil {
			continue
		}
		mergeGrantObject(grant, obj)
	}
}

func mergeGrantValue(grant capGrant, raw []byte) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return
	}
	if trimmed[0] == '[' {
		mergeGrantList(grant, trimmed)
		return
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &obj); err != nil {
		return
	}
	mergeGrantObject(grant, obj)
}

func parseTailscaleCaps(grant capGrant, headerValue, capName string) {
	if capName == "" {
		return
	}
	decoded := headerValue
	if strings.HasPrefix(strings.TrimSpace(headerValue), "=?") {
		if dec, err := new(mime.WordDecoder).DecodeHeader(headerValue); err == nil {
			decoded = dec
		}
	}
	var caps map[string]json.RawMessage
	if err := json.Unmarshal([]byte(decoded), &caps); err != nil {
		return
	}
	capRaw, ok := caps[capName]
	if !ok {
		return
	}
	mergeGrantList(grant, capRaw)
}

func lastHeaderValue(h http.Header, name string) string {
	vals := h.Values(name)
	if len(vals) == 0 {
		return ""
	}
	return vals[len(vals)-1]
}

func enforceCaps(trustedCapsHeader, trustedTailscaleCaps string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		grant := capGrant{}
		if trustedCapsHeader != "" {
			if v := lastHeaderValue(r.Header, trustedCapsHeader); v != "" {
				mergeGrantValue(grant, []byte(v))
			}
		}
		if trustedTailscaleCaps != "" {
			if v := lastHeaderValue(r.Header, tailscaleCapHeader); v != "" {
				parseTailscaleCaps(grant, v, trustedTailscaleCaps)
			}
		}
		if trustedCapsHeader != "" {
			r.Header.Del(trustedCapsHeader)
		}
		r.Header.Del(tailscaleCapHeader)
		next.ServeHTTP(w, r.WithContext(withGrant(r.Context(), grant)))
	})
}

func enforceListenerAccess(access Access, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(withListenerAccess(r.Context(), access)))
	})
}
