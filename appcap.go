package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
)

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

type resticRequestPolicy struct {
	access       Access
	grant        capGrant
	requireGrant bool
}

type resticPolicyContextKey struct{}

func capGrantAccessForRepo(grant capGrant, repo string) Access {
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

func mergeGrantRule(grant capGrant, raw []byte) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return
	}
	mergeGrantObject(grant, obj)
}

func mergeGrantList(grant capGrant, raw []byte) {
	var rules []json.RawMessage
	if err := json.Unmarshal(raw, &rules); err != nil {
		return
	}
	for _, rule := range rules {
		mergeGrantRule(grant, rule)
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
	mergeGrantRule(grant, trimmed)
}

type resticListenerPolicyJSON struct {
	Access json.RawMessage `json:"access"`
}

func parseResticListenerPolicy(raw json.RawMessage) (Access, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return AccessReadWrite, nil
	}
	var policy *resticListenerPolicyJSON
	if err := decodeStrictJSON(raw, &policy); err != nil {
		return AccessNone, fmt.Errorf("invalid listener policy: %w", err)
	}
	if policy == nil {
		return AccessNone, fmt.Errorf("invalid listener policy: policy cannot be null")
	}
	if policy.Access == nil {
		return AccessReadWrite, nil
	}
	var accessValue *string
	if err := json.Unmarshal(policy.Access, &accessValue); err != nil {
		return AccessNone, fmt.Errorf("invalid listener policy: access must be a string")
	}
	if accessValue == nil {
		return AccessNone, fmt.Errorf("invalid listener policy: access cannot be null")
	}
	var access Access
	switch *accessValue {
	case "r":
		access = AccessRead
	case "ra":
		access = AccessReadAppend
	case "rw":
		access = AccessReadWrite
	default:
		return AccessNone, fmt.Errorf("invalid listener policy access %q (must be r, ra, or rw)", *accessValue)
	}
	return access, nil
}

func validateResticListenerPolicies(configs ListenerConfigs) error {
	for _, config := range configs {
		info := listenerInfo(config)
		if _, err := newResticPolicyReducer(info); err != nil {
			return fmt.Errorf("listener %s policy: %w", info.Endpoint, err)
		}
	}
	return nil
}

func newResticPolicyReducer(info ListenerInfo) (PolicyReducer, error) {
	configuredAccess, err := parseResticListenerPolicy(info.Policy)
	if err != nil {
		return nil, err
	}
	requireGrant := info.TrustedAppCapsHeader != "" || len(info.AcceptAppCaps) != 0
	return func(ctx context.Context, documents []PolicyDocument) (context.Context, error) {
		access := configuredAccess
		grant := capGrant{}
		requestRequiresGrant := requireGrant
		for _, document := range documents {
			switch document.Origin {
			case PolicyOriginConfigured:
				documentAccess, err := parseResticListenerPolicy(document.Value)
				if err != nil {
					return ctx, err
				}
				if documentAccess < access {
					access = documentAccess
				}
			case PolicyOriginTrustedHeader:
				requestRequiresGrant = true
				mergeGrantValue(grant, document.Value)
			case PolicyOriginTailscale:
				requestRequiresGrant = true
				mergeGrantRule(grant, document.Value)
			default:
				return ctx, fmt.Errorf("unsupported policy origin %d", document.Origin)
			}
		}
		policy := resticRequestPolicy{
			access:       access,
			grant:        grant,
			requireGrant: requestRequiresGrant,
		}
		return context.WithValue(ctx, resticPolicyContextKey{}, policy), nil
	}, nil
}

func resticPolicyAccessForRepo(ctx context.Context, repo string) Access {
	policy, ok := ctx.Value(resticPolicyContextKey{}).(resticRequestPolicy)
	if !ok {
		return AccessReadWrite
	}
	if !policy.requireGrant {
		return policy.access
	}
	grantAccess := capGrantAccessForRepo(policy.grant, repo)
	if grantAccess < policy.access {
		return grantAccess
	}
	return policy.access
}
