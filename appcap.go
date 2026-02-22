package main

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
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

func parseAppCapabilities(header string) (map[string][]map[string]string, error) {
	dec := new(mime.WordDecoder)
	decoded, err := dec.DecodeHeader(header)
	if err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}

	var caps map[string][]map[string]string
	if err := json.Unmarshal([]byte(decoded), &caps); err != nil {
		return nil, fmt.Errorf("unmarshal capabilities: %w", err)
	}
	return caps, nil
}

func mergePermissions(configs []map[string]string, repo string) Access {
	best := AccessNone
	for _, cfg := range configs {
		if v, ok := cfg[repo]; ok {
			if a := ParseAccess(v); a > best {
				best = a
			}
		}
		if v, ok := cfg["*"]; ok {
			if a := ParseAccess(v); a > best {
				best = a
			}
		}
	}
	return best
}

func checkRepoAccess(capName, repo string, r *http.Request) (Access, error) {
	header := r.Header.Get("Tailscale-Caps")
	if header == "" {
		return AccessNone, fmt.Errorf("missing Tailscale-Caps header")
	}

	caps, err := parseAppCapabilities(header)
	if err != nil {
		return AccessNone, err
	}

	configs, ok := caps[capName]
	if !ok {
		return AccessNone, fmt.Errorf("capability %q not found", capName)
	}

	return mergePermissions(configs, repo), nil
}
