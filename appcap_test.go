package main

import (
	"net/http"
	"testing"
)

func TestParseAccess(t *testing.T) {
	tests := []struct {
		input string
		want  Access
	}{
		{"r", AccessRead},
		{"ra", AccessReadAppend},
		{"rw", AccessReadWrite},
		{"read-only", AccessRead},
		{"read-append", AccessReadAppend},
		{"read-write", AccessReadWrite},
		{"", AccessNone},
		{"invalid", AccessNone},
		{"RW", AccessNone},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseAccess(tt.input)
			if got != tt.want {
				t.Errorf("ParseAccess(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseAppCapabilities(t *testing.T) {
	t.Run("valid JSON", func(t *testing.T) {
		caps, err := parseAppCapabilities(`{"cap.example.com":[{"default":"rw"}]}`)
		if err != nil {
			t.Fatal(err)
		}
		configs, ok := caps["cap.example.com"]
		if !ok {
			t.Fatal("expected capability key")
		}
		if len(configs) != 1 {
			t.Fatalf("expected 1 config, got %d", len(configs))
		}
		if configs[0]["default"] != "rw" {
			t.Errorf("expected default=rw, got %s", configs[0]["default"])
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		_, err := parseAppCapabilities(`not json`)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("RFC 2047 Q-encoding", func(t *testing.T) {
		caps, err := parseAppCapabilities(`=?utf-8?q?{"cap":[{"*":"r"}]}?=`)
		if err != nil {
			t.Fatal(err)
		}
		configs, ok := caps["cap"]
		if !ok {
			t.Fatal("expected capability key")
		}
		if configs[0]["*"] != "r" {
			t.Errorf("expected *=r, got %s", configs[0]["*"])
		}
	})

	t.Run("RFC 2047 B-encoding", func(t *testing.T) {
		caps, err := parseAppCapabilities(`=?utf-8?b?eyJjYXAiOlt7IioiOiJyIn1dfQ==?=`)
		if err != nil {
			t.Fatal(err)
		}
		configs, ok := caps["cap"]
		if !ok {
			t.Fatal("expected capability key")
		}
		if configs[0]["*"] != "r" {
			t.Errorf("expected *=r, got %s", configs[0]["*"])
		}
	})

	t.Run("plain text passthrough", func(t *testing.T) {
		caps, err := parseAppCapabilities(`{"cap":[{"repo":"ra"}]}`)
		if err != nil {
			t.Fatal(err)
		}
		if caps["cap"][0]["repo"] != "ra" {
			t.Errorf("expected repo=ra, got %s", caps["cap"][0]["repo"])
		}
	})
}

func TestMergePermissions(t *testing.T) {
	t.Run("repo key match", func(t *testing.T) {
		configs := []map[string]string{{"myrepo": "ra"}}
		got := mergePermissions(configs, "myrepo")
		if got != AccessReadAppend {
			t.Errorf("expected AccessReadAppend, got %d", got)
		}
	})

	t.Run("wildcard match", func(t *testing.T) {
		configs := []map[string]string{{"*": "r"}}
		got := mergePermissions(configs, "anyrepo")
		if got != AccessRead {
			t.Errorf("expected AccessRead, got %d", got)
		}
	})

	t.Run("repo key more permissive than wildcard", func(t *testing.T) {
		configs := []map[string]string{{"*": "ra", "foo": "rw"}}
		got := mergePermissions(configs, "foo")
		if got != AccessReadWrite {
			t.Errorf("expected AccessReadWrite, got %d", got)
		}
	})

	t.Run("wildcard more permissive than repo key", func(t *testing.T) {
		configs := []map[string]string{{"*": "rw", "foo": "r"}}
		got := mergePermissions(configs, "foo")
		if got != AccessReadWrite {
			t.Errorf("expected AccessReadWrite, got %d", got)
		}
	})

	t.Run("merge across configs takes most permissive", func(t *testing.T) {
		configs := []map[string]string{
			{"default": "ra"},
			{"default": "rw"},
		}
		got := mergePermissions(configs, "default")
		if got != AccessReadWrite {
			t.Errorf("expected AccessReadWrite, got %d", got)
		}
	})

	t.Run("no match returns AccessNone", func(t *testing.T) {
		configs := []map[string]string{{"other": "rw"}}
		got := mergePermissions(configs, "default")
		if got != AccessNone {
			t.Errorf("expected AccessNone, got %d", got)
		}
	})

	t.Run("empty configs returns AccessNone", func(t *testing.T) {
		got := mergePermissions(nil, "default")
		if got != AccessNone {
			t.Errorf("expected AccessNone, got %d", got)
		}
	})

	t.Run("invalid access values ignored", func(t *testing.T) {
		configs := []map[string]string{{"repo": "invalid", "*": "bad"}}
		got := mergePermissions(configs, "repo")
		if got != AccessNone {
			t.Errorf("expected AccessNone, got %d", got)
		}
	})

	t.Run("wildcard and repo across configs", func(t *testing.T) {
		configs := []map[string]string{
			{"*": "ra"},
			{"foo": "rw"},
		}
		got := mergePermissions(configs, "foo")
		if got != AccessReadWrite {
			t.Errorf("expected AccessReadWrite, got %d", got)
		}
	})

	t.Run("only wildcard no repo", func(t *testing.T) {
		configs := []map[string]string{
			{"*": "ra"},
			{"foo": "rw"},
		}
		got := mergePermissions(configs, "bar")
		if got != AccessReadAppend {
			t.Errorf("expected AccessReadAppend, got %d", got)
		}
	})
}

func TestCheckRepoAccess(t *testing.T) {
	t.Run("missing header", func(t *testing.T) {
		r, _ := http.NewRequest("GET", "/", nil)
		_, err := checkRepoAccess("cap", "repo", r)
		if err == nil {
			t.Fatal("expected error for missing header")
		}
	})

	t.Run("missing capability", func(t *testing.T) {
		r, _ := http.NewRequest("GET", "/", nil)
		r.Header.Set("Tailscale-Caps", `{"other.cap":[{"*":"rw"}]}`)
		_, err := checkRepoAccess("my.cap", "repo", r)
		if err == nil {
			t.Fatal("expected error for missing capability")
		}
	})

	t.Run("valid capability", func(t *testing.T) {
		r, _ := http.NewRequest("GET", "/", nil)
		r.Header.Set("Tailscale-Caps", `{"my.cap":[{"default":"ra","archive":"r"}]}`)

		access, err := checkRepoAccess("my.cap", "default", r)
		if err != nil {
			t.Fatal(err)
		}
		if access != AccessReadAppend {
			t.Errorf("expected AccessReadAppend for default, got %d", access)
		}

		access, err = checkRepoAccess("my.cap", "archive", r)
		if err != nil {
			t.Fatal(err)
		}
		if access != AccessRead {
			t.Errorf("expected AccessRead for archive, got %d", access)
		}
	})

	t.Run("no repo match returns AccessNone", func(t *testing.T) {
		r, _ := http.NewRequest("GET", "/", nil)
		r.Header.Set("Tailscale-Caps", `{"my.cap":[{"other":"rw"}]}`)
		access, err := checkRepoAccess("my.cap", "default", r)
		if err != nil {
			t.Fatal(err)
		}
		if access != AccessNone {
			t.Errorf("expected AccessNone, got %d", access)
		}
	})
}
