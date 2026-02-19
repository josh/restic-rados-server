package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigArgs(t *testing.T) {
	args := []string{
		"--verbose",
		"--listen", "tcp://0.0.0.0:8080",
		"--listen", "unix:///var/run/restic.sock",
		"--stdio",
		"--shutdown-timeout", "30s",
		"--append-only",
		"--max-idle-time", "5m",
		"--log-file", "/var/log/restic.log",
		"--keyring", "/etc/ceph/keyring",
		"--id", "restic",
		"--pool", "my-pool",
		"--ceph-conf", "/etc/ceph/ceph.conf",
		"--disable-striper",
		"--read-buffer-size", "1024",
		"--write-buffer-size", "2048",
		"--max-object-size", "4096",
	}

	config, showVersion, err := loadConfig(args)
	if err != nil {
		t.Fatal(err)
	}
	if showVersion {
		t.Error("expected showVersion false")
	}
	if !config.Verbose {
		t.Error("expected Verbose true")
	}
	if len(config.Listeners) != 2 {
		t.Errorf("expected 2 listeners, got %d", len(config.Listeners))
	}
	if !config.Stdio {
		t.Error("expected Stdio true")
	}
	if time.Duration(config.ShutdownTimeout) != 30*time.Second {
		t.Errorf("expected 30s shutdown timeout, got %s", config.ShutdownTimeout)
	}
	if !config.AppendOnly {
		t.Error("expected AppendOnly true")
	}
	if time.Duration(config.MaxIdleTime) != 5*time.Minute {
		t.Errorf("expected 5m max idle time, got %s", config.MaxIdleTime)
	}
	if config.LogFile != "/var/log/restic.log" {
		t.Errorf("expected log file /var/log/restic.log, got %s", config.LogFile)
	}
	if config.Keyring != "/etc/ceph/keyring" {
		t.Errorf("expected keyring /etc/ceph/keyring, got %s", config.Keyring)
	}
	if config.ClientID != "restic" {
		t.Errorf("expected client_id restic, got %s", config.ClientID)
	}
	if config.BlobPools == nil {
		t.Fatal("expected BlobPools to be set")
	}
	for _, field := range []struct{ name, val string }{
		{"Config", config.BlobPools.Config},
		{"Keys", config.BlobPools.Keys},
		{"Locks", config.BlobPools.Locks},
		{"Snapshots", config.BlobPools.Snapshots},
		{"Data", config.BlobPools.Data},
		{"Index", config.BlobPools.Index},
	} {
		if field.val != "my-pool" {
			t.Errorf("expected BlobPools.%s = my-pool, got %s", field.name, field.val)
		}
	}
	if config.CephConf != "/etc/ceph/ceph.conf" {
		t.Errorf("expected ceph_conf /etc/ceph/ceph.conf, got %s", config.CephConf)
	}
	if !config.DisableStriper {
		t.Error("expected DisableStriper true")
	}
	if config.ReadBufferSize != 1024 {
		t.Errorf("expected read_buffer_size 1024, got %d", config.ReadBufferSize)
	}
	if config.WriteBufferSize != 2048 {
		t.Errorf("expected write_buffer_size 2048, got %d", config.WriteBufferSize)
	}
	if config.MaxObjectSize != 4096 {
		t.Errorf("expected max_object_size 4096, got %d", config.MaxObjectSize)
	}
}

func TestLoadConfigArgsPartial(t *testing.T) {
	args := []string{"--verbose", "--id", "myid"}

	config, _, err := loadConfig(args)
	if err != nil {
		t.Fatal(err)
	}

	if !config.Verbose {
		t.Error("expected Verbose true")
	}
	if config.ClientID != "myid" {
		t.Errorf("expected ClientID myid, got %s", config.ClientID)
	}
	if config.ReadBufferSize != defaultReadBufferSize {
		t.Errorf("expected ReadBufferSize default %d, got %d", defaultReadBufferSize, config.ReadBufferSize)
	}
	if config.WriteBufferSize != defaultWriteBufferSize {
		t.Errorf("expected WriteBufferSize default %d, got %d", defaultWriteBufferSize, config.WriteBufferSize)
	}
	if time.Duration(config.ShutdownTimeout) != 60*time.Second {
		t.Errorf("expected default 60s shutdown timeout, got %s", config.ShutdownTimeout)
	}
}

func TestLoadConfigFile(t *testing.T) {
	json := `{
		"verbose": true,
		"listen": ["tcp://0.0.0.0:8080", "unix:///var/run/restic.sock"],
		"shutdown_timeout": "90s",
		"append_only": true,
		"max_idle_time": "5m",
		"log_file": "/var/log/restic.log",
		"keyring": "/etc/ceph/keyring",
		"client_id": "restic",
		"pools": ["my-pool"],
		"ceph_conf": "/etc/ceph/ceph.conf",
		"disable_striper": true,
		"read_buffer_size": 1024,
		"write_buffer_size": 2048,
		"max_object_size": 4096
	}`
	path := writeTemp(t, json)

	config, _, err := loadConfig([]string{"--config", path})
	if err != nil {
		t.Fatal(err)
	}

	if !config.Verbose {
		t.Error("expected Verbose true")
	}
	if len(config.Listeners) != 2 {
		t.Errorf("expected 2 listeners, got %d", len(config.Listeners))
	}
	if time.Duration(config.ShutdownTimeout) != 90*time.Second {
		t.Errorf("expected 90s shutdown timeout, got %s", config.ShutdownTimeout)
	}
	if !config.AppendOnly {
		t.Error("expected AppendOnly true")
	}
	if time.Duration(config.MaxIdleTime) != 5*time.Minute {
		t.Errorf("expected 5m max idle time, got %s", config.MaxIdleTime)
	}
	if config.LogFile != "/var/log/restic.log" {
		t.Errorf("expected log file /var/log/restic.log, got %s", config.LogFile)
	}
	if config.Keyring != "/etc/ceph/keyring" {
		t.Errorf("expected keyring /etc/ceph/keyring, got %s", config.Keyring)
	}
	if config.ClientID != "restic" {
		t.Errorf("expected client_id restic, got %s", config.ClientID)
	}
	if config.BlobPools == nil {
		t.Fatal("expected BlobPools to be set")
	}
	for _, field := range []struct{ name, val string }{
		{"Config", config.BlobPools.Config},
		{"Keys", config.BlobPools.Keys},
		{"Locks", config.BlobPools.Locks},
		{"Snapshots", config.BlobPools.Snapshots},
		{"Data", config.BlobPools.Data},
		{"Index", config.BlobPools.Index},
	} {
		if field.val != "my-pool" {
			t.Errorf("expected BlobPools.%s = my-pool, got %s", field.name, field.val)
		}
	}
	if config.CephConf != "/etc/ceph/ceph.conf" {
		t.Errorf("expected ceph_conf /etc/ceph/ceph.conf, got %s", config.CephConf)
	}
	if !config.DisableStriper {
		t.Error("expected DisableStriper true")
	}
	if config.ReadBufferSize != 1024 {
		t.Errorf("expected read_buffer_size 1024, got %d", config.ReadBufferSize)
	}
	if config.WriteBufferSize != 2048 {
		t.Errorf("expected write_buffer_size 2048, got %d", config.WriteBufferSize)
	}
	if config.MaxObjectSize != 4096 {
		t.Errorf("expected max_object_size 4096, got %d", config.MaxObjectSize)
	}
}

func TestLoadConfigFileUnknownField(t *testing.T) {
	json := `{"verbose": true, "unknown_field": "bad"}`
	path := writeTemp(t, json)

	_, _, err := loadConfig([]string{"--config", path})
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestLoadConfigEnv(t *testing.T) {
	envs := map[string]string{
		"CEPH_SERVER_VERBOSE":           "true",
		"CEPH_SERVER_APPEND_ONLY":       "1",
		"CEPH_SERVER_DISABLE_STRIPER":   "yes",
		"CEPH_SERVER_LOG_FILE":          "/var/log/test.log",
		"CEPH_KEYRING":                  "/etc/ceph/keyring",
		"CEPH_ID":                       "restic",
		"CEPH_POOL":                     "my-pool;other-pool:data",
		"CEPH_CONF":                     "/etc/ceph/ceph.conf",
		"CEPH_SERVER_READ_BUFFER_SIZE":  "1024",
		"CEPH_SERVER_WRITE_BUFFER_SIZE": "2048",
		"CEPH_SERVER_MAX_OBJECT_SIZE":   "4096",
	}
	for k, v := range envs {
		t.Setenv(k, v)
	}

	config, _, err := loadConfig([]string{})
	if err != nil {
		t.Fatal(err)
	}

	if !config.Verbose {
		t.Error("expected Verbose true")
	}
	if !config.AppendOnly {
		t.Error("expected AppendOnly true")
	}
	if !config.DisableStriper {
		t.Error("expected DisableStriper true")
	}
	if config.LogFile != "/var/log/test.log" {
		t.Errorf("expected LogFile /var/log/test.log, got %s", config.LogFile)
	}
	if config.Keyring != "/etc/ceph/keyring" {
		t.Errorf("expected Keyring /etc/ceph/keyring, got %s", config.Keyring)
	}
	if config.ClientID != "restic" {
		t.Errorf("expected ClientID restic, got %s", config.ClientID)
	}
	if config.BlobPools == nil {
		t.Fatal("expected BlobPools to be set")
	}
	if config.BlobPools.Data != "other-pool" {
		t.Errorf("expected BlobPools.Data = other-pool, got %s", config.BlobPools.Data)
	}
	for _, field := range []struct{ name, val string }{
		{"Config", config.BlobPools.Config},
		{"Keys", config.BlobPools.Keys},
		{"Locks", config.BlobPools.Locks},
		{"Snapshots", config.BlobPools.Snapshots},
		{"Index", config.BlobPools.Index},
	} {
		if field.val != "my-pool" {
			t.Errorf("expected BlobPools.%s = my-pool, got %s", field.name, field.val)
		}
	}
	if config.CephConf != "/etc/ceph/ceph.conf" {
		t.Errorf("expected CephConf /etc/ceph/ceph.conf, got %s", config.CephConf)
	}
	if config.ReadBufferSize != 1024 {
		t.Errorf("expected ReadBufferSize 1024, got %d", config.ReadBufferSize)
	}
	if config.WriteBufferSize != 2048 {
		t.Errorf("expected WriteBufferSize 2048, got %d", config.WriteBufferSize)
	}
	if config.MaxObjectSize != 4096 {
		t.Errorf("expected MaxObjectSize 4096, got %d", config.MaxObjectSize)
	}
}

func TestLoadConfigCLIOverridesFile(t *testing.T) {
	json := `{
		"verbose": false,
		"client_id": "from-file",
		"read_buffer_size": 1024,
		"write_buffer_size": 2048
	}`
	path := writeTemp(t, json)

	config, _, err := loadConfig([]string{
		"--config", path,
		"--verbose",
		"--id", "from-cli",
		"--read-buffer-size", "4096",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !config.Verbose {
		t.Error("expected Verbose true (CLI override)")
	}
	if config.ClientID != "from-cli" {
		t.Errorf("expected ClientID from-cli, got %s", config.ClientID)
	}
	if config.ReadBufferSize != 4096 {
		t.Errorf("expected ReadBufferSize 4096 (CLI override), got %d", config.ReadBufferSize)
	}
	if config.WriteBufferSize != 2048 {
		t.Errorf("expected WriteBufferSize 2048 (from file), got %d", config.WriteBufferSize)
	}
}

func TestLoadConfigCLIOverridesEnv(t *testing.T) {
	t.Setenv("CEPH_ID", "from-env")
	t.Setenv("CEPH_SERVER_READ_BUFFER_SIZE", "1024")

	config, _, err := loadConfig([]string{
		"--id", "from-cli",
		"--read-buffer-size", "4096",
	})
	if err != nil {
		t.Fatal(err)
	}

	if config.ClientID != "from-cli" {
		t.Errorf("expected ClientID from-cli, got %s", config.ClientID)
	}
	if config.ReadBufferSize != 4096 {
		t.Errorf("expected ReadBufferSize 4096 (CLI override), got %d", config.ReadBufferSize)
	}
}

func TestLoadConfigFileFromEnv(t *testing.T) {
	json := `{
		"verbose": true,
		"read_buffer_size": 1024,
		"write_buffer_size": 2048
	}`
	path := writeTemp(t, json)
	t.Setenv("CEPH_SERVER_CONFIG", path)

	config, _, err := loadConfig([]string{})
	if err != nil {
		t.Fatal(err)
	}

	if !config.Verbose {
		t.Error("expected Verbose true (from file via env)")
	}
	if config.ReadBufferSize != 1024 {
		t.Errorf("expected ReadBufferSize 1024, got %d", config.ReadBufferSize)
	}
	if config.WriteBufferSize != 2048 {
		t.Errorf("expected WriteBufferSize 2048, got %d", config.WriteBufferSize)
	}
}

func TestLoadConfigVersion(t *testing.T) {
	_, showVersion, err := loadConfig([]string{"--version"})
	if err != nil {
		t.Fatal(err)
	}
	if !showVersion {
		t.Error("expected showVersion true")
	}
}

func TestLoadConfigUnknownFlag(t *testing.T) {
	_, _, err := loadConfig([]string{"--unknown-flag"})
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
}

func TestLoadConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"negative read buffer", []string{"--read-buffer-size", "-1"}},
		{"zero read buffer", []string{"--read-buffer-size", "0"}},
		{"negative write buffer", []string{"--write-buffer-size", "-1"}},
		{"zero write buffer", []string{"--write-buffer-size", "0"}},
		{"negative max object size", []string{"--max-object-size", "-1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := loadConfig(tt.args)
			if err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestLoadConfigPoolClearsBlobPools(t *testing.T) {
	json := `{
		"blob_pools": {
			"config": "meta-pool",
			"keys": "meta-pool",
			"locks": "meta-pool",
			"snapshots": "meta-pool",
			"data": "data-pool",
			"index": "data-pool"
		},
		"read_buffer_size": 1024,
		"write_buffer_size": 2048
	}`
	path := writeTemp(t, json)

	config, _, err := loadConfig([]string{"--config", path, "--pool", "new-pool"})
	if err != nil {
		t.Fatal(err)
	}

	if config.BlobPools == nil {
		t.Fatal("expected BlobPools to be set")
	}
	for _, field := range []struct{ name, val string }{
		{"Config", config.BlobPools.Config},
		{"Keys", config.BlobPools.Keys},
		{"Locks", config.BlobPools.Locks},
		{"Snapshots", config.BlobPools.Snapshots},
		{"Data", config.BlobPools.Data},
		{"Index", config.BlobPools.Index},
	} {
		if field.val != "new-pool" {
			t.Errorf("expected BlobPools.%s = new-pool, got %s", field.name, field.val)
		}
	}
}

func TestLoadConfigPoolErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"unknown blob type", []string{"--pool", "my-pool:badtype"}},
		{"duplicate type across pools", []string{"--pool", "pool1:data;pool2:data"}},
		{"multiple catch-all pools", []string{"--pool", "pool1;pool2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := loadConfig(tt.args)
			if err == nil {
				t.Fatal("expected pool parsing error")
			}
		})
	}
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
