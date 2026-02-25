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
		"--access", "ra",
		"--max-idle-time", "5m",
		"--log-file", "/var/log/restic.log",
		"--keyring", "/etc/ceph/keyring",
		"--id", "restic",
		"--pool", "my-pool",
		"--ceph-conf", "/etc/ceph/ceph.conf",
		"--striper=false",
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
	if config.CephConf != "/etc/ceph/ceph.conf" {
		t.Errorf("expected ceph_conf /etc/ceph/ceph.conf, got %s", config.CephConf)
	}
	if config.ReadBufferSize != 1024 {
		t.Errorf("expected read_buffer_size 1024, got %d", config.ReadBufferSize)
	}
	if config.WriteBufferSize != 2048 {
		t.Errorf("expected write_buffer_size 2048, got %d", config.WriteBufferSize)
	}

	def := config.Repos["default"]
	if def == nil {
		t.Fatal("expected Repos[\"default\"] to be set")
	}
	if def.Access != "ra" {
		t.Errorf("expected Access ra, got %s", def.Access)
	}
	if def.Striper == nil || *def.Striper != false {
		t.Error("expected Striper false")
	}
	if def.MaxObjectSize != 4096 {
		t.Errorf("expected max_object_size 4096, got %d", def.MaxObjectSize)
	}
	if def.BlobPools == nil {
		t.Fatal("expected BlobPools to be set")
	}
	for _, field := range []struct{ name, val string }{
		{"Config", def.BlobPools.Config.Pool},
		{"Keys", def.BlobPools.Keys.Pool},
		{"Locks", def.BlobPools.Locks.Pool},
		{"Snapshots", def.BlobPools.Snapshots.Pool},
		{"Data", def.BlobPools.Data.Pool},
		{"Index", def.BlobPools.Index.Pool},
	} {
		if field.val != "my-pool" {
			t.Errorf("expected BlobPools.%s = my-pool, got %s", field.name, field.val)
		}
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
		"max_idle_time": "5m",
		"log_file": "/var/log/restic.log",
		"keyring": "/etc/ceph/keyring",
		"client_id": "restic",
		"ceph_conf": "/etc/ceph/ceph.conf",
		"read_buffer_size": 1024,
		"write_buffer_size": 2048,
		"repos": {
			"default": {
				"pools": ["my-pool"],
				"access": "ra",
				"striper": false,
				"max_object_size": 4096
			}
		}
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
	if config.CephConf != "/etc/ceph/ceph.conf" {
		t.Errorf("expected ceph_conf /etc/ceph/ceph.conf, got %s", config.CephConf)
	}
	if config.ReadBufferSize != 1024 {
		t.Errorf("expected read_buffer_size 1024, got %d", config.ReadBufferSize)
	}
	if config.WriteBufferSize != 2048 {
		t.Errorf("expected write_buffer_size 2048, got %d", config.WriteBufferSize)
	}

	def := config.Repos["default"]
	if def == nil {
		t.Fatal("expected Repos[\"default\"] to be set")
	}
	if def.Access != "ra" {
		t.Errorf("expected Access ra, got %s", def.Access)
	}
	if def.Striper == nil || *def.Striper != false {
		t.Error("expected Striper false")
	}
	if def.MaxObjectSize != 4096 {
		t.Errorf("expected max_object_size 4096, got %d", def.MaxObjectSize)
	}
	if def.BlobPools == nil {
		t.Fatal("expected BlobPools to be set")
	}
	for _, field := range []struct{ name, val string }{
		{"Config", def.BlobPools.Config.Pool},
		{"Keys", def.BlobPools.Keys.Pool},
		{"Locks", def.BlobPools.Locks.Pool},
		{"Snapshots", def.BlobPools.Snapshots.Pool},
		{"Data", def.BlobPools.Data.Pool},
		{"Index", def.BlobPools.Index.Pool},
	} {
		if field.val != "my-pool" {
			t.Errorf("expected BlobPools.%s = my-pool, got %s", field.name, field.val)
		}
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

func TestLoadConfigFlatFieldsRejected(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"pools at top level", `{"pools": ["my-pool"]}`},
		{"access at top level", `{"access": "ra"}`},
		{"striper at top level", `{"striper": false}`},
		{"max_object_size at top level", `{"max_object_size": 4096}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTemp(t, tt.json)
			_, _, err := loadConfig([]string{"--config", path})
			if err == nil {
				t.Fatal("expected error for flat per-repo field at top level")
			}
		})
	}
}

func TestLoadConfigEnv(t *testing.T) {
	envs := map[string]string{
		"RESTIC_RADOS_SERVER_VERBOSE":           "true",
		"RESTIC_RADOS_SERVER_ACCESS":            "ra",
		"RESTIC_RADOS_SERVER_STRIPER":           "no",
		"RESTIC_RADOS_SERVER_LOG_FILE":          "/var/log/test.log",
		"CEPH_KEYRING":                          "/etc/ceph/keyring",
		"CEPH_ID":                               "restic",
		"RESTIC_RADOS_SERVER_POOL":              "my-pool;other-pool:data",
		"CEPH_CONF":                             "/etc/ceph/ceph.conf",
		"RESTIC_RADOS_SERVER_READ_BUFFER_SIZE":  "1024",
		"RESTIC_RADOS_SERVER_WRITE_BUFFER_SIZE": "2048",
		"RESTIC_RADOS_SERVER_MAX_OBJECT_SIZE":   "4096",
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
	if config.LogFile != "/var/log/test.log" {
		t.Errorf("expected LogFile /var/log/test.log, got %s", config.LogFile)
	}
	if config.Keyring != "/etc/ceph/keyring" {
		t.Errorf("expected Keyring /etc/ceph/keyring, got %s", config.Keyring)
	}
	if config.ClientID != "restic" {
		t.Errorf("expected ClientID restic, got %s", config.ClientID)
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

	def := config.Repos["default"]
	if def == nil {
		t.Fatal("expected Repos[\"default\"] to be set")
	}
	if def.Access != "ra" {
		t.Errorf("expected Access ra, got %s", def.Access)
	}
	if def.Striper == nil || *def.Striper != false {
		t.Error("expected Striper false")
	}
	if def.MaxObjectSize != 4096 {
		t.Errorf("expected MaxObjectSize 4096, got %d", def.MaxObjectSize)
	}
	if def.BlobPools == nil {
		t.Fatal("expected BlobPools to be set")
	}
	if def.BlobPools.Data.Pool != "other-pool" {
		t.Errorf("expected BlobPools.Data = other-pool, got %s", def.BlobPools.Data.Pool)
	}
	for _, field := range []struct{ name, val string }{
		{"Config", def.BlobPools.Config.Pool},
		{"Keys", def.BlobPools.Keys.Pool},
		{"Locks", def.BlobPools.Locks.Pool},
		{"Snapshots", def.BlobPools.Snapshots.Pool},
		{"Index", def.BlobPools.Index.Pool},
	} {
		if field.val != "my-pool" {
			t.Errorf("expected BlobPools.%s = my-pool, got %s", field.name, field.val)
		}
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
	t.Setenv("RESTIC_RADOS_SERVER_READ_BUFFER_SIZE", "1024")

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
	t.Setenv("RESTIC_RADOS_SERVER_CONFIG", path)

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

func TestLoadConfigEnvPrefixFallback(t *testing.T) {
	t.Run("CEPH_RESTIC_SERVER_ fallback", func(t *testing.T) {
		t.Setenv("CEPH_RESTIC_SERVER_VERBOSE", "true")
		t.Setenv("CEPH_RESTIC_SERVER_POOL", "fallback-pool")

		config, _, err := loadConfig([]string{})
		if err != nil {
			t.Fatal(err)
		}
		if !config.Verbose {
			t.Error("expected Verbose true via CEPH_RESTIC_SERVER_ fallback")
		}
		def := config.Repos["default"]
		if def == nil || def.BlobPools == nil {
			t.Fatal("expected pool to be set via CEPH_RESTIC_SERVER_ fallback")
		}
		if def.BlobPools.Config.Pool != "fallback-pool" {
			t.Errorf("expected pool fallback-pool, got %s", def.BlobPools.Config.Pool)
		}
	})

	t.Run("RADOS_RESTIC_SERVER_ fallback", func(t *testing.T) {
		t.Setenv("RADOS_RESTIC_SERVER_POOL", "rados-pool")

		config, _, err := loadConfig([]string{})
		if err != nil {
			t.Fatal(err)
		}
		def := config.Repos["default"]
		if def == nil || def.BlobPools == nil {
			t.Fatal("expected pool to be set via RADOS_RESTIC_SERVER_ fallback")
		}
		if def.BlobPools.Config.Pool != "rados-pool" {
			t.Errorf("expected pool rados-pool, got %s", def.BlobPools.Config.Pool)
		}
	})

	t.Run("higher priority prefix wins", func(t *testing.T) {
		t.Setenv("RESTIC_RADOS_SERVER_POOL", "primary-pool")
		t.Setenv("CEPH_RESTIC_SERVER_POOL", "fallback-pool")
		t.Setenv("RADOS_RESTIC_SERVER_POOL", "last-pool")

		config, _, err := loadConfig([]string{})
		if err != nil {
			t.Fatal(err)
		}
		def := config.Repos["default"]
		if def == nil || def.BlobPools == nil {
			t.Fatal("expected pool to be set")
		}
		if def.BlobPools.Config.Pool != "primary-pool" {
			t.Errorf("expected primary-pool (highest priority), got %s", def.BlobPools.Config.Pool)
		}
	})
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
		"repos": {
			"default": {
				"blob_pools": {
					"config": {"pool": "meta-pool"},
					"keys": {"pool": "meta-pool"},
					"locks": {"pool": "meta-pool"},
					"snapshots": {"pool": "meta-pool"},
					"data": {"pool": "data-pool"},
					"index": {"pool": "data-pool"}
				}
			}
		},
		"read_buffer_size": 1024,
		"write_buffer_size": 2048
	}`
	path := writeTemp(t, json)

	config, _, err := loadConfig([]string{"--config", path, "--pool", "new-pool"})
	if err != nil {
		t.Fatal(err)
	}

	def := config.Repos["default"]
	if def == nil {
		t.Fatal("expected Repos[\"default\"] to be set")
	}
	if def.BlobPools == nil {
		t.Fatal("expected BlobPools to be set")
	}
	for _, field := range []struct{ name, val string }{
		{"Config", def.BlobPools.Config.Pool},
		{"Keys", def.BlobPools.Keys.Pool},
		{"Locks", def.BlobPools.Locks.Pool},
		{"Snapshots", def.BlobPools.Snapshots.Pool},
		{"Data", def.BlobPools.Data.Pool},
		{"Index", def.BlobPools.Index.Pool},
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

func TestLoadConfigMultiRepo(t *testing.T) {
	json := `{
		"repos": {
			"default": {
				"pools": ["meta-pool"],
				"access": "ra"
			},
			"offsite": {
				"pools": ["offsite-data:data,snapshots", "offsite-meta:*"],
				"striper": false,
				"max_object_size": 8192
			}
		}
	}`
	path := writeTemp(t, json)

	config, _, err := loadConfig([]string{"--config", path})
	if err != nil {
		t.Fatal(err)
	}

	if len(config.Repos) != 2 {
		t.Fatalf("expected 2 repos, got %d", len(config.Repos))
	}

	def := config.Repos["default"]
	if def == nil {
		t.Fatal("expected Repos[\"default\"] to be set")
	}
	if def.Access != "ra" {
		t.Errorf("expected default repo Access ra, got %s", def.Access)
	}
	if def.BlobPools == nil {
		t.Fatal("expected default BlobPools to be set")
	}
	if def.BlobPools.Config.Pool != "meta-pool" {
		t.Errorf("expected default Config pool = meta-pool, got %s", def.BlobPools.Config.Pool)
	}

	offsite := config.Repos["offsite"]
	if offsite == nil {
		t.Fatal("expected Repos[\"offsite\"] to be set")
	}
	if offsite.Access != "rw" {
		t.Errorf("expected offsite repo Access rw, got %s", offsite.Access)
	}
	if offsite.Striper == nil || *offsite.Striper != false {
		t.Error("expected offsite repo Striper false")
	}
	if offsite.MaxObjectSize != 8192 {
		t.Errorf("expected offsite MaxObjectSize 8192, got %d", offsite.MaxObjectSize)
	}
	if offsite.BlobPools == nil {
		t.Fatal("expected offsite BlobPools to be set")
	}
	if offsite.BlobPools.Data.Pool != "offsite-data" {
		t.Errorf("expected offsite Data pool = offsite-data, got %s", offsite.BlobPools.Data.Pool)
	}
	if offsite.BlobPools.Config.Pool != "offsite-meta" {
		t.Errorf("expected offsite Config pool = offsite-meta, got %s", offsite.BlobPools.Config.Pool)
	}
}

func TestLoadConfigReservedRepoName(t *testing.T) {
	reserved := []string{"keys", "locks", "snapshots", "data", "index", "config"}
	for _, name := range reserved {
		t.Run(name, func(t *testing.T) {
			json := `{"repos": {"` + name + `": {"pools": ["my-pool"]}}}`
			path := writeTemp(t, json)
			_, _, err := loadConfig([]string{"--config", path})
			if err == nil {
				t.Fatalf("expected error for reserved repo name %q", name)
			}
		})
	}
}

func TestLoadConfigMultiRepoPoolParsing(t *testing.T) {
	json := `{
		"repos": {
			"default": {
				"pools": ["data-pool:data,snapshots", "meta-pool:*"]
			}
		}
	}`
	path := writeTemp(t, json)

	config, _, err := loadConfig([]string{"--config", path})
	if err != nil {
		t.Fatal(err)
	}

	def := config.Repos["default"]
	if def == nil || def.BlobPools == nil {
		t.Fatal("expected default repo BlobPools")
	}
	if def.BlobPools.Data.Pool != "data-pool" {
		t.Errorf("expected Data = data-pool, got %s", def.BlobPools.Data.Pool)
	}
	if def.BlobPools.Snapshots.Pool != "data-pool" {
		t.Errorf("expected Snapshots = data-pool, got %s", def.BlobPools.Snapshots.Pool)
	}
	if def.BlobPools.Config.Pool != "meta-pool" {
		t.Errorf("expected Config = meta-pool, got %s", def.BlobPools.Config.Pool)
	}
	if def.BlobPools.Keys.Pool != "meta-pool" {
		t.Errorf("expected Keys = meta-pool, got %s", def.BlobPools.Keys.Pool)
	}
}

func TestLoadConfigMultiRepoValidation(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{
			"negative max-object-size per repo",
			`{"repos": {"default": {"pools": ["my-pool"], "max_object_size": -1}}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTemp(t, tt.json)
			_, _, err := loadConfig([]string{"--config", path})
			if err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestLoadConfigPoolSpecNamespace(t *testing.T) {
	config, _, err := loadConfig([]string{
		"--pool", "my-pool/metadata:keys,locks,config",
		"--pool", "my-pool/data:*",
	})
	if err != nil {
		t.Fatal(err)
	}

	def := config.Repos["default"]
	if def == nil || def.BlobPools == nil {
		t.Fatal("expected default repo BlobPools")
	}
	if def.BlobPools.Config.Pool != "my-pool" {
		t.Errorf("expected Config pool = my-pool, got %s", def.BlobPools.Config.Pool)
	}
	if def.BlobPools.Config.Namespace != "metadata" {
		t.Errorf("expected Config namespace = metadata, got %s", def.BlobPools.Config.Namespace)
	}
	if def.BlobPools.Keys.Namespace != "metadata" {
		t.Errorf("expected Keys namespace = metadata, got %s", def.BlobPools.Keys.Namespace)
	}
	if def.BlobPools.Data.Pool != "my-pool" {
		t.Errorf("expected Data pool = my-pool, got %s", def.BlobPools.Data.Pool)
	}
	if def.BlobPools.Data.Namespace != "data" {
		t.Errorf("expected Data namespace = data, got %s", def.BlobPools.Data.Namespace)
	}
}

func TestLoadConfigPoolSpecNamespaceErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"empty namespace", []string{"--pool", "my-pool/:keys"}},
		{"empty pool with namespace", []string{"--pool", "/ns:keys"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := loadConfig(tt.args)
			if err == nil {
				t.Fatal("expected pool spec parsing error")
			}
		})
	}
}

func TestLoadConfigBlobPoolsJSON(t *testing.T) {
	json := `{
		"repos": {
			"default": {
				"blob_pools": {
					"config": {"pool": "meta-pool", "namespace": "cfg"},
					"keys": {"pool": "meta-pool", "namespace": "cfg"},
					"locks": {"pool": "meta-pool"},
					"snapshots": {"pool": "data-pool"},
					"data": {"pool": "data-pool", "striped": false},
					"index": {"pool": "data-pool"}
				}
			}
		}
	}`
	path := writeTemp(t, json)

	config, _, err := loadConfig([]string{"--config", path})
	if err != nil {
		t.Fatal(err)
	}

	def := config.Repos["default"]
	if def == nil || def.BlobPools == nil {
		t.Fatal("expected default repo BlobPools")
	}
	if def.BlobPools.Config.Pool != "meta-pool" {
		t.Errorf("expected Config pool = meta-pool, got %s", def.BlobPools.Config.Pool)
	}
	if def.BlobPools.Config.Namespace != "cfg" {
		t.Errorf("expected Config namespace = cfg, got %s", def.BlobPools.Config.Namespace)
	}
	if def.BlobPools.Data.Pool != "data-pool" {
		t.Errorf("expected Data pool = data-pool, got %s", def.BlobPools.Data.Pool)
	}
	if def.BlobPools.Data.Striped == nil || *def.BlobPools.Data.Striped != false {
		t.Error("expected Data striped = false")
	}
	if def.BlobPools.Locks.Namespace != "" {
		t.Errorf("expected Locks namespace empty, got %s", def.BlobPools.Locks.Namespace)
	}
}

func TestValidateTailscaleCapabilityListeners(t *testing.T) {
	t.Run("no capability allows anything", func(t *testing.T) {
		c := &Config{Stdio: true}
		c.validateTailscaleCapabilityListeners()
	})

	t.Run("stdio warns but no error", func(t *testing.T) {
		c := &Config{
			TailscaleCapability: "cap.example.com",
			Stdio:               true,
		}
		c.validateTailscaleCapabilityListeners()
	})

	t.Run("loopback TCP allowed", func(t *testing.T) {
		c := &Config{
			TailscaleCapability: "cap.example.com",
			Listeners: listenerFlags{
				{kind: listenerTypeTCP, address: "127.0.0.1:8080"},
			},
		}
		c.validateTailscaleCapabilityListeners()
	})

	t.Run("non-loopback TCP warns but no error", func(t *testing.T) {
		c := &Config{
			TailscaleCapability: "cap.example.com",
			Listeners: listenerFlags{
				{kind: listenerTypeTCP, address: "0.0.0.0:8080"},
			},
		}
		c.validateTailscaleCapabilityListeners()
	})

	t.Run("unix socket allowed", func(t *testing.T) {
		c := &Config{
			TailscaleCapability: "cap.example.com",
			Listeners: listenerFlags{
				{kind: listenerTypeUnix, address: "/tmp/test.sock"},
			},
		}
		c.validateTailscaleCapabilityListeners()
	})

	t.Run("systemd allowed", func(t *testing.T) {
		c := &Config{
			TailscaleCapability: "cap.example.com",
			Listeners: listenerFlags{
				{kind: listenerTypeSystemd, address: "test"},
			},
		}
		c.validateTailscaleCapabilityListeners()
	})
}

func TestLoadConfigAccessValidation(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantErr bool
	}{
		{"valid r", `{"repos":{"default":{"pools":["p"],"access":"r"}}}`, false},
		{"valid ra", `{"repos":{"default":{"pools":["p"],"access":"ra"}}}`, false},
		{"valid rw", `{"repos":{"default":{"pools":["p"],"access":"rw"}}}`, false},
		{"valid read-only", `{"repos":{"default":{"pools":["p"],"access":"read-only"}}}`, false},
		{"valid read-append", `{"repos":{"default":{"pools":["p"],"access":"read-append"}}}`, false},
		{"valid read-write", `{"repos":{"default":{"pools":["p"],"access":"read-write"}}}`, false},
		{"empty defaults to rw", `{"repos":{"default":{"pools":["p"]}}}`, false},
		{"invalid access", `{"repos":{"default":{"pools":["p"],"access":"bad"}}}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTemp(t, tt.json)
			config, _, err := loadConfig([]string{"--config", path})
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			def := config.Repos["default"]
			if def == nil {
				t.Fatal("expected default repo")
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
