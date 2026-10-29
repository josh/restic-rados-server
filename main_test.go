package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
)

var cephLogPath string

func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"restic-rados-server": main,
	})
}

const (
	timeoutGracePeriod  = 2 * time.Second
	testECProfileName   = "k2m1"
	testECCrushRuleName = "test-k2m1"

	// testECCrushRuleKeepalivePoolName names an otherwise unused pool that keeps
	// testECCrushRuleName referenced for the test cluster's lifetime. Newer Ceph
	// releases delete an erasure CRUSH rule with its last pool, which can race
	// with parallel creation of another pool using the rule.
	testECCrushRuleKeepalivePoolName = "test-k2m1-rule-keepalive"
)

func TestScript(t *testing.T) {
	ctx := t.Context()
	var cancel context.CancelFunc

	var deadline time.Time

	if dl, ok := t.Deadline(); ok {
		if time.Until(dl) <= timeoutGracePeriod {
			t.Fatalf("not enough time")
		}
		deadline = dl.Add(-timeoutGracePeriod)
		ctx, cancel = context.WithDeadline(ctx, deadline)
		t.Cleanup(cancel)
	}

	cephLogPath = filepath.Join(t.TempDir(), "ceph.log")
	cephLog, err := os.OpenFile(cephLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cephLog.Close() })
	var confPath string
	for attempt := 1; attempt <= 2; attempt++ {
		attemptCtx, attemptCancel := context.WithCancel(ctx)
		confPath, err = startCephCluster(t, attemptCtx, cephLog)
		if err == nil {
			t.Cleanup(attemptCancel)
			break
		}
		attemptCancel()
		t.Logf("ceph cluster startup attempt %d failed: %v", attempt, err)
	}
	if err != nil {
		data, _ := os.ReadFile(cephLogPath)
		t.Log("=== Ceph cluster setup logs ===")
		_, _ = t.Output().Write(data)
		t.Fatal(err)
	}

	updateScripts, _ := strconv.ParseBool(os.Getenv("UPDATE_SCRIPTS"))

	for _, poolType := range []string{"replicated", "erasure"} {
		poolType := poolType
		t.Run(poolType, func(t *testing.T) {
			testscript.Run(t, testscript.Params{
				Dir:                 "testdata",
				ContinueOnError:     true,
				RequireExplicitExec: true,
				UpdateScripts:       updateScripts,
				Deadline:            deadline,
				Cmds: map[string]func(*testscript.TestScript, bool, []string){
					"bin-cmp":            cmdBinCmp,
					"bin-file":           cmdBinFile,
					"byte-count":         cmdByteCount,
					"capture-output":     cmdCaptureOutput,
					"create-pool":        cmdCreatePool,
					"envsubst":           cmdEnvsubst,
					"partial-http-post":  cmdPartialHTTPPost,
					"rados-object-count": cmdRadosObjectCount,
					"scrubhex":           cmdScrubHex,
					"sha256":             cmdSHA256,
					"tail-logs":          cmdTailLogs,
					"wait4exit":          cmdWait4Exit,
					"wait4http":          cmdWait4HTTP,
					"wait4log":           cmdWait4log,
					"wait4socket":        cmdWait4socket,
				},
				Setup: func(env *testscript.Env) error {
					scriptCtx, cancel := context.WithCancel(ctx)
					env.Defer(cancel)
					env.Values["ctx"] = scriptCtx

					logFile, err := touchServerLog(t, t.TempDir())
					if err != nil {
						return err
					}
					env.Setenv("RESTIC_RADOS_SERVER_VERBOSE", "true")
					env.Setenv("RESTIC_RADOS_SERVER_LOG_FILE", logFile)
					env.Setenv("CEPH_RESTIC_SERVER_VERBOSE", "true")
					env.Setenv("CEPH_RESTIC_SERVER_LOG_FILE", logFile)

					env.Setenv("CEPH_CONF", confPath)
					env.Setenv("RESTIC_CACHE_DIR", filepath.Join(t.TempDir(), "restic-cache"))
					env.Setenv("DEFAULT_POOL_TYPE", poolType)

					port, err := getFreePort()
					if err != nil {
						return fmt.Errorf("failed to allocate PORT: %w", err)
					}
					env.Setenv("PORT", strconv.Itoa(port))

					return nil
				},
			})
		})
	}
}

func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = listener.Close() }()

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

func touchServerLog(t *testing.T, tmpDir string) (string, error) {
	t.Helper()

	logFile := filepath.Join(tmpDir, "server.log")

	file, err := os.Create(logFile)
	if err != nil {
		return "", fmt.Errorf("failed to create log file: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("failed to close log file: %w", err)
	}

	return logFile, nil
}

func scriptContext(ts *testscript.TestScript) context.Context {
	ctx, ok := ts.Value("ctx").(context.Context)
	if !ok {
		ts.Fatalf("context not found in testscript Env.Values")
	}
	return ctx
}

func cmdTailLogs(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! tail-logs")
	}

	logFile := ts.Getenv("RESTIC_RADOS_SERVER_LOG_FILE")
	if logFile == "" {
		ts.Fatalf("RESTIC_RADOS_SERVER_LOG_FILE not set")
	}

	cephLog, err := os.Open(cephLogPath)
	ts.Check(err)
	_, err = cephLog.Seek(0, io.SeekEnd)
	ts.Check(err)

	ts.Defer(func() {
		defer func() { _ = cephLog.Close() }()
		logLines := func(prefix string, data []byte) {
			if len(data) > 0 {
				ts.Logf("%s %s", prefix, strings.ReplaceAll(strings.TrimSuffix(string(data), "\n"), "\n", "\n"+prefix+" "))
			}
		}
		data, _ := os.ReadFile(logFile)
		logLines("[restic-rados-server]", data)
		data, _ = io.ReadAll(cephLog)
		logLines("[ceph]", data[max(0, len(data)-32<<10):])
	})
}

func startCephCluster(t *testing.T, ctx context.Context, out io.Writer) (string, error) {
	t.Helper()

	startupCtx, startupCancel := context.WithTimeout(ctx, 30*time.Second)
	defer startupCancel()

	tmpDir := t.TempDir()
	confPath, err := setupCephDir(startupCtx, tmpDir, out)
	if err != nil {
		return "", err
	}

	if err := startCephMon(t, ctx, startupCtx, confPath, out); err != nil {
		return "", err
	}

	if err := startCephOsd(t, ctx, startupCtx, confPath, out); err != nil {
		return "", err
	}

	if err := runCommand(startupCtx, out, "ceph", "--conf", confPath, "osd", "erasure-code-profile", "set", testECProfileName, "k=2", "m=1", "crush-failure-domain=osd"); err != nil {
		return "", err
	}
	if err := runCommand(startupCtx, out, "ceph", "--conf", confPath, "osd", "crush", "rule", "create-erasure", testECCrushRuleName, testECProfileName); err != nil {
		return "", err
	}
	if err := runCommand(startupCtx, out, "ceph", "--conf", confPath, "osd", "pool", "create", testECCrushRuleKeepalivePoolName, "1", "1", "erasure", testECProfileName, testECCrushRuleName); err != nil {
		return "", err
	}

	return confPath, nil
}

func randomUUID() string {
	uuid := make([]byte, 16)
	_, _ = rand.Read(uuid)
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}

const cephConfTemplate = `[global]
fsid = %[1]s
mon_host = v1:%[2]s/0
public_network = 127.0.0.1/32
auth_cluster_required = none
auth_service_required = none
auth_client_required = none
auth_allow_insecure_global_id_reclaim = true
pid_file = %[3]s/$type.$id.pid
admin_socket = %[3]s/$name.$pid.asok
crash_dir = %[3]s/crash
exporter_sock_dir = %[3]s/run
immutable_object_cache_sock = %[3]s/run/immutable_object_cache.sock
keyring = /dev/null
run_dir = %[3]s/run
log_to_file = false
log_to_stderr = true
osd_max_object_size = 33554432
osd_max_write_size = 16
osd_pool_default_size = 1
osd_pool_default_min_size = 1
osd_crush_chooseleaf_type = 0
mon_allow_pool_size_one = true
rados_osd_op_timeout = 60

[mon]
mon_initial_members = mon1
mon_data = %[3]s/mon/ceph-$id
mon_cluster_log_to_file = false
mon_cluster_log_to_stderr = true
mon_allow_pool_delete = true

[osd]
osd_data = %[3]s/osd/ceph-$id
osd_objectstore = memstore
osd_class_update_on_start = false
osd_crush_update_on_start = false
`

func setupCephDir(ctx context.Context, tmpDir string, out io.Writer) (string, error) {
	monPort, err := getFreePort()
	if err != nil {
		return "", fmt.Errorf("failed to allocate monitor port: %w", err)
	}
	monAddr := fmt.Sprintf("127.0.0.1:%d", monPort)
	fsid := randomUUID()

	confPath := filepath.Join(tmpDir, "ceph.conf")
	if err := os.WriteFile(confPath, []byte(fmt.Sprintf(cephConfTemplate, fsid, monAddr, tmpDir)), 0o644); err != nil {
		return confPath, err
	}

	for _, dir := range []string{"mon", "osd/ceph-0", "osd/ceph-1", "osd/ceph-2", "run", "crash"} {
		if err := os.MkdirAll(filepath.Join(tmpDir, dir), 0o755); err != nil {
			return confPath, err
		}
	}

	monmapPath := filepath.Join(tmpDir, "monmap")
	if err := runCommand(ctx, out, "monmaptool", "--conf", confPath, monmapPath, "--create", "--fsid", fsid); err != nil {
		return confPath, err
	}
	if err := runCommand(ctx, out, "monmaptool", "--conf", confPath, monmapPath, "--add", "mon1", "v1:"+monAddr); err != nil {
		return confPath, err
	}
	if err := runCommand(ctx, out, "ceph-mon", "--conf", confPath, "--mkfs", "--id", "mon1", "--monmap", monmapPath); err != nil {
		return confPath, err
	}

	return confPath, os.Remove(monmapPath)
}

func runCommand(ctx context.Context, out io.Writer, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

func startDaemon(t *testing.T, ctx context.Context, out io.Writer, name string, args ...string) error {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to spawn %s: %w", name, err)
	}
	t.Cleanup(func() {
		if err := cmd.Wait(); err != nil {
			t.Logf("%s %s exited: %v", name, strings.Join(args, " "), err)
		}
	})
	return nil
}

func waitForCeph(ctx context.Context, confPath string, ready func(cephStatus) bool) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if status, err := checkCephStatus(ctx, confPath); err == nil && ready(status) {
				return nil
			}
		}
	}
}

func startCephMon(t *testing.T, ctx context.Context, startupCtx context.Context, confPath string, out io.Writer) error {
	t.Helper()
	if err := startDaemon(t, ctx, out, "ceph-mon", "--conf", confPath, "--id", "mon1", "--foreground"); err != nil {
		return err
	}
	return waitForCeph(startupCtx, confPath, func(status cephStatus) bool { return status.Monmap.NumMons > 0 })
}

func startCephOsd(t *testing.T, ctx context.Context, startupCtx context.Context, confPath string, out io.Writer) error {
	t.Helper()

	for i := 0; i < 3; i++ {
		osdID := strconv.Itoa(i)
		osdUUID := randomUUID()
		if err := runCommand(startupCtx, out, "ceph", "--conf", confPath, "osd", "new", osdUUID, osdID); err != nil {
			return err
		}
		if err := runCommand(startupCtx, out, "ceph", "--conf", confPath, "osd", "crush", "create-or-move", "osd."+osdID, "1.0", "root=default", "host=localhost"); err != nil {
			return err
		}
		if err := runCommand(ctx, out, "ceph-osd", "--conf", confPath, "--id", osdID, "--mkfs", "--osd-uuid", osdUUID); err != nil {
			return err
		}
		if err := startDaemon(t, ctx, out, "ceph-osd", "--conf", confPath, "--id", osdID, "--foreground"); err != nil {
			return err
		}
	}

	return waitForCeph(startupCtx, confPath, func(status cephStatus) bool { return status.Osdmap.NumUpOsds >= 3 })
}

type cephStatus struct {
	Monmap struct {
		NumMons int `json:"num_mons"`
	} `json:"monmap"`
	Osdmap struct {
		NumUpOsds int `json:"num_up_osds"`
	} `json:"osdmap"`
}

func checkCephStatus(ctx context.Context, confPath string) (cephStatus, error) {
	output, err := exec.CommandContext(ctx, "ceph", "--conf", confPath, "status", "--format", "json").Output()
	if err != nil {
		return cephStatus{}, err
	}
	var status cephStatus
	return status, json.Unmarshal(output, &status)
}

func cmdWait4socket(ts *testscript.TestScript, neg bool, args []string) {
	ctx := scriptContext(ts)

	if len(args) < 1 {
		ts.Fatalf("usage: wait4socket <endpoint> [<endpoint>...]")
	}

	for _, endpoint := range args {
		network := "unix"
		if strings.Contains(endpoint, ":") {
			network = "tcp"
		}

		check := func() bool {
			conn, err := net.DialTimeout(network, endpoint, 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				return !neg
			}
			if neg {
				return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, fs.ErrNotExist)
			}
			return false
		}

		if check() {
			continue
		}

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		timeout := time.After(15 * time.Second)
		var success bool

		for !success {
			select {
			case <-ctx.Done():
				ts.Fatalf("context cancelled while waiting for %s: %v", endpoint, ctx.Err())
			case <-timeout:
				if neg {
					ts.Fatalf("endpoint still accepting connections: %s", endpoint)
				}
				if network == "tcp" {
					ts.Fatalf("TCP listener did not respond in time: %s", endpoint)
				}
				ts.Fatalf("socket did not appear in time: %s", endpoint)
			case <-ticker.C:
				success = check()
			}
		}
	}
}

func cmdWait4Exit(ts *testscript.TestScript, neg bool, args []string) {
	ctx := scriptContext(ts)

	if neg {
		ts.Fatalf("unsupported: ! wait4exit")
	}
	if len(args) != 1 {
		ts.Fatalf("usage: wait4exit <timeout>")
	}

	timeout, err := time.ParseDuration(args[0])
	ts.Check(err)
	if timeout <= 0 {
		ts.Fatalf("timeout must be positive")
	}

	background := ts.BackgroundCmds()
	if len(background) != 1 {
		ts.Fatalf("wait4exit requires exactly one background process, got %d", len(background))
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		err := background[0].Process.Signal(syscall.Signal(0))
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return
		}
		ts.Check(err)

		select {
		case <-ctx.Done():
			ts.Fatalf("context cancelled while waiting for background process: %v", ctx.Err())
		case <-timer.C:
			ts.Fatalf("background process did not exit within %s", timeout)
		case <-ticker.C:
		}
	}
}

func cmdWait4log(ts *testscript.TestScript, neg bool, args []string) {
	ctx := scriptContext(ts)

	if neg {
		ts.Fatalf("unsupported: ! wait4log")
	}
	if len(args) != 2 {
		ts.Fatalf("usage: wait4log <pattern> <file>")
	}

	pattern, err := regexp.Compile(args[0])
	ts.Check(err)
	path := ts.MkAbs(args[1])

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(15 * time.Second)

	for {
		select {
		case <-ctx.Done():
			ts.Fatalf("context cancelled while waiting for %q in %s: %v", args[0], args[1], ctx.Err())
		case <-timeout:
			ts.Fatalf("pattern %q did not appear in %s in time", args[0], args[1])
		case <-ticker.C:
			data, err := os.ReadFile(path)
			if err == nil && pattern.Match(data) {
				return
			}
		}
	}
}

func cmdWait4HTTP(ts *testscript.TestScript, neg bool, args []string) {
	ctx := scriptContext(ts)

	if neg {
		ts.Fatalf("unsupported: ! wait4http")
	}
	if len(args) != 3 {
		ts.Fatalf("usage: wait4http <expected-status> <endpoint> <path>")
	}

	expected, err := strconv.Atoi(args[0])
	ts.Check(err)
	endpoint := args[1]
	urlPath := args[2]

	transport := &http.Transport{}
	if strings.Contains(endpoint, ":") {
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
		}
	} else {
		socketPath := ts.MkAbs(endpoint)
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	defer transport.CloseIdleConnections()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	timeout := time.After(30 * time.Second)

	lastStatus := "none"
	for {
		select {
		case <-ctx.Done():
			ts.Fatalf("context cancelled while waiting for status %d from %s%s: %v", expected, endpoint, urlPath, ctx.Err())
		case <-timeout:
			ts.Fatalf("status %d never returned from %s%s (last=%s)", expected, endpoint, urlPath, lastStatus)
		case <-ticker.C:
			resp, err := client.Get("http://localhost" + urlPath)
			if err != nil {
				lastStatus = err.Error()
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			lastStatus = strconv.Itoa(resp.StatusCode)
			if resp.StatusCode == expected {
				return
			}
		}
	}
}

func cmdPartialHTTPPost(ts *testscript.TestScript, neg bool, args []string) {
	ctx := scriptContext(ts)

	if neg {
		ts.Fatalf("unsupported: ! partial-http-post")
	}

	holdOpen := false
	if len(args) > 0 && args[0] == "--hold-open" {
		holdOpen = true
		args = args[1:]
	}
	if len(args) != 4 {
		ts.Fatalf("usage: partial-http-post [--hold-open] <address> <path> <declared-length> <body>")
	}

	declaredLength, err := strconv.ParseInt(args[2], 10, 64)
	ts.Check(err)
	if declaredLength <= int64(len(args[3])) {
		ts.Fatalf("declared length must exceed body length")
	}

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", args[0])
	if err != nil {
		ts.Fatalf("failed to connect to %s: %v", args[0], err)
	}

	request := fmt.Sprintf("POST %s HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", args[1], declaredLength, args[3])
	if _, err := io.Copy(conn, strings.NewReader(request)); err != nil {
		_ = conn.Close()
		ts.Fatalf("failed to write partial request: %v", err)
	}

	if holdOpen {
		ts.Defer(func() { _ = conn.Close() })
		return
	}
	if err := conn.Close(); err != nil {
		ts.Fatalf("failed to close partial request: %v", err)
	}
}

func cmdCaptureOutput(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! capture-output")
	}
	if len(args) != 1 {
		ts.Fatalf("usage: capture-output <env-var>")
	}
	if args[0] == "" || strings.Contains(args[0], "=") {
		ts.Fatalf("invalid environment variable name: %q", args[0])
	}

	value := strings.TrimSpace(ts.ReadFile("stdout"))
	if value == "" {
		ts.Fatalf("cannot capture empty stdout")
	}
	if strings.ContainsAny(value, "\r\n") {
		ts.Fatalf("cannot capture multiple stdout lines")
	}

	ts.Setenv(args[0], value)
}

func cmdEnvsubst(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! envsubst")
	}
	if len(args) != 2 {
		ts.Fatalf("usage: envsubst <input> <output>")
	}

	data, err := os.ReadFile(ts.MkAbs(args[0]))
	ts.Check(err)

	result := os.Expand(string(data), ts.Getenv)

	if err := os.WriteFile(ts.MkAbs(args[1]), []byte(result), 0o644); err != nil {
		ts.Fatalf("failed to write output: %v", err)
	}
}

func cmdRadosObjectCount(ts *testscript.TestScript, neg bool, args []string) {
	ctx := scriptContext(ts)

	if neg {
		ts.Fatalf("unsupported: ! rados-object-count")
	}
	if len(args) != 1 {
		ts.Fatalf("usage: rados-object-count <prefix>")
	}

	prefix := args[0]
	pool := ts.Getenv("RESTIC_RADOS_SERVER_POOL")
	if pool == "" {
		ts.Fatalf("RESTIC_RADOS_SERVER_POOL environment variable not set")
	}

	confPath := ts.Getenv("CEPH_CONF")
	if confPath == "" {
		ts.Fatalf("CEPH_CONF environment variable not set")
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "rados", "--conf", confPath, "--pool", pool, "ls")
	output, err := cmd.CombinedOutput()
	if err != nil {
		ts.Fatalf("failed to list rados objects: %v\noutput: %s", err, string(output))
	}

	count := 0
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, prefix) {
			count++
		}
	}

	if err := scanner.Err(); err != nil {
		ts.Fatalf("failed to scan output: %v", err)
	}

	_, _ = fmt.Fprintf(ts.Stdout(), "%d\n", count)
}

func cmdScrubHex(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! scrubhex")
	}
	if len(args) != 2 {
		ts.Fatalf("usage: scrubhex <input-file> <output-file>\n  use 'stdin' for reading previous command output, 'stdout' for writing to stdout")
	}

	inputPath := args[0]
	outputPath := args[1]

	var input []byte
	var err error

	switch inputPath {
	case "stdin":
		input = []byte(ts.ReadFile("stdin"))
	case "stdout":
		input = []byte(ts.ReadFile("stdout"))
	case "stderr":
		input = []byte(ts.ReadFile("stderr"))
	default:
		input, err = os.ReadFile(ts.MkAbs(inputPath))
		ts.Check(err)
	}

	quotedHexPattern := regexp.MustCompile(`"[0-9a-f]{8,}"`)
	output := quotedHexPattern.ReplaceAll(input, []byte(`"[HEX]"`))

	hexPattern := regexp.MustCompile(`[0-9a-f]{8,}`)
	output = hexPattern.ReplaceAll(output, []byte("[HEX]"))

	switch outputPath {
	case "stdout":
		_, _ = ts.Stdout().Write(output)
	case "stderr":
		_, _ = ts.Stderr().Write(output)
	default:
		err = os.WriteFile(ts.MkAbs(outputPath), output, 0o644)
		ts.Check(err)
	}
}

func cmdBinFile(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! bin-file")
	}
	if len(args) != 3 {
		ts.Fatalf("usage: bin-file <rand|zeros|ones> <path> <size-bytes>")
	}

	mode := args[0]
	path := args[1]
	sizeStr := args[2]

	if mode != "rand" && mode != "zeros" && mode != "ones" {
		ts.Fatalf("mode must be rand, zeros, or ones")
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	ts.Check(err)

	if size < 0 {
		ts.Fatalf("size must be non-negative")
	}

	file, err := os.Create(ts.MkAbs(path))
	ts.Check(err)
	defer func() { _ = file.Close() }()

	var reader io.Reader
	switch mode {
	case "rand":
		reader = io.LimitReader(rand.Reader, size)
	case "zeros":
		reader = io.LimitReader(zeroReader{}, size)
	case "ones":
		reader = io.LimitReader(onesReader{}, size)
	}

	_, err = io.Copy(file, reader)
	ts.Check(err)
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

type onesReader struct{}

func (onesReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0xFF
	}
	return len(p), nil
}

func cmdSHA256(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! sha256")
	}
	if len(args) < 1 || len(args) > 2 {
		ts.Fatalf("usage: sha256 <path> [env-var]")
	}

	path := ts.MkAbs(args[0])

	data, err := os.ReadFile(path)
	ts.Check(err)

	hash := sha256.Sum256(data)
	hashStr := hex.EncodeToString(hash[:])

	if len(args) == 2 {
		ts.Setenv(args[1], hashStr)
	}
	_, _ = fmt.Fprintln(ts.Stdout(), hashStr)
}

func cmdBinCmp(ts *testscript.TestScript, neg bool, args []string) {
	if len(args) != 2 {
		ts.Fatalf("usage: bin-cmp file1 file2")
	}

	file1Path := ts.MkAbs(args[0])
	file2Path := ts.MkAbs(args[1])

	data1, err := os.ReadFile(file1Path)
	if err != nil {
		ts.Fatalf("failed to read %s: %v", args[0], err)
	}

	data2, err := os.ReadFile(file2Path)
	if err != nil {
		ts.Fatalf("failed to read %s: %v", args[1], err)
	}

	filesMatch := bytes.Equal(data1, data2)

	if neg {
		if filesMatch {
			ts.Fatalf("files are identical")
		}
		return
	}

	if filesMatch {
		return
	}

	hash1 := sha256.Sum256(data1)
	hash2 := sha256.Sum256(data2)

	var errMsg strings.Builder
	fmt.Fprintf(&errMsg, "files differ:\n")
	fmt.Fprintf(&errMsg, "  %s:\n", args[0])
	fmt.Fprintf(&errMsg, "    size: %d bytes\n", len(data1))
	fmt.Fprintf(&errMsg, "    sha256: %s\n", hex.EncodeToString(hash1[:]))
	fmt.Fprintf(&errMsg, "  %s:\n", args[1])
	fmt.Fprintf(&errMsg, "    size: %d bytes\n", len(data2))
	fmt.Fprintf(&errMsg, "    sha256: %s\n", hex.EncodeToString(hash2[:]))

	if len(data1) != len(data2) {
		ts.Fatalf("%s", errMsg.String())
	}

	var diffOffset int
	for i := 0; i < len(data1); i++ {
		if data1[i] != data2[i] {
			diffOffset = i
			break
		}
	}

	fmt.Fprintf(&errMsg, "  first difference at byte offset: %d (0x%x)\n\n", diffOffset, diffOffset)

	const contextSize = 32
	start := diffOffset - contextSize
	if start < 0 {
		start = 0
	}

	end1 := diffOffset + contextSize
	if end1 > len(data1) {
		end1 = len(data1)
	}

	end2 := diffOffset + contextSize
	if end2 > len(data2) {
		end2 = len(data2)
	}

	fmt.Fprintf(&errMsg, "--- %s context (bytes %d-%d) ---\n", args[0], start, end1)
	fmt.Fprintf(&errMsg, "%s\n", hex.Dump(data1[start:end1]))

	fmt.Fprintf(&errMsg, "--- %s context (bytes %d-%d) ---\n", args[1], start, end2)
	fmt.Fprintf(&errMsg, "%s", hex.Dump(data2[start:end2]))

	ts.Fatalf("%s", errMsg.String())
}

func cmdCreatePool(ts *testscript.TestScript, neg bool, args []string) {
	ctx := scriptContext(ts)

	if neg {
		ts.Fatalf("unsupported: ! create-pool")
	}

	var poolEnvName string
	var poolTypeArg string

	for _, arg := range args {
		if arg == "replicated" || arg == "erasure" {
			poolTypeArg = arg
		} else {
			poolEnvName = arg
		}
	}

	poolType := ts.Getenv("DEFAULT_POOL_TYPE")
	if poolType == "" {
		poolType = "replicated"
	}
	if poolTypeArg != "" {
		poolType = poolTypeArg
	}
	if poolType != "replicated" && poolType != "erasure" {
		ts.Fatalf("pool type must be 'replicated' or 'erasure', got: %s", poolType)
	}

	confPath := ts.Getenv("CEPH_CONF")
	if confPath == "" {
		ts.Fatalf("CEPH_CONF not set")
	}

	randBytes := make([]byte, 4)
	_, err := rand.Read(randBytes)
	ts.Check(err)
	poolName := "test-" + hex.EncodeToString(randBytes)

	const maxAttempts = 3
	var lastCreateErr error
	var output []byte

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(1 * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				ts.Fatalf("context cancelled during retry: %v", ctx.Err())
			case <-timer.C:
			}
		}

		if lastCreateErr == nil || attempt > 0 {
			var cmd *exec.Cmd
			switch poolType {
			case "replicated":
				cmd = exec.CommandContext(ctx, "ceph", "--conf", confPath,
					"osd", "pool", "create", poolName, "8")

			case "erasure":
				cmd = exec.CommandContext(ctx, "ceph", "--conf", confPath,
					"osd", "pool", "create", poolName, "8", "8", "erasure", testECProfileName, testECCrushRuleName)
			}

			output, lastCreateErr = cmd.CombinedOutput()

			if lastCreateErr != nil && attempt > 0 {
				ts.Logf("pool creation failed (attempt %d/%d), retrying: %v\noutput: %s",
					attempt+1, maxAttempts, lastCreateErr, output)
			}
		}

		checkCmd := exec.CommandContext(ctx, "ceph", "--conf", confPath,
			"osd", "pool", "get", poolName, "size")

		if checkCmd.Run() == nil {
			if lastCreateErr != nil {
				ts.Logf("warning: pool %s exists despite creation error: %v", poolName, lastCreateErr)
			}
			break
		}

		if lastCreateErr == nil {
			if attempt < maxAttempts-1 {
				continue
			}
			ts.Fatalf("pool %s does not exist after successful creation", poolName)
		}

		if attempt == maxAttempts-1 {
			ts.Fatalf("failed to create %s pool after %d attempts: %v\noutput: %s",
				poolType, maxAttempts, lastCreateErr, output)
		}
	}

	if poolEnvName == "" {
		ts.Setenv("RESTIC_RADOS_SERVER_POOL", poolName)
		ts.Setenv("CEPH_RESTIC_SERVER_POOL", poolName)
	} else {
		envVarName := "CEPH_POOL_" + strings.ToUpper(poolEnvName)
		ts.Setenv(envVarName, poolName)
	}
	ts.Setenv("CEPH_POOL_TYPE", poolType)

	ts.Defer(func() {
		cleanupCtx := context.Background()
		deleteCmd := exec.CommandContext(cleanupCtx,
			"ceph", "--conf", confPath,
			"osd", "pool", "delete", poolName, poolName,
			"--yes-i-really-really-mean-it")

		if err := deleteCmd.Run(); err != nil {
			ts.Logf("warning: failed to delete pool %s: %v", poolName, err)
			return
		}
	})
}

func cmdByteCount(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! byte-count")
	}
	if len(args) != 1 {
		ts.Fatalf("usage: byte-count <file>")
	}

	data, err := os.ReadFile(ts.MkAbs(args[0]))
	ts.Check(err)

	_, _ = fmt.Fprintf(ts.Stdout(), "%d\n", len(data))
}
