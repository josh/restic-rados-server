package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
)

var cephDaemonLogs *LogDemux

func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"restic-rados-server": main,
	})
}

const timeoutGracePeriod = 2 * time.Second

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

	cephDaemonLogs = &LogDemux{}

	var setupBuffer bytes.Buffer
	detachSetup := cephDaemonLogs.Attach(&setupBuffer)
	confPath, err := startCephCluster(t, ctx, cephDaemonLogs)
	if err != nil {
		detachSetup()
		t.Log("=== Ceph cluster setup logs ===")
		_, _ = io.Copy(t.Output(), &setupBuffer)
		t.Fatal(err)
	}
	detachSetup()

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
					"sha256":             cmdSHA256,
					"byte-count":         cmdByteCount,
					"create-pool":        cmdCreatePool,
					"envsubst":           cmdEnvsubst,
					"rados-object-count": cmdRadosObjectCount,
					"scrubhex":           cmdScrubHex,
					"tail-logs":          cmdTailLogs,
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
					env.Setenv("CEPH_SERVER_VERBOSE", "true")
					env.Setenv("CEPH_SERVER_LOG_FILE", logFile)

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

func cmdTailLogs(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! tail-logs")
	}

	ctx, ok := ts.Value("ctx").(context.Context)
	if !ok {
		ts.Fatalf("context not found in testscript Env.Values")
	}

	logFile := ts.Getenv("CEPH_SERVER_LOG_FILE")
	if logFile == "" {
		ts.Fatalf("CEPH_SERVER_LOG_FILE not set")
	}

	f, err := os.Open(logFile)
	if err != nil {
		ts.Fatalf("failed to open log file for reading: %v", err)
	}
	ts.Defer(func() {
		if err := f.Close(); err != nil {
			ts.Logf("failed to close log file: %v", err)
		}
	})

	pipeReader, detach := cephDaemonLogs.AttachPipe()
	ts.Defer(detach)

	serverReader := bufio.NewReader(f)
	cephReader := bufio.NewReader(pipeReader)

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				line, err := serverReader.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						continue
					}
					ts.Logf("tail-server-log: error: %v", err)
					return
				}
				ts.Logf("[restic-rados-server] %s", strings.TrimRight(line, "\n"))
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				line, err := cephReader.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						continue
					}
					ts.Logf("tail-ceph-log: error: %v", err)
					return
				}
				ts.Logf("[ceph] %s", strings.TrimRight(line, "\n"))
			}
		}
	}()
}

func startCephCluster(t *testing.T, ctx context.Context, out io.Writer) (string, error) {
	t.Helper()

	startupCtx, startupCancel := context.WithTimeout(ctx, 10*time.Second)
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

	if err := createECProfile(startupCtx, confPath); err != nil {
		return "", err
	}

	return confPath, nil
}

func setupCephDir(ctx context.Context, tmpDir string, out io.Writer) (string, error) {
	fsid := "6bb5784d-86b1-4b48-aff7-04d5dd22ef07"
	confPath := filepath.Join(tmpDir, "ceph.conf")

	cephConfig := map[string]map[string]string{
		"global": {
			"fsid":                                  fsid,
			"mon_host":                              "v1:127.0.0.1:6789/0",
			"public_network":                        "127.0.0.1/32",
			"auth_cluster_required":                 "none",
			"auth_service_required":                 "none",
			"auth_client_required":                  "none",
			"auth_allow_insecure_global_id_reclaim": "true",
			"pid_file":                              filepath.Join(tmpDir, "$type.$id.pid"),
			"admin_socket":                          filepath.Join(tmpDir, "$name.$pid.asok"),
			"crash_dir":                             filepath.Join(tmpDir, "crash"),
			"exporter_sock_dir":                     filepath.Join(tmpDir, "run"),
			"immutable_object_cache_sock":           filepath.Join(tmpDir, "run", "immutable_object_cache.sock"),
			"keyring":                               "/dev/null",
			"run_dir":                               filepath.Join(tmpDir, "run"),
			"log_to_file":                           "false",
			"log_to_stderr":                         "true",
			"osd_max_object_size":                   "33554432", // 32Mi
			"osd_max_write_size":                    "16",       // 16 MB
			"osd_pool_default_size":                 "1",
			"osd_pool_default_min_size":             "1",
			"osd_crush_chooseleaf_type":             "0",
			"mon_allow_pool_size_one":               "true",
		},
		"mon": {
			"mon_initial_members":       "mon1",
			"mon_data":                  filepath.Join(tmpDir, "mon", "ceph-$id"),
			"mon_cluster_log_to_file":   "false",
			"mon_cluster_log_to_stderr": "true",
			"mon_allow_pool_delete":     "true",
		},
		"osd": {
			"osd_data":        filepath.Join(tmpDir, "osd", "ceph-$id"),
			"osd_objectstore": "memstore",
		},
	}

	err := os.MkdirAll(filepath.Join(tmpDir, "mon"), 0o755)
	if err != nil {
		return confPath, err
	}

	for i := 0; i < 3; i++ {
		err = os.MkdirAll(filepath.Join(tmpDir, "osd", fmt.Sprintf("ceph-%d", i)), 0o755)
		if err != nil {
			return confPath, err
		}
	}

	err = os.MkdirAll(filepath.Join(tmpDir, "run"), 0o755)
	if err != nil {
		return confPath, err
	}

	err = os.MkdirAll(filepath.Join(tmpDir, "crash"), 0o755)
	if err != nil {
		return confPath, err
	}

	confContent := generateINIConfig(cephConfig)
	err = os.WriteFile(confPath, []byte(confContent), 0o644)
	if err != nil {
		return confPath, err
	}

	monmapPath := filepath.Join(tmpDir, "monmap")
	cmd := exec.CommandContext(ctx, "monmaptool", "--conf", confPath, monmapPath, "--create", "--fsid", fsid)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return confPath, fmt.Errorf("failed to create monitor map: %w", err)
	}

	cmd = exec.CommandContext(ctx, "monmaptool", "--conf", confPath, monmapPath, "--add", "mon1", "127.0.0.1:6789")
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return confPath, fmt.Errorf("failed to add monitor to map: %w", err)
	}

	cmd = exec.CommandContext(ctx, "ceph-mon", "--conf", confPath, "--mkfs", "--id", "mon1", "--monmap", monmapPath)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return confPath, fmt.Errorf("failed to initialize monitor filesystem: %w", err)
	}

	err = os.Remove(monmapPath)
	if err != nil {
		return confPath, err
	}

	return confPath, nil
}

func generateINIConfig(config map[string]map[string]string) string {
	var result strings.Builder

	sections := make([]string, 0, len(config))
	for section := range config {
		sections = append(sections, section)
	}
	sort.Strings(sections)

	for i, section := range sections {
		if i > 0 {
			result.WriteString("\n")
		}
		result.WriteString(fmt.Sprintf("[%s]\n", section))

		keys := make([]string, 0, len(config[section]))
		for key := range config[section] {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			result.WriteString(fmt.Sprintf("%s = %s\n", key, config[section][key]))
		}
	}

	return result.String()
}

func startCephMon(t *testing.T, ctx context.Context, startupCtx context.Context, confPath string, out io.Writer) error {
	t.Helper()
	cmd := exec.CommandContext(ctx, "ceph-mon", "--conf", confPath, "--id", "mon1", "--foreground")
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to spawn ceph-mon: %w", err)
	}

	t.Cleanup(func() {
		if err := cmd.Wait(); err != nil {
			t.Logf("ceph-mon exited with error: %v", err)
		}
	})

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-startupCtx.Done():
			return startupCtx.Err()
		case <-ticker.C:
			if status, err := checkCephStatus(startupCtx, confPath); err == nil && status.Monmap.NumMons > 0 {
				return nil
			}
		}
	}
}

func startCephOsd(t *testing.T, ctx context.Context, startupCtx context.Context, confPath string, out io.Writer) error {
	t.Helper()

	for i := 0; i < 3; i++ {
		osdID := strconv.Itoa(i)

		cmd := exec.CommandContext(ctx, "ceph-osd", "--conf", confPath, "--id", osdID, "--mkfs")
		cmd.Stdout = out
		cmd.Stderr = out

		err := cmd.Run()
		if err != nil {
			return fmt.Errorf("failed to initialize OSD %d filesystem: %w", i, err)
		}

		cmd = exec.CommandContext(ctx, "ceph-osd", "--conf", confPath, "--id", osdID, "--foreground")
		cmd.Stdout = out
		cmd.Stderr = out

		err = cmd.Start()
		if err != nil {
			return fmt.Errorf("failed to start OSD %d: %w", i, err)
		}

		t.Cleanup(func() {
			if err := cmd.Wait(); err != nil {
				t.Logf("ceph-osd %d exited: %v", i, err)
			}
		})
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-startupCtx.Done():
			return startupCtx.Err()
		case <-ticker.C:
			if status, err := checkCephStatus(startupCtx, confPath); err == nil && status.Osdmap.NumUpOsds >= 3 {
				return nil
			}
		}
	}
}

func createECProfile(ctx context.Context, confPath string) error {
	cmd := exec.CommandContext(ctx,
		"ceph", "--conf", confPath,
		"osd", "erasure-code-profile", "set", "k2m1",
		"k=2", "m=1", "crush-failure-domain=osd")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to create EC profile k2m1: %w, output: %s", err, output)
	}

	return nil
}

type cephStatus struct {
	Health cephStatusHealth `json:"health"`
	Pgmap  cephStatusPgmap  `json:"pgmap"`
	Monmap cephStatusMonmap `json:"monmap"`
	Osdmap cephStatusOsdmap `json:"osdmap"`
}

type cephStatusHealth struct {
	Status string `json:"status"`
}

type cephStatusPgmap struct {
	PgsByState []cephStatusPgState `json:"pgs_by_state"`
	NumPgs     int                 `json:"num_pgs"`
}

type cephStatusPgState struct {
	StateName string `json:"state_name"`
	Count     int    `json:"count"`
}

type cephStatusMonmap struct {
	NumMons int `json:"num_mons"`
}

type cephStatusOsdmap struct {
	NumUpOsds int `json:"num_up_osds"`
}

func checkCephStatus(ctx context.Context, confPath string) (cephStatus, error) {
	statusCmd := exec.CommandContext(ctx, "ceph", "--conf", confPath, "status", "--format", "json")
	output, err := statusCmd.Output()
	if err != nil {
		return cephStatus{}, err
	}

	var status cephStatus
	err = json.Unmarshal(output, &status)
	if err != nil {
		return cephStatus{}, err
	}

	return status, err
}

type LogDemux struct {
	outs sync.Map
}

func (ld *LogDemux) Write(p []byte) (n int, err error) {
	var writeErr error
	ld.outs.Range(func(key, _ interface{}) bool {
		if writer, ok := key.(io.Writer); ok {
			if written, err := writer.Write(p); err != nil {
				writeErr = err
				return false
			} else if written != len(p) {
				writeErr = fmt.Errorf("short write: expected %d, got %d", len(p), written)
				return false
			}
		}
		return true
	})

	if writeErr != nil {
		return 0, writeErr
	}
	return len(p), nil
}

func (ld *LogDemux) Attach(writer io.Writer) func() {
	ld.outs.Store(writer, struct{}{})
	return func() {
		ld.outs.Delete(writer)
	}
}

func (ld *LogDemux) AttachPipe() (*io.PipeReader, func()) {
	pr, pw := io.Pipe()
	detach := ld.Attach(pw)

	return pr, func() {
		detach()
		_ = pw.Close()
	}
}

func cmdWait4socket(ts *testscript.TestScript, neg bool, args []string) {
	ctx, ok := ts.Value("ctx").(context.Context)
	if !ok {
		ts.Fatalf("context not found in testscript Env.Values")
	}

	if neg {
		ts.Fatalf("unsupported: ! wait4socket")
	}
	if len(args) < 1 {
		ts.Fatalf("usage: wait4socket <endpoint> [<endpoint>...]")
	}

	for _, endpoint := range args {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		timeout := time.After(3 * time.Second)
		var success bool

		if strings.Contains(endpoint, ":") {
			for !success {
				select {
				case <-ctx.Done():
					ts.Fatalf("context cancelled while waiting for %s: %v", endpoint, ctx.Err())
				case <-timeout:
					ts.Fatalf("TCP listener did not respond in time: %s", endpoint)
				case <-ticker.C:
					conn, err := net.DialTimeout("tcp", endpoint, 100*time.Millisecond)
					if err == nil {
						_ = conn.Close()
						success = true
					}
				}
			}
		} else {
			for !success {
				select {
				case <-ctx.Done():
					ts.Fatalf("context cancelled while waiting for %s: %v", endpoint, ctx.Err())
				case <-timeout:
					ts.Fatalf("socket did not appear in time: %s", endpoint)
				case <-ticker.C:
					info, err := os.Stat(endpoint)
					if err == nil && info.Mode()&os.ModeSocket != 0 {
						success = true
					}
				}
			}
		}
	}
}

func cmdEnvsubst(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! envsubst")
	}
	if len(args) != 2 {
		ts.Fatalf("usage: envsubst <input> <output>")
	}

	data, err := os.ReadFile(ts.MkAbs(args[0]))
	if err != nil {
		ts.Fatalf("failed to read input: %v", err)
	}

	result := os.Expand(string(data), ts.Getenv)

	if err := os.WriteFile(ts.MkAbs(args[1]), []byte(result), 0o644); err != nil {
		ts.Fatalf("failed to write output: %v", err)
	}
}

func cmdRadosObjectCount(ts *testscript.TestScript, neg bool, args []string) {
	ctx, ok := ts.Value("ctx").(context.Context)
	if !ok {
		ts.Fatalf("context not found in testscript Env.Values")
	}

	if neg {
		ts.Fatalf("unsupported: ! rados-object-count")
	}
	if len(args) != 1 {
		ts.Fatalf("usage: rados-object-count <prefix>")
	}

	prefix := args[0]
	pool := ts.Getenv("CEPH_POOL")
	if pool == "" {
		ts.Fatalf("CEPH_POOL environment variable not set")
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
		if err != nil {
			ts.Fatalf("failed to read input file: %v", err)
		}
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
		if err != nil {
			ts.Fatalf("failed to write output file: %v", err)
		}
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
	if err != nil {
		ts.Fatalf("invalid size: %v", err)
	}

	if size < 0 {
		ts.Fatalf("size must be non-negative")
	}

	file, err := os.Create(ts.MkAbs(path))
	if err != nil {
		ts.Fatalf("failed to create file: %v", err)
	}
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
	if err != nil {
		ts.Fatalf("failed to write file: %v", err)
	}
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
	if err != nil {
		ts.Fatalf("failed to read file: %v", err)
	}

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
	ctx, ok := ts.Value("ctx").(context.Context)
	if !ok {
		ts.Fatalf("context not found in testscript Env.Values")
	}

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
	if err != nil {
		ts.Fatalf("failed to generate pool name: %v", err)
	}
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
					"osd", "pool", "create", poolName, "8", "8", "erasure", "k2m1")
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
		ts.Setenv("CEPH_POOL", poolName)
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
	if err != nil {
		ts.Fatalf("failed to read file: %v", err)
	}

	_, _ = fmt.Fprintf(ts.Stdout(), "%d\n", len(data))
}
