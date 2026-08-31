package main

import (
	"errors"
	"io"
	"math"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ceph/go-ceph/rados"
)

var (
	httpDurationBuckets   = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}
	radosDurationBuckets  = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	radosCallCountBuckets = []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000}
)

type histogramValue struct {
	mu     sync.RWMutex
	bounds []float64
	counts []atomic.Uint64
	count  atomic.Uint64
	sum    atomic.Uint64
}

func addFloatBits(bits *atomic.Uint64, v float64) {
	for {
		old := bits.Load()
		updated := math.Float64bits(math.Float64frombits(old) + v)
		if bits.CompareAndSwap(old, updated) {
			return
		}
	}
}

func (h *histogramValue) observe(v float64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for i, bound := range h.bounds {
		if v <= bound {
			h.counts[i].Add(1)
			break
		}
	}
	h.count.Add(1)
	addFloatBits(&h.sum, v)
}

type metricChild struct {
	labelValues []string
	value       atomic.Uint64
	histogram   *histogramValue
}

func (c *metricChild) add(v float64) {
	addFloatBits(&c.value, v)
}

func (c *metricChild) set(v float64) {
	c.value.Store(math.Float64bits(v))
}

func (c *metricChild) get() float64 {
	return math.Float64frombits(c.value.Load())
}

type metricFamily struct {
	name       string
	help       string
	kind       string
	labelNames []string
	buckets    []float64
	mu         sync.Mutex
	children   map[string]*metricChild
	order      []*metricChild
}

func (f *metricFamily) with(labelValues ...string) *metricChild {
	if len(labelValues) != len(f.labelNames) {
		panic("metric " + f.name + ": label count mismatch")
	}
	key := strings.Join(labelValues, "\x00")
	f.mu.Lock()
	defer f.mu.Unlock()
	if child, ok := f.children[key]; ok {
		return child
	}
	child := &metricChild{labelValues: labelValues}
	if f.kind == "histogram" {
		child.histogram = &histogramValue{
			bounds: f.buckets,
			counts: make([]atomic.Uint64, len(f.buckets)),
		}
	}
	f.children[key] = child
	f.order = append(f.order, child)
	return child
}

type metricsRegistry struct {
	families []*metricFamily
	collect  []func()
}

func (r *metricsRegistry) newFamily(name, help, kind string, buckets []float64, labelNames ...string) *metricFamily {
	family := &metricFamily{
		name:       name,
		help:       help,
		kind:       kind,
		labelNames: labelNames,
		buckets:    buckets,
		children:   make(map[string]*metricChild),
	}
	r.families = append(r.families, family)
	return family
}

func (r *metricsRegistry) newCounter(name, help string, labelNames ...string) *metricFamily {
	return r.newFamily(name, help, "counter", nil, labelNames...)
}

func (r *metricsRegistry) newGauge(name, help string, labelNames ...string) *metricFamily {
	return r.newFamily(name, help, "gauge", nil, labelNames...)
}

func (r *metricsRegistry) newHistogram(name, help string, buckets []float64, labelNames ...string) *metricFamily {
	return r.newFamily(name, help, "histogram", buckets, labelNames...)
}

func (r *metricsRegistry) onCollect(fn func()) {
	r.collect = append(r.collect, fn)
}

var metricLabelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func formatMetricValue(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func formatMetricLabels(names, values []string, extra ...string) string {
	if len(names)+len(extra) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, name := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(name)
		b.WriteString(`="`)
		b.WriteString(metricLabelEscaper.Replace(values[i]))
		b.WriteByte('"')
	}
	for i := 0; i+1 < len(extra); i += 2 {
		if i > 0 || len(names) > 0 {
			b.WriteByte(',')
		}
		b.WriteString(extra[i])
		b.WriteString(`="`)
		b.WriteString(extra[i+1])
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func (r *metricsRegistry) writeTo(w io.Writer) {
	for _, fn := range r.collect {
		fn()
	}
	var b strings.Builder
	for _, family := range r.families {
		family.mu.Lock()
		children := make([]*metricChild, len(family.order))
		copy(children, family.order)
		family.mu.Unlock()
		if len(children) == 0 {
			continue
		}
		b.WriteString("# HELP " + family.name + " " + family.help + "\n")
		b.WriteString("# TYPE " + family.name + " " + family.kind + "\n")
		for _, child := range children {
			if family.kind == "histogram" {
				child.histogram.mu.Lock()
				bucketCounts := make([]uint64, len(family.buckets))
				for i := range family.buckets {
					bucketCounts[i] = child.histogram.counts[i].Load()
				}
				count := child.histogram.count.Load()
				sum := math.Float64frombits(child.histogram.sum.Load())
				child.histogram.mu.Unlock()
				cumulative := uint64(0)
				for i, bound := range family.buckets {
					cumulative += bucketCounts[i]
					le := strconv.FormatFloat(bound, 'g', -1, 64)
					b.WriteString(family.name + "_bucket" + formatMetricLabels(family.labelNames, child.labelValues, "le", le) + " " + strconv.FormatUint(cumulative, 10) + "\n")
				}
				b.WriteString(family.name + "_bucket" + formatMetricLabels(family.labelNames, child.labelValues, "le", "+Inf") + " " + strconv.FormatUint(count, 10) + "\n")
				b.WriteString(family.name + "_sum" + formatMetricLabels(family.labelNames, child.labelValues) + " " + formatMetricValue(sum) + "\n")
				b.WriteString(family.name + "_count" + formatMetricLabels(family.labelNames, child.labelValues) + " " + strconv.FormatUint(count, 10) + "\n")
				continue
			}
			b.WriteString(family.name + formatMetricLabels(family.labelNames, child.labelValues) + " " + formatMetricValue(child.get()) + "\n")
		}
	}
	_, _ = io.WriteString(w, b.String())
}

type radosOpMetrics struct {
	success  *metricChild
	notFound *metricChild
	exists   *metricChild
	failure  *metricChild
	duration *histogramValue
}

type serverMetricsSet struct {
	registry  *metricsRegistry
	buildInfo *metricFamily

	httpRequests      *metricFamily
	httpDuration      *metricFamily
	httpRequestBytes  *metricFamily
	httpResponseBytes *metricFamily
	httpTTFB          *metricFamily
	httpRadosCalls    *histogramValue

	radosOps map[string]*radosOpMetrics

	cephConnected  *metricChild
	cephLosses     *metricChild
	cephReconnects *metricChild
}

var radosOpNames = []string{"stat", "read", "append", "remove", "create", "get_xattr", "set_xattr", "iter", "open_ioctx"}

var serverMetrics = newServerMetrics()

func newServerMetrics() *serverMetricsSet {
	registry := &metricsRegistry{}
	m := &serverMetricsSet{registry: registry}

	m.buildInfo = registry.newGauge("restic_rados_build_info", "Build information for restic-rados-server.", "version")

	m.httpRequests = registry.newCounter("restic_rados_http_requests_total", "HTTP requests served, by repository, operation, blob type, method, and status code.", "repo", "op", "type", "method", "status")
	m.httpDuration = registry.newHistogram("restic_rados_http_request_duration_seconds", "HTTP request duration in seconds.", httpDurationBuckets, "op", "method")
	m.httpRequestBytes = registry.newCounter("restic_rados_http_request_bytes_total", "Request body bytes read from clients.", "op")
	m.httpResponseBytes = registry.newCounter("restic_rados_http_response_bytes_total", "Response body bytes written.", "op")
	m.httpTTFB = registry.newHistogram("restic_rados_http_time_to_first_byte_seconds", "Time from request start to the first response header or body write.", httpDurationBuckets, "op")
	m.httpRadosCalls = registry.newHistogram("restic_rados_http_rados_calls", "RADOS calls issued per HTTP request.", radosCallCountBuckets).with().histogram

	radosOps := registry.newCounter("restic_rados_rados_ops_total", "RADOS operations issued, by operation and outcome.", "op", "outcome")
	radosDuration := registry.newHistogram("restic_rados_rados_op_duration_seconds", "RADOS operation duration in seconds, by operation.", radosDurationBuckets, "op")
	m.radosOps = make(map[string]*radosOpMetrics, len(radosOpNames))
	for _, op := range radosOpNames {
		m.radosOps[op] = &radosOpMetrics{
			success:  radosOps.with(op, "success"),
			notFound: radosOps.with(op, "not_found"),
			exists:   radosOps.with(op, "exists"),
			failure:  radosOps.with(op, "error"),
			duration: radosDuration.with(op).histogram,
		}
	}

	m.cephConnected = registry.newGauge("restic_rados_ceph_connected", "Whether the server currently holds a Ceph cluster connection.").with()
	m.cephLosses = registry.newCounter("restic_rados_ceph_connection_losses_total", "Ceph cluster connections lost to transient errors.").with()
	m.cephReconnects = registry.newCounter("restic_rados_ceph_reconnects_total", "Successful Ceph cluster reconnections.").with()

	registerRuntimeMetrics(registry)
	registerProcessMetrics(registry)
	return m
}

func registerRuntimeMetrics(registry *metricsRegistry) {
	registry.newGauge("go_info", "Information about the Go environment.", "version").with(runtime.Version()).set(1)
	goroutines := registry.newGauge("go_goroutines", "Number of goroutines that currently exist.").with()
	gomaxprocs := registry.newGauge("go_sched_gomaxprocs_threads", "The current runtime.GOMAXPROCS setting.").with()
	heapAlloc := registry.newGauge("go_memstats_heap_alloc_bytes", "Number of heap bytes allocated and still in use.").with()
	heapSys := registry.newGauge("go_memstats_heap_sys_bytes", "Number of heap bytes obtained from system.").with()
	allocTotal := registry.newCounter("go_memstats_alloc_bytes_total", "Total number of bytes allocated, even if freed.").with()
	gcCycles := registry.newCounter("go_gc_cycles_total", "Completed GC cycles.").with()
	lastGC := registry.newGauge("go_memstats_last_gc_time_seconds", "Number of seconds since 1970 of last garbage collection.").with()
	registry.onCollect(func() {
		goroutines.set(float64(runtime.NumGoroutine()))
		gomaxprocs.set(float64(runtime.GOMAXPROCS(0)))
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		heapAlloc.set(float64(stats.HeapAlloc))
		heapSys.set(float64(stats.HeapSys))
		allocTotal.set(float64(stats.TotalAlloc))
		gcCycles.set(float64(stats.NumGC))
		lastGC.set(float64(stats.LastGC) / 1e9)
	})
}

func registerProcessMetrics(registry *metricsRegistry) {
	registry.newGauge("process_start_time_seconds", "Start time of the process since unix epoch in seconds.").with().set(float64(time.Now().UnixNano()) / 1e9)
	cpu := registry.newCounter("process_cpu_seconds_total", "Total user and system CPU time spent in seconds.").with()
	rss := registry.newGauge("process_resident_memory_bytes", "Resident memory size in bytes.").with()
	openFDs := registry.newGauge("process_open_fds", "Number of open file descriptors.").with()
	maxFDs := registry.newGauge("process_max_fds", "Maximum number of open file descriptors.").with()
	registry.onCollect(func() {
		if seconds, ok := readProcessCPUSeconds(); ok {
			cpu.set(seconds)
		}
		if bytes, ok := readProcessResidentBytes(); ok {
			rss.set(bytes)
		}
		if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
			openFDs.set(float64(len(entries)))
		}
		if limit, ok := readProcessMaxFDs(); ok {
			maxFDs.set(limit)
		}
	})
}

func readProcessCPUSeconds() (float64, bool) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, false
	}
	_, after, found := strings.Cut(string(data), ") ")
	if !found {
		return 0, false
	}
	fields := strings.Fields(after)
	if len(fields) < 13 {
		return 0, false
	}
	utime, err := strconv.ParseFloat(fields[11], 64)
	if err != nil {
		return 0, false
	}
	stime, err := strconv.ParseFloat(fields[12], 64)
	if err != nil {
		return 0, false
	}
	return (utime + stime) / 100, true
}

func readProcessResidentBytes() (float64, bool) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0, false
	}
	return pages * float64(os.Getpagesize()), true
}

func readProcessMaxFDs() (float64, bool) {
	data, err := os.ReadFile("/proc/self/limits")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "Max open files") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "Max open files"))
		if len(fields) == 0 {
			return 0, false
		}
		limit, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return 0, false
		}
		return limit, true
	}
	return 0, false
}

func (m *serverMetricsSet) recordBuildInfo(v string) {
	m.buildInfo.with(v).set(1)
}

func radosObserve(op string, radosCalls *uint64) func(error) {
	if radosCalls != nil {
		atomic.AddUint64(radosCalls, 1)
	}
	series := serverMetrics.radosOps[op]
	started := time.Now()
	return func(err error) {
		series.duration.observe(time.Since(started).Seconds())
		switch {
		case err == nil || errors.Is(err, io.EOF):
			series.success.add(1)
		case errors.Is(err, rados.ErrNotFound):
			series.notFound.add(1)
		case errors.Is(err, rados.ErrObjectExists):
			series.exists.add(1)
		default:
			series.failure.add(1)
		}
	}
}

func normalizeMetricMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodDelete:
		return method
	default:
		return "other"
	}
}

func recordHTTPRequest(repoKey, op, blobType, method string, status int, duration, ttfb time.Duration, requestBytes, responseBytes int64, radosCalls uint64) {
	method = normalizeMetricMethod(method)
	serverMetrics.httpRequests.with(repoKey, op, blobType, method, strconv.Itoa(status)).add(1)
	serverMetrics.httpDuration.with(op, method).histogram.observe(duration.Seconds())
	if requestBytes > 0 {
		serverMetrics.httpRequestBytes.with(op).add(float64(requestBytes))
	}
	if responseBytes > 0 {
		serverMetrics.httpResponseBytes.with(op).add(float64(responseBytes))
	}
	if ttfb >= 0 {
		serverMetrics.httpTTFB.with(op).histogram.observe(ttfb.Seconds())
	}
	serverMetrics.httpRadosCalls.observe(float64(radosCalls))
}

func metricsExposition() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		serverMetrics.registry.writeTo(w)
	})
}
