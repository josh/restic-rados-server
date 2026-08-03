package main

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ceph/go-ceph/rados"
)

const configObjectName = "config"

var (
	errObjectNotFound      = errors.New("object not found")
	errObjectExists        = errors.New("object exists")
	errHashMismatch        = errors.New("hash mismatch")
	errClientAborted       = errors.New("client aborted request")
	errLengthRequired      = errors.New("content length required")
	errRangeNotSatisfiable = errors.New("requested range not satisfiable")
)

var (
	hexBlobIDRegex          = regexp.MustCompile(`^[0-9a-f]{64}$`)
	stripedBlobIDRegex      = regexp.MustCompile(`^[0-9a-f]{64}\.[0-9a-f]{16}$`)
	firstStripedBlobIDRegex = regexp.MustCompile(`^[0-9a-f]{64}\.0000000000000000$`)
)

type Handler struct {
	connMgr         *ConnectionManager
	repo            string
	dynamic         bool
	access          Access
	readBufferPool  *BufferPool
	writeBufferPool *BufferPool
}

type repoNameContextKey struct{}

func withRepoName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, repoNameContextKey{}, name)
}

func (h *Handler) repoName(ctx context.Context) string {
	if h.dynamic {
		if name, ok := ctx.Value(repoNameContextKey{}).(string); ok {
			return name
		}
	}
	return h.repo
}

type HandlerContext struct {
	conn            *connHandle
	ioctx           *rados.IOContext
	prefix          string
	radosIO         RadosIOContext
	striperIO       RadosIOContext
	lowerIoctx      *rados.IOContext
	lowerPrefix     string
	lowerRadosIO    RadosIOContext
	lowerStriperIO  RadosIOContext
	stripedWrites   bool
	maxObjectSize   int64
	radosCalls      *uint64
	readBufferPool  *BufferPool
	readBufPtr      *[]byte
	writeBufferPool *BufferPool
	writeBufPtr     *[]byte
}

type responseWriter struct {
	http.ResponseWriter
	statusCode    int
	bytesWritten  int64
	headerWritten bool
}

type errorCoder interface {
	ErrorCode() int
}

type blobInfo struct {
	Name string `json:"name"`
	Size uint64 `json:"size"`
}

type httpRange struct {
	start  int64
	end    int64
	status int
}

func (hctx *HandlerContext) Destroy() {
	hctx.ioctx.Destroy()
	if hctx.lowerIoctx != nil {
		hctx.lowerIoctx.Destroy()
	}
	if hctx.conn != nil {
		hctx.conn.release()
	}
	if hctx.readBufPtr != nil {
		hctx.readBufferPool.Put(hctx.readBufPtr)
	}
	if hctx.writeBufPtr != nil {
		hctx.writeBufferPool.Put(hctx.writeBufPtr)
	}
}

func statInLayer(plainIO, striperIO RadosIOContext, object string) (RadosIOContext, StatInfo, error) {
	if striperIO != nil {
		stat, err := plainIO.Stat(object)
		if !errors.Is(err, rados.ErrNotFound) {
			return plainIO, stat, err
		}
		_, stripeErr := plainIO.Stat(object + firstStripeSuffix)
		if stripeErr == nil {
			stat, err = striperIO.Stat(object)
			return striperIO, stat, err
		}
		if !errors.Is(stripeErr, rados.ErrNotFound) {
			return plainIO, StatInfo{}, stripeErr
		}
		return plainIO, StatInfo{}, err
	}
	stat, err := plainIO.Stat(object)
	return plainIO, stat, err
}

func (hctx *HandlerContext) statRadosObject(object string) (RadosIOContext, StatInfo, error) {
	rioctx, stat, err := statInLayer(hctx.radosIO, hctx.striperIO, object)
	if errors.Is(err, rados.ErrNotFound) && hctx.lowerRadosIO != nil {
		return statInLayer(hctx.lowerRadosIO, hctx.lowerStriperIO, object)
	}
	return rioctx, stat, err
}

func (hctx *HandlerContext) removeRadosObject(object string, canStripe bool) error {
	type layer struct {
		name      string
		plainIO   RadosIOContext
		striperIO RadosIOContext
	}
	var layers []layer
	if hctx.lowerRadosIO != nil {
		layers = append(layers, layer{"lower", hctx.lowerRadosIO, hctx.lowerStriperIO})
	}
	layers = append(layers, layer{"upper", hctx.radosIO, hctx.striperIO})

	for _, l := range layers {
		striperIO := l.striperIO
		if !canStripe {
			striperIO = nil
		}
		rioctx, _, err := statInLayer(l.plainIO, striperIO, object)
		if errors.Is(err, rados.ErrNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("stat object %s: %w", object, err)
		}
		slog.Debug("removing object from layer", "object", object, "layer", l.name)
		if err := rioctx.Remove(object); err != nil && !errors.Is(err, rados.ErrNotFound) {
			return fmt.Errorf("delete object %s: %w", object, err)
		}
	}
	return nil
}

func (rw *responseWriter) WriteHeader(code int) {
	if rw.headerWritten {
		return
	}
	rw.statusCode = code
	rw.headerWritten = true
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.headerWritten {
		rw.statusCode = http.StatusOK
		rw.headerWritten = true
	}
	n, err := rw.ResponseWriter.Write(b)
	rw.bytesWritten += int64(n)
	return n, err
}

func logRequest(repo, method, path string, status int, duration time.Duration, reqBytes, respBytes int64, radosCalls uint64) {
	attrs := []any{
		"method", method,
		"path", path,
		"status", status,
		"duration", duration.Round(time.Millisecond).String(),
		"req_bytes", reqBytes,
		"resp_bytes", respBytes,
		"rados_calls", radosCalls,
	}
	if repo != "" && repo != "default" {
		attrs = append(attrs, "repo", repo)
	}
	slog.Info("request", attrs...)
}

type radosCallsKey struct{}

func radosCallCounter(ctx context.Context) *uint64 {
	if counter, ok := ctx.Value(radosCallsKey{}).(*uint64); ok {
		return counter
	}
	return new(uint64)
}

func (h *Handler) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		slog.Debug("request-start", "method", r.Method, "path", r.URL.Path)
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		var radosCalls uint64
		ctx := context.WithValue(r.Context(), radosCallsKey{}, &radosCalls)
		defer func() {
			logRequest(h.repoName(r.Context()), r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten, atomic.LoadUint64(&radosCalls))
		}()
		next.ServeHTTP(rw, r.WithContext(ctx))
	})
}

func (h *Handler) openIOContext(ctx context.Context, blobType BlobType) (*HandlerContext, error) {
	radosCalls := radosCallCounter(ctx)
	ioctx, lowerIoctx, conn, bp, err := h.connMgr.GetIOContextForRepo(h.repoName(ctx), blobType, radosCalls)
	if err != nil {
		return nil, err
	}

	readBufPtr := h.readBufferPool.Get()
	writeBufPtr := h.writeBufferPool.Get()

	hctx := &HandlerContext{
		conn:            conn,
		ioctx:           ioctx,
		prefix:          bp.Prefix,
		lowerIoctx:      lowerIoctx,
		stripedWrites:   bp.Striped,
		maxObjectSize:   bp.MaxObjectSize,
		radosCalls:      radosCalls,
		readBufferPool:  h.readBufferPool,
		readBufPtr:      readBufPtr,
		writeBufferPool: h.writeBufferPool,
		writeBufPtr:     writeBufPtr,
	}

	hctx.radosIO = NewRadosIO(ioctx, bp.Prefix, bp.Alignment, *readBufPtr, *writeBufPtr, radosCalls)
	hctx.striperIO = NewStripedIO(ioctx, bp.Prefix, uint64(bp.MaxObjectSize), bp.Alignment, *readBufPtr, *writeBufPtr, radosCalls)

	if lowerIoctx != nil {
		hctx.lowerPrefix = bp.Lower.Prefix
		hctx.lowerRadosIO = NewRadosIO(lowerIoctx, bp.Lower.Prefix, bp.Lower.Alignment, *readBufPtr, *writeBufPtr, radosCalls)
		hctx.lowerStriperIO = NewStripedIO(lowerIoctx, bp.Lower.Prefix, uint64(bp.Lower.MaxObjectSize), bp.Lower.Alignment, *readBufPtr, *writeBufPtr, radosCalls)
	}

	return hctx, nil
}

func writeInfraError(w http.ResponseWriter, err error, logMsg string) {
	switch {
	case errors.Is(err, errConnectionUnavailable):
		http.Error(w, "ceph cluster unavailable", http.StatusServiceUnavailable)
	case errors.Is(err, errPoolNotConfigured):
		http.Error(w, "pool not configured", http.StatusServiceUnavailable)
	default:
		slog.Error(logMsg, "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func (h *Handler) openHTTPIOContext(w http.ResponseWriter, r *http.Request, blobType BlobType) (*HandlerContext, bool) {
	hctx, err := h.openIOContext(r.Context(), blobType)
	if err != nil {
		writeInfraError(w, err, "failed to open IO context")
		return nil, false
	}
	return hctx, true
}

func isValidBlobType(blobType string) bool {
	switch blobType {
	case "keys", "locks", "snapshots", "data", "index":
		return true
	default:
		return false
	}
}

func canStripeBlobType(blobType string) bool {
	switch blobType {
	case "snapshots", "data", "index":
		return true
	default:
		return false
	}
}

func (h *Handler) handleRadosError(w http.ResponseWriter, r *http.Request, object string, err error) {
	var ec errorCoder
	if errors.As(err, &ec) {
		switch ec.ErrorCode() {
		case -int(syscall.EFBIG):
			http.Error(w, "object size exceeds cluster limit", http.StatusRequestEntityTooLarge)
			return
		case -int(syscall.EMSGSIZE):
			http.Error(w, "write chunk exceeds message limit", http.StatusRequestEntityTooLarge)
			return
		case -int(syscall.EOPNOTSUPP):
			slog.Error("operation not supported", "object", object, "error", err)
			http.Error(w, "operation not supported", http.StatusInternalServerError)
			return
		case -int(syscall.ENOSPC):
			slog.Error("insufficient storage", "object", object, "error", err)
			http.Error(w, "insufficient storage", http.StatusInsufficientStorage)
			return
		case -int(syscall.EDQUOT):
			slog.Error("disk quota exceeded", "object", object, "error", err)
			http.Error(w, "insufficient storage", http.StatusInsufficientStorage)
			return
		}
	}

	switch {
	case errors.Is(err, errConnectionUnavailable):
		http.Error(w, "ceph cluster unavailable", http.StatusServiceUnavailable)
	case errors.Is(err, errObjectNotFound):
		http.NotFound(w, r)
	case errors.Is(err, errRangeNotSatisfiable):
		http.Error(w, "requested range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
	case errors.Is(err, errObjectExists):
		http.Error(w, "object already exists", http.StatusForbidden)
	case errors.Is(err, errHashMismatch):
		http.Error(w, "hash mismatch", http.StatusBadRequest)
	case errors.Is(err, errClientAborted):
		http.Error(w, "client aborted request", http.StatusBadRequest)
	case errors.Is(err, errLengthRequired):
		http.Error(w, "content length required", http.StatusLengthRequired)
	default:
		slog.Error("failed to serve object", "object", object, "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func (h *Handler) getConfig(w http.ResponseWriter, r *http.Request) {
	hctx, ok := h.openHTTPIOContext(w, r, BlobTypeConfig)
	if !ok {
		return
	}
	defer hctx.Destroy()

	if err := hctx.serveRadosObject(w, r, configObjectName); err != nil {
		h.handleRadosError(w, r, configObjectName, err)
	}
}

func (h *Handler) createConfig(w http.ResponseWriter, r *http.Request) {
	hctx, ok := h.openHTTPIOContext(w, r, BlobTypeConfig)
	if !ok {
		return
	}
	defer hctx.Destroy()

	if err := hctx.createRadosObject(w, r, configObjectName, configObjectName, false); err != nil {
		h.handleRadosError(w, r, configObjectName, err)
	}
}

func (h *Handler) deleteConfig(w http.ResponseWriter, r *http.Request) {
	hctx, ok := h.openHTTPIOContext(w, r, BlobTypeConfig)
	if !ok {
		return
	}
	defer hctx.Destroy()

	if err := hctx.removeRadosObject(configObjectName, false); err != nil {
		h.handleRadosError(w, r, configObjectName, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) createRepo(w http.ResponseWriter, r *http.Request) {
	createParam := r.URL.Query().Get("create")
	if createParam == "" {
		http.Error(w, "missing required query parameter: create", http.StatusBadRequest)
		return
	}
	if createParam != "true" {
		http.Error(w, "invalid value for create parameter: must be 'true'", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

type purgeTarget struct {
	pool         string
	namespace    string
	layer        string
	layerRank    int
	prefixes     []string
	exact        []string
	snapPrefixes []string
	lockPrefixes []string
}

var (
	errRepoHasSnapshots = errors.New("repository contains snapshot objects")
	errRepoLocked       = errors.New("repository is locked")
)

const purgeDeleteWorkers = 8

func hasAnyPrefix(name string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func matchesExactOrStripedObject(name string, exact []string) bool {
	for _, object := range exact {
		if name == object {
			return true
		}
		if len(name) != len(object)+stripeSuffixLen || !strings.HasPrefix(name, object) {
			continue
		}
		suffix := name[len(object):]
		if suffix[0] != '.' {
			continue
		}
		valid := true
		for _, c := range suffix[1:] {
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				valid = false
				break
			}
		}
		if valid {
			return true
		}
	}
	return false
}

func (h *Handler) purgeTargets(repo string) ([]*purgeTarget, error) {
	type targetKey struct {
		layer     string
		pool      string
		namespace string
	}
	targets := make(map[targetKey]*purgeTarget)
	add := func(layer string, rank int, bp *BlobPool, bt BlobType) {
		key := targetKey{layer, bp.Pool, bp.Namespace}
		t := targets[key]
		if t == nil {
			t = &purgeTarget{pool: bp.Pool, namespace: bp.Namespace, layer: layer, layerRank: rank}
			targets[key] = t
		}
		if bt == BlobTypeConfig {
			t.exact = append(t.exact, bp.Prefix+configObjectName)
			return
		}
		prefix := bp.Prefix + string(bt) + "/"
		t.prefixes = append(t.prefixes, prefix)
		if bt == BlobTypeSnapshots {
			t.snapPrefixes = append(t.snapPrefixes, prefix)
		}
		if bt == BlobTypeLocks {
			t.lockPrefixes = append(t.lockPrefixes, prefix)
		}
	}
	var missingErr error
	for _, bt := range AllBlobTypes {
		bp, err := h.connMgr.GetBlobPoolForRepo(repo, bt)
		if err != nil {
			if errors.Is(err, errPoolNotConfigured) {
				if missingErr == nil {
					missingErr = err
				}
				continue
			}
			return nil, err
		}
		add("upper", 1, bp, bt)
		if bp.Lower != nil {
			add("lower", 0, bp.Lower, bt)
		}
	}
	if len(targets) == 0 {
		return nil, missingErr
	}
	ordered := slices.SortedFunc(maps.Values(targets), func(a, b *purgeTarget) int {
		if c := cmp.Compare(a.layerRank, b.layerRank); c != 0 {
			return c
		}
		if c := cmp.Compare(a.pool, b.pool); c != 0 {
			return c
		}
		return cmp.Compare(a.namespace, b.namespace)
	})
	for _, t := range ordered {
		slices.Sort(t.prefixes)
		slices.Sort(t.exact)
	}
	return ordered, nil
}

func (h *Handler) withPurgeTarget(t *purgeTarget, radosCalls *uint64, fn func(*rados.IOContext) error) error {
	ioctx, conn, err := h.connMgr.OpenNamespaceContext(t.pool, t.namespace, radosCalls)
	if err != nil {
		return err
	}
	defer func() {
		ioctx.Destroy()
		conn.release()
	}()
	return fn(ioctx)
}

func iterateNamespace(ioctx *rados.IOContext, radosCalls *uint64, fn func(string) error) error {
	slog.Debug("rados.Iter")
	atomic.AddUint64(radosCalls, 1)
	iter, err := ioctx.Iter()
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()
	for iter.Next() {
		name := iter.Value()
		if name == "" {
			continue
		}
		if err := fn(name); err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterate objects: %w", err)
	}
	return nil
}

func (h *Handler) checkPurgeGate(targets []*purgeTarget, radosCalls *uint64) error {
	for _, t := range targets {
		if len(t.snapPrefixes) == 0 && len(t.lockPrefixes) == 0 {
			continue
		}
		err := h.withPurgeTarget(t, radosCalls, func(ioctx *rados.IOContext) error {
			return iterateNamespace(ioctx, radosCalls, func(name string) error {
				if hasAnyPrefix(name, t.snapPrefixes) {
					return errRepoHasSnapshots
				}
				if hasAnyPrefix(name, t.lockPrefixes) {
					return errRepoLocked
				}
				return nil
			})
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) purgeTargetObjects(ctx context.Context, t *purgeTarget, radosCalls *uint64) (int, error) {
	var deleted atomic.Uint64
	foreign := 0
	err := h.withPurgeTarget(t, radosCalls, func(ioctx *rados.IOContext) error {
		names := make(chan string, purgeDeleteWorkers)
		errs := make(chan error, purgeDeleteWorkers)
		var stop atomic.Bool
		var wg sync.WaitGroup
		for range purgeDeleteWorkers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for name := range names {
					if stop.Load() {
						continue
					}
					slog.Debug("purging object", "object", name, "pool", t.pool, "namespace", t.namespace, "layer", t.layer)
					atomic.AddUint64(radosCalls, 1)
					if err := ioctx.Delete(name); err != nil && !errors.Is(err, rados.ErrNotFound) {
						stop.Store(true)
						select {
						case errs <- fmt.Errorf("delete object %s: %w", name, err):
						default:
						}
						continue
					}
					deleted.Add(1)
				}
			}()
		}
		iterErr := iterateNamespace(ioctx, radosCalls, func(name string) error {
			if stop.Load() {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if hasAnyPrefix(name, t.prefixes) || matchesExactOrStripedObject(name, t.exact) {
				names <- name
			} else {
				foreign++
			}
			return nil
		})
		close(names)
		wg.Wait()
		select {
		case err := <-errs:
			return err
		default:
		}
		return iterErr
	})
	if foreign > 0 {
		slog.Debug("leaving foreign objects", "pool", t.pool, "namespace", t.namespace, "count", foreign)
	}
	return int(deleted.Load()), err
}

func (h *Handler) purgeRepo(w http.ResponseWriter, r *http.Request) {
	purgeParam := r.URL.Query().Get("purge")
	if purgeParam == "" {
		http.Error(w, "missing required query parameter: purge", http.StatusBadRequest)
		return
	}
	if purgeParam != "true" {
		http.Error(w, "invalid value for purge parameter: must be 'true'", http.StatusBadRequest)
		return
	}

	repo := h.repoName(r.Context())
	radosCalls := radosCallCounter(r.Context())

	targets, err := h.purgeTargets(repo)
	if err != nil {
		writeInfraError(w, err, "failed to purge repository")
		return
	}

	if err := h.checkPurgeGate(targets, radosCalls); err != nil {
		if errors.Is(err, errRepoHasSnapshots) || errors.Is(err, errRepoLocked) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeInfraError(w, err, "failed to purge repository")
		return
	}

	total := 0
	for _, t := range targets {
		deleted, err := h.purgeTargetObjects(r.Context(), t, radosCalls)
		if err != nil {
			writeInfraError(w, err, "failed to purge repository")
			return
		}
		total += deleted
	}

	slog.Info("purged repository", "repo", repo, "objects", total)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) listBlobs(w http.ResponseWriter, r *http.Request) {
	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) || r.URL.Path != "/"+blobType+"/" {
		http.NotFound(w, r)
		return
	}

	hctx, ok := h.openHTTPIOContext(w, r, BlobType(blobType))
	if !ok {
		return
	}
	defer hctx.Destroy()

	useV2 := acceptsBlobListV2(r)
	logicalPrefix := blobType + "/"

	blobNames := []string{}
	blobInfos := []blobInfo{}

	type listSource struct {
		ioctx  *rados.IOContext
		prefix string
	}
	sources := []listSource{{hctx.ioctx, hctx.prefix}}
	var seen map[string]struct{}
	if hctx.lowerIoctx != nil {
		sources = append(sources, listSource{hctx.lowerIoctx, hctx.lowerPrefix})
		seen = make(map[string]struct{})
	}

	for _, src := range sources {
		err := hctx.collectBlobs(src.ioctx, src.prefix+logicalPrefix, logicalPrefix, useV2, seen, &blobNames, &blobInfos)
		if err != nil {
			slog.Error("failed to list blobs", "type", blobType, "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	}

	var data []byte
	var err error
	if useV2 {
		data, err = json.Marshal(blobInfos)
		if err != nil {
			slog.Error("failed to list blobs", "type", blobType, "error", fmt.Errorf("marshal JSON: %w", err))
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.x.restic.rest.v2")
	} else {
		data, err = json.Marshal(blobNames)
		if err != nil {
			slog.Error("failed to list blobs", "type", blobType, "error", fmt.Errorf("marshal JSON: %w", err))
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.x.restic.rest.v1")
	}

	w.WriteHeader(http.StatusOK)
	if _, err = w.Write(data); err != nil {
		slog.Warn("failed to list blobs", "type", blobType, "error", err)
	}
}

func (hctx *HandlerContext) collectBlobs(src *rados.IOContext, storagePrefix, logicalPrefix string, useV2 bool, seen map[string]struct{}, blobNames *[]string, blobInfos *[]blobInfo) error {
	slog.Debug("rados.Iter")
	atomic.AddUint64(hctx.radosCalls, 1)
	iter, err := src.Iter()
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	for iter.Next() {
		objectName := iter.Value()
		if objectName == "" || !strings.HasPrefix(objectName, storagePrefix) {
			continue
		}

		blobID := strings.TrimPrefix(objectName, storagePrefix)

		if stripedBlobIDRegex.MatchString(blobID) && !firstStripedBlobIDRegex.MatchString(blobID) {
			continue
		}

		if firstStripedBlobIDRegex.MatchString(blobID) {
			blobID = blobID[:len(blobID)-stripeSuffixLen]
		}

		if !hexBlobIDRegex.MatchString(blobID) {
			slog.Warn("skipping unknown object", "object", objectName)
			continue
		}

		if seen != nil {
			if _, ok := seen[blobID]; ok {
				continue
			}
			seen[blobID] = struct{}{}
		}

		baseObjectName := logicalPrefix + blobID

		if useV2 {
			_, stat, err := hctx.statRadosObject(baseObjectName)
			if errors.Is(err, errUnsupportedStriperLayout) {
				slog.Warn("skipping striped object with unsupported layout", "object", baseObjectName, "error", err)
				continue
			}
			if err != nil {
				return fmt.Errorf("stat %s: %w", baseObjectName, err)
			}
			*blobInfos = append(*blobInfos, blobInfo{
				Name: blobID,
				Size: stat.Size,
			})
		} else {
			*blobNames = append(*blobNames, blobID)
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterate objects: %w", err)
	}

	return nil
}

func (h *Handler) getBlob(w http.ResponseWriter, r *http.Request) {
	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(w, r)
		return
	}

	blobID := r.PathValue("id")
	if !hexBlobIDRegex.MatchString(blobID) {
		http.NotFound(w, r)
		return
	}

	hctx, ok := h.openHTTPIOContext(w, r, BlobType(blobType))
	if !ok {
		return
	}
	defer hctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := hctx.serveRadosObject(w, r, objectName); err != nil {
		h.handleRadosError(w, r, blobID, err)
	}
}

func (h *Handler) createBlob(w http.ResponseWriter, r *http.Request) {
	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(w, r)
		return
	}

	blobID := r.PathValue("id")
	if !hexBlobIDRegex.MatchString(blobID) {
		http.NotFound(w, r)
		return
	}

	hctx, ok := h.openHTTPIOContext(w, r, BlobType(blobType))
	if !ok {
		return
	}
	defer hctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := hctx.createRadosObject(w, r, objectName, blobID, canStripeBlobType(blobType)); err != nil {
		h.handleRadosError(w, r, blobID, err)
	}
}

func (h *Handler) deleteBlob(w http.ResponseWriter, r *http.Request) {
	blobType := r.PathValue("type")
	if !isValidBlobType(blobType) {
		http.NotFound(w, r)
		return
	}

	blobID := r.PathValue("id")
	if !hexBlobIDRegex.MatchString(blobID) {
		http.NotFound(w, r)
		return
	}

	minAccess := AccessReadWrite
	if blobType == "locks" {
		minAccess = AccessReadAppend
	}
	if min(h.access, grantForRepo(r.Context(), h.repoName(r.Context()))) < minAccess {
		http.Error(w, "access denied", http.StatusForbidden)
		return
	}

	hctx, ok := h.openHTTPIOContext(w, r, BlobType(blobType))
	if !ok {
		return
	}
	defer hctx.Destroy()

	objectName := blobType + "/" + blobID

	if err := hctx.removeRadosObject(objectName, true); err != nil {
		h.handleRadosError(w, r, blobID, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func acceptsBlobListV2(r *http.Request) bool {
	for _, value := range r.Header.Values("Accept") {
		for _, mediaRange := range strings.Split(value, ",") {
			mediaRange = strings.TrimSpace(mediaRange)
			if mediaRange == "" {
				continue
			}
			mediaType, params, err := mime.ParseMediaType(mediaRange)
			if err != nil {
				continue
			}
			if mediaType != "application/vnd.x.restic.rest.v2" {
				continue
			}
			if qValue, ok := params["q"]; ok {
				q, err := strconv.ParseFloat(qValue, 64)
				if err == nil && q == 0 {
					continue
				}
			}
			return true
		}
	}
	return false
}

func (h *Handler) requireAccess(minAccess Access, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if min(h.access, grantForRepo(r.Context(), h.repoName(r.Context()))) < minAccess {
			http.Error(w, "access denied", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func (h *Handler) setupRoutes(mux *http.ServeMux) {
	mux.HandleFunc("HEAD /config", h.requireAccess(AccessRead, h.getConfig))
	mux.HandleFunc("GET /config", h.requireAccess(AccessRead, h.getConfig))
	mux.HandleFunc("POST /config", h.requireAccess(AccessReadAppend, h.createConfig))
	mux.HandleFunc("DELETE /config", h.requireAccess(AccessReadWrite, h.deleteConfig))

	mux.HandleFunc("GET /{type}/", h.requireAccess(AccessRead, h.listBlobs))
	mux.HandleFunc("HEAD /{type}/{id}", h.requireAccess(AccessRead, h.getBlob))
	mux.HandleFunc("GET /{type}/{id}", h.requireAccess(AccessRead, h.getBlob))
	mux.HandleFunc("POST /{type}/{id}", h.requireAccess(AccessReadAppend, h.createBlob))
	mux.HandleFunc("DELETE /{type}/{id}", h.deleteBlob)

	mux.HandleFunc("POST /{$}", h.requireAccess(AccessReadAppend, h.createRepo))
	mux.HandleFunc("DELETE /{$}", h.requireAccess(AccessReadWrite, h.purgeRepo))
}

type dynamicRepoDispatcher struct {
	fallback http.Handler
	patterns []repoPattern
	handlers map[string]http.Handler
	static   map[string]*RepoConfig
}

func rejectEncodedPathSeparators(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		escaped := r.URL.EscapedPath()
		for i := 0; i+2 < len(escaped); i++ {
			if escaped[i] == '%' && escaped[i+1] == '2' && (escaped[i+2] == 'f' || escaped[i+2] == 'F') {
				http.NotFound(w, r)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (d *dynamicRepoDispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	seg, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	_, static := d.static[seg]
	if !static && seg != "default" && !isReservedRepoName(seg) && isValidRepoName(seg) {
		for _, p := range d.patterns {
			if _, ok := p.match(seg); ok {
				if r.URL.Path == "/"+seg {
					u := &url.URL{Path: r.URL.Path + "/", RawQuery: r.URL.RawQuery}
					http.Redirect(w, r, u.String(), http.StatusMovedPermanently)
					return
				}
				r = r.WithContext(withRepoName(r.Context(), seg))
				http.StripPrefix("/"+seg, d.handlers[p.key]).ServeHTTP(w, r)
				return
			}
		}
	}
	if d.fallback != nil {
		d.fallback.ServeHTTP(w, r)
		return
	}
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	http.NotFound(rw, r)
	logRequest("", r.Method, r.URL.Path, rw.statusCode, time.Since(start), r.ContentLength, rw.bytesWritten, 0)
}

func setupAllRoutes(mux *http.ServeMux, connMgr *ConnectionManager, repos map[string]*RepoConfig, readPool, writePool *BufferPool) {
	routes := http.NewServeMux()
	var defaultHandler http.Handler
	patternHandlers := make(map[string]http.Handler)
	for name, repo := range repos {
		h := &Handler{
			connMgr:         connMgr,
			repo:            name,
			dynamic:         strings.Contains(name, "*"),
			access:          ParseAccess(repo.Access),
			readBufferPool:  readPool,
			writeBufferPool: writePool,
		}
		repoMux := http.NewServeMux()
		h.setupRoutes(repoMux)
		switch {
		case h.dynamic:
			patternHandlers[name] = h.logRequests(repoMux)
		case name == "default":
			defaultHandler = h.logRequests(repoMux)
		default:
			routes.Handle("/"+name+"/", http.StripPrefix("/"+name, h.logRequests(repoMux)))
		}
	}
	switch {
	case len(patternHandlers) > 0:
		routes.Handle("/", &dynamicRepoDispatcher{
			fallback: defaultHandler,
			patterns: compileRepoPatterns(repos),
			handlers: patternHandlers,
			static:   repos,
		})
	case defaultHandler != nil:
		routes.Handle("/", defaultHandler)
	}

	routes.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	routes.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !connMgr.Ready() {
			http.Error(w, "ceph cluster unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.Handle("/", rejectEncodedPathSeparators(routes))
}

func parseExpectedHash(object string) ([32]byte, error) {
	if object == configObjectName {
		return [32]byte{}, nil
	}

	hashBytes, err := hex.DecodeString(object)
	if err != nil {
		return [32]byte{}, fmt.Errorf("invalid hash format: %w", err)
	}
	if len(hashBytes) != 32 {
		return [32]byte{}, fmt.Errorf("invalid hash length: expected 32 bytes, got %d", len(hashBytes))
	}

	return [32]byte(hashBytes), nil
}

func parseRange(r *http.Request, size int64) (*httpRange, error) {
	if size == 0 {
		return &httpRange{start: 0, end: -1, status: http.StatusOK}, nil
	}

	if r == nil {
		return &httpRange{start: 0, end: size - 1, status: http.StatusOK}, nil
	}

	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		return &httpRange{start: 0, end: size - 1, status: http.StatusOK}, nil
	}

	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return nil, fmt.Errorf("unsupported range unit in: %s", rangeHeader)
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")

	if strings.Contains(rangeSpec, ",") {
		return nil, fmt.Errorf("multiple ranges not supported: %s", rangeHeader)
	}

	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid range format: %s", rangeHeader)
	}

	if parts[0] == "" && parts[1] == "" {
		return nil, fmt.Errorf("empty range spec: %s", rangeHeader)
	}

	var start, end int64

	if parts[0] == "" {
		suffixLength, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffixLength < 0 {
			return nil, fmt.Errorf("invalid suffix length in range: %s", rangeHeader)
		}
		if suffixLength == 0 {
			return nil, fmt.Errorf("zero-length suffix range: %s", rangeHeader)
		}
		if suffixLength >= size {
			start = 0
		} else {
			start = size - suffixLength
		}
		end = size - 1
	} else {
		rangeStart, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || rangeStart < 0 {
			return nil, fmt.Errorf("invalid range start: %w", err)
		}

		if rangeStart >= size {
			return nil, fmt.Errorf("range start %d out of bounds for size %d", rangeStart, size)
		}

		start = rangeStart

		if parts[1] != "" {
			rangeEnd, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil || rangeEnd < 0 {
				return nil, fmt.Errorf("invalid range end: %w", err)
			}
			if rangeEnd >= size {
				rangeEnd = size - 1
			}
			end = rangeEnd
		} else {
			end = size - 1
		}

		if start > end {
			return nil, fmt.Errorf("range start %d greater than end %d", start, end)
		}
	}

	return &httpRange{start: start, end: end, status: http.StatusPartialContent}, nil
}

func (hctx *HandlerContext) serveRadosObject(w http.ResponseWriter, r *http.Request, object string) error {
	rioctx, stat, err := hctx.statRadosObject(object)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return errObjectNotFound
		}
		return fmt.Errorf("stat %s: %w", object, err)
	}

	striped := (hctx.striperIO != nil && rioctx == hctx.striperIO) ||
		(hctx.lowerStriperIO != nil && rioctx == hctx.lowerStriperIO)
	slog.Debug("reading blob", "object", object, "size", stat.Size, "striped", striped)

	if stat.Size > uint64(math.MaxInt64) {
		return fmt.Errorf("object %s size exceeds max int64: %d", object, stat.Size)
	}

	rng, err := parseRange(r, int64(stat.Size))
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", stat.Size))
		return fmt.Errorf("%w: %v", errRangeNotSatisfiable, err)
	}

	if rng.status == http.StatusPartialContent {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, stat.Size))
	}

	contentLength := rng.end - rng.start + 1
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.WriteHeader(rng.status)

	if r.Method == "HEAD" || contentLength == 0 {
		return nil
	}

	_, sum, err := rioctx.ReadObject(object, rng.start, contentLength, w)
	if err != nil {
		return fmt.Errorf("read %s: %w", object, err)
	}

	if rng.start == 0 && contentLength == int64(stat.Size) {
		hashID := object[strings.LastIndex(object, "/")+1:]
		expected, parseErr := parseExpectedHash(hashID)
		if parseErr == nil && expected != [32]byte{} && sum != expected {
			slog.Warn("hash mismatch on read",
				"object", object,
				"expected", hex.EncodeToString(expected[:]),
				"actual", hex.EncodeToString(sum[:]))
		}
	}

	return nil
}

func (hctx *HandlerContext) createRadosObject(w http.ResponseWriter, r *http.Request, object string, hashID string, canStripe bool) error {
	size := r.ContentLength
	if size < 0 && canStripe && hctx.stripedWrites {
		return errLengthRequired
	}
	useStriper := canStripe && hctx.stripedWrites && size > hctx.maxObjectSize

	expected, err := parseExpectedHash(hashID)
	if err != nil {
		return err
	}

	_, _, err = hctx.statRadosObject(object)
	if err == nil {
		return errObjectExists
	}
	if !errors.Is(err, rados.ErrNotFound) {
		return fmt.Errorf("stat object %s: %w", object, err)
	}

	var rioctx RadosIOContext
	if useStriper {
		rioctx = hctx.striperIO
	} else {
		rioctx = hctx.radosIO
	}

	_, sum, err := rioctx.WriteObject(object, r.Body)
	if err != nil {
		if errors.Is(err, errObjectExists) {
			return errObjectExists
		}
		if rmErr := rioctx.Remove(object); rmErr != nil && !errors.Is(rmErr, rados.ErrNotFound) {
			slog.Error("failed to clean up object after write error; a truncated object may remain",
				"object", object, "write_error", err, "cleanup_error", rmErr)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, io.ErrUnexpectedEOF) || r.Context().Err() != nil {
			return errClientAborted
		}
		return fmt.Errorf("write object %s: %w", object, err)
	}

	slog.Debug("created blob", "object", object, "size", size, "striped", useStriper)

	if expected != [32]byte{} && sum != expected {
		slog.Warn("input hash mismatch", "object", object, "expected", fmt.Sprintf("%x", expected), "got", fmt.Sprintf("%x", sum))
		if rmErr := rioctx.Remove(object); rmErr != nil && !errors.Is(rmErr, rados.ErrNotFound) {
			slog.Error("failed to clean up object after hash mismatch; a corrupt object may remain",
				"object", object, "cleanup_error", rmErr)
		}
		return errHashMismatch
	}

	w.WriteHeader(http.StatusOK)
	return nil
}
