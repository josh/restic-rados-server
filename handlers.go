package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ceph/go-ceph/rados"
)

const (
	mimeTypeAPIV1 = "application/vnd.x.restic.rest.v1"
	mimeTypeAPIV2 = "application/vnd.x.restic.rest.v2"

	purgeDeleteWorkers = 8
)

var (
	errClientAborted     = errors.New("client aborted request")
	errHashMismatch      = errors.New("hash mismatch")
	errContentTooLarge   = errors.New("content too large")
	errLengthRequired    = errors.New("content length required")
	errInvalidRange      = errors.New("invalid range")
	errRepositoryLocked  = errors.New("repository is locked")
	errRepositoryHasSnap = errors.New("repository contains snapshot objects")
)

type Handler struct {
	connections *ConnectionManager
	repos       map[string]*RepoConfig
	patterns    []repoPattern
	access      Access
	readPool    *BufferPool
	writePool   *BufferPool
}

type responseWriter struct {
	http.ResponseWriter
	statusCode    int
	bytesWritten  int64
	headerWritten bool
}

func (w *responseWriter) WriteHeader(code int) {
	if w.headerWritten {
		return
	}
	w.statusCode = code
	w.headerWritten = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriter) Write(data []byte) (int, error) {
	if !w.headerWritten {
		w.statusCode = http.StatusOK
		w.headerWritten = true
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytesWritten += int64(n)
	return n, err
}

func (w *responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *responseWriter) FlushError() error {
	if !w.headerWritten {
		w.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

type repositoryRoute struct {
	name         string
	config       *RepoConfig
	resourcePath string
	patternMatch bool
}

type HandlerContext struct {
	layers []storageLayer
	handle *connHandle
}

type storageLayer struct {
	name   string
	ioctx  *rados.IOContext
	config *BlobPool
}

type storedObject struct {
	backend RadosIOContext
	stat    StatInfo
	striped bool
}

type byteRange struct {
	start   int64
	length  int64
	partial bool
}

type blobRouteKind uint8

const (
	invalidBlobRoute blobRouteKind = iota
	blobRouteRedirect
	blobRouteList
	blobRouteObject
)

type blobRoute struct {
	kind     blobRouteKind
	blobType BlobType
	objectID string
}

type physicalBlobKind uint8

const (
	invalidPhysicalBlob physicalBlobKind = iota
	plainBlob
	firstStripe
	continuationStripe
)

type blobRepresentations struct {
	plain   bool
	striped bool
}

type listedBlob struct {
	Name string `json:"name"`
	Size uint64 `json:"size"`
}

type purgeScope struct {
	layer     string
	pool      string
	namespace string
	prefix    string
	types     map[BlobType]bool
}

type purgeTarget struct {
	pool      string
	namespace string
	scopes    []*purgeScope
}

type purgeScopeKey struct {
	layer     string
	pool      string
	namespace string
	prefix    string
}

type purgeTargetKey struct {
	pool      string
	namespace string
}

func setupAllRoutes(mux *http.ServeMux, connections *ConnectionManager, repos map[string]*RepoConfig, access Access, readPool, writePool *BufferPool) {
	mux.Handle("/", &Handler{
		connections: connections,
		repos:       repos,
		patterns:    compileRepoPatterns(repos),
		access:      access,
		readPool:    readPool,
		writePool:   writePool,
	})
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path, ok := requestRoutePath(r)
	if !ok {
		http.NotFound(w, r)
		return
	}

	switch path {
	case "/healthz":
		h.serveHealth(w, r)
		return
	case "/readyz":
		h.serveReady(w, r)
		return
	}

	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	var radosCalls uint64
	h.serveRequest(rw, r, path, &radosCalls)
}

func (h *Handler) serveRequest(w *responseWriter, r *http.Request, path string, radosCalls *uint64) {
	started := time.Now()
	repoName := ""
	logPath := r.URL.Path

	repo, ok := h.resolveRepository(path)
	if ok {
		repoName = repo.name
		logPath = repo.resourcePath
		if logPath == "" {
			logPath = "/"
		}
	}

	slog.Debug("request-start", "method", r.Method, "path", logPath)
	defer func() {
		logRequest(repoName, r.Method, logPath, w.statusCode, time.Since(started), r.ContentLength, w.bytesWritten, atomic.LoadUint64(radosCalls))
	}()

	if !ok {
		http.NotFound(w, r)
		return
	}
	if repo.resourcePath == "" {
		status := http.StatusTemporaryRedirect
		if repo.patternMatch {
			status = http.StatusMovedPermanently
		}
		redirectWithTrailingSlash(w, r, status)
		return
	}
	h.serveRepository(w, r, repo, radosCalls)
}

func logRequest(repo, method, path string, status int, duration time.Duration, requestBytes, responseBytes int64, radosCalls uint64) {
	attrs := []any{
		"method", method,
		"path", path,
		"status", status,
		"duration", duration.Round(time.Millisecond).String(),
		"req_bytes", requestBytes,
		"resp_bytes", responseBytes,
		"rados_calls", radosCalls,
	}
	if repo != "" && repo != "default" {
		attrs = append(attrs, "repo", repo)
	}
	slog.Info("request", attrs...)
}

func requestRoutePath(r *http.Request) (string, bool) {
	escaped := r.URL.EscapedPath()
	for i := 0; i+2 < len(escaped); i++ {
		if escaped[i] == '%' && escaped[i+1] == '2' && (escaped[i+2] == 'f' || escaped[i+2] == 'F') {
			return "", false
		}
	}
	return r.URL.Path, true
}

func redirectWithTrailingSlash(w http.ResponseWriter, r *http.Request, status int) {
	target := (&url.URL{Path: r.URL.Path + "/", RawQuery: r.URL.RawQuery}).String()
	http.Redirect(w, r, target, status)
}

func (h *Handler) resolveRepository(path string) (repositoryRoute, bool) {
	defaultConfig := h.repos["default"]
	if defaultConfig != nil && (path == "/" || isDefaultRepositoryPath(path)) {
		return repositoryRoute{name: "default", config: defaultConfig, resourcePath: path}, true
	}

	trimmed := strings.TrimPrefix(path, "/")
	name, remainder, hasSlash := strings.Cut(trimmed, "/")
	if name != "" && name != "default" && !isReservedRepoName(name) && isValidRepoName(name) {
		if config, patternMatch := h.resolveNamedRepository(name); config != nil {
			if !hasSlash {
				return repositoryRoute{name: name, config: config, patternMatch: patternMatch}, true
			}
			return repositoryRoute{name: name, config: config, resourcePath: "/" + remainder, patternMatch: patternMatch}, true
		}
	}
	if defaultConfig != nil {
		return repositoryRoute{name: "default", config: defaultConfig, resourcePath: path}, true
	}
	return repositoryRoute{}, false
}

func isDefaultRepositoryPath(path string) bool {
	if path == "/config" || path == "/config/" {
		return true
	}
	for _, blobType := range AllBlobTypes {
		if blobType == BlobTypeConfig {
			continue
		}
		prefix := "/" + string(blobType)
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

func (h *Handler) resolveNamedRepository(name string) (*RepoConfig, bool) {
	if config := h.repos[name]; config != nil && !strings.Contains(name, "*") {
		return config, false
	}
	for _, pattern := range h.patterns {
		if _, ok := pattern.match(name); ok {
			return h.repos[pattern.key], true
		}
	}
	return nil, false
}

func (h *Handler) serveHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}
	writeHealthBody(w)
}

func writeHealthBody(w http.ResponseWriter) {
	_, _ = io.WriteString(w, "ok\n")
}

func (h *Handler) serveReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}
	if !h.connections.Ready() {
		http.Error(w, "ceph cluster unavailable", http.StatusServiceUnavailable)
		return
	}
	writeHealthBody(w)
}

func (h *Handler) serveRepository(w *responseWriter, r *http.Request, repo repositoryRoute, radosCalls *uint64) {
	access := minimumAccess(
		h.access,
		ParseAccess(repo.config.Access),
		listenerAccessForRequest(r.Context()),
		grantForRepo(r.Context(), repo.name),
	)

	if repo.resourcePath == "/" {
		h.serveRepositoryRoot(w, r, repo.name, access, radosCalls)
		return
	}
	if repo.resourcePath == "/config" {
		h.serveConfig(w, r, repo.name, access, radosCalls)
		return
	}
	route, ok := parseBlobRoute(repo.resourcePath)
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch route.kind {
	case blobRouteRedirect:
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			methodNotAllowed(w, http.MethodGet, http.MethodHead)
			return
		}
		redirectWithTrailingSlash(w, r, http.StatusTemporaryRedirect)
	case blobRouteList:
		h.serveList(w, r, repo.name, route.blobType, access, radosCalls)
	case blobRouteObject:
		h.serveObject(w, r, repo.name, route.blobType, route.objectID, access, radosCalls)
	}
}

func minimumAccess(accesses ...Access) Access {
	access := AccessReadWrite
	for _, candidate := range accesses {
		if candidate < access {
			access = candidate
		}
	}
	return access
}

func parseBlobRoute(path string) (blobRoute, bool) {
	trimmed := strings.TrimPrefix(path, "/")
	typeName, remainder, hasSlash := strings.Cut(trimmed, "/")
	blobType := BlobType(typeName)
	if !hasSlash {
		return blobRoute{kind: blobRouteRedirect, blobType: blobType}, true
	}
	if remainder == "" {
		return blobRoute{kind: blobRouteList, blobType: blobType}, true
	}
	if strings.Contains(remainder, "/") {
		return blobRoute{}, false
	}
	return blobRoute{kind: blobRouteObject, blobType: blobType, objectID: remainder}, true
}

func canStripeBlobType(blobType BlobType) bool {
	switch blobType {
	case BlobTypeSnapshots, BlobTypeData, BlobTypeIndex:
		return true
	default:
		return false
	}
}

func isBlobType(blobType BlobType) bool {
	for _, candidate := range AllBlobTypes {
		if candidate == blobType {
			return true
		}
	}
	return false
}

func isObjectID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (h *Handler) serveRepositoryRoot(w *responseWriter, r *http.Request, repo string, access Access, radosCalls *uint64) {
	switch r.Method {
	case http.MethodPost:
		if access < AccessReadAppend {
			accessDenied(w)
			return
		}
		if !requireQueryValue(w, r, "create") {
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if access < AccessReadWrite {
			accessDenied(w)
			return
		}
		if !requireQueryValue(w, r, "purge") {
			return
		}
		if err := h.purgeRepository(r.Context(), repo, radosCalls); err != nil {
			h.respondError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		methodNotAllowed(w, http.MethodDelete, http.MethodPost)
	}
}

func requireQueryValue(w http.ResponseWriter, r *http.Request, name string) bool {
	value := r.URL.Query().Get(name)
	if value == "" {
		http.Error(w, "missing required query parameter: "+name, http.StatusBadRequest)
		return false
	}
	if value != "true" {
		http.Error(w, "invalid value for "+name+" parameter: must be 'true'", http.StatusBadRequest)
		return false
	}
	return true
}

func (h *Handler) serveConfig(w *responseWriter, r *http.Request, repo string, access Access, radosCalls *uint64) {
	switch r.Method {
	case http.MethodHead, http.MethodGet:
		if access < AccessRead {
			accessDenied(w)
			return
		}
		if err := h.readObject(w, r, repo, BlobTypeConfig, "", radosCalls); err != nil {
			h.respondError(w, r, err)
		}
	case http.MethodPost:
		if access < AccessReadAppend {
			accessDenied(w)
			return
		}
		if err := h.writeObject(r, repo, BlobTypeConfig, "", radosCalls); err != nil {
			h.respondError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if access < AccessReadWrite {
			accessDenied(w)
			return
		}
		if err := h.deleteObject(repo, BlobTypeConfig, "", radosCalls); err != nil {
			h.respondError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		methodNotAllowed(w, http.MethodDelete, http.MethodGet, http.MethodHead, http.MethodPost)
	}
}

func (h *Handler) serveList(w *responseWriter, r *http.Request, repo string, blobType BlobType, access Access, radosCalls *uint64) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}
	if access < AccessRead {
		accessDenied(w)
		return
	}
	if blobType == BlobTypeConfig || !isBlobType(blobType) {
		http.NotFound(w, r)
		return
	}
	v2 := acceptsV2(r.Header)
	data, err := h.listObjects(repo, blobType, v2, radosCalls)
	if err != nil {
		h.respondError(w, r, err)
		return
	}
	contentType := mimeTypeAPIV1
	if v2 {
		contentType = mimeTypeAPIV2
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		if _, err := w.Write(data); err != nil {
			slog.Warn("failed to list blobs", "repo", repo, "blob_type", blobType, "error", err)
		}
	}
}

func acceptsV2(header http.Header) bool {
	for _, value := range header.Values("Accept") {
		for _, item := range strings.Split(value, ",") {
			mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(item))
			if err != nil || !strings.EqualFold(mediaType, mimeTypeAPIV2) {
				continue
			}
			if raw, ok := params["q"]; ok {
				if quality, err := strconv.ParseFloat(raw, 64); err == nil && quality <= 0 {
					continue
				}
			}
			return true
		}
	}
	return false
}

func (h *Handler) serveObject(w *responseWriter, r *http.Request, repo string, blobType BlobType, objectID string, access Access, radosCalls *uint64) {
	validObject := blobType != BlobTypeConfig && isBlobType(blobType) && isObjectID(objectID)
	switch r.Method {
	case http.MethodHead, http.MethodGet:
		if access < AccessRead {
			accessDenied(w)
			return
		}
		if !validObject {
			http.NotFound(w, r)
			return
		}
		if err := h.readObject(w, r, repo, blobType, objectID, radosCalls); err != nil {
			h.respondError(w, r, err)
		}
	case http.MethodPost:
		if access < AccessReadAppend {
			accessDenied(w)
			return
		}
		if !validObject {
			http.NotFound(w, r)
			return
		}
		if err := h.writeObject(r, repo, blobType, objectID, radosCalls); err != nil {
			h.respondError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if !validObject {
			http.NotFound(w, r)
			return
		}
		required := AccessReadWrite
		if blobType == BlobTypeLocks {
			required = AccessReadAppend
		}
		if access < required {
			accessDenied(w)
			return
		}
		if err := h.deleteObject(repo, blobType, objectID, radosCalls); err != nil {
			h.respondError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		methodNotAllowed(w, http.MethodDelete, http.MethodGet, http.MethodHead, http.MethodPost)
	}
}

func methodNotAllowed(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}

func accessDenied(w http.ResponseWriter) {
	http.Error(w, "access denied", http.StatusForbidden)
}

func objectName(blobType BlobType, objectID string) string {
	if blobType == BlobTypeConfig {
		return "config"
	}
	return string(blobType) + "/" + objectID
}

func (h *Handler) openHandlerContext(repo string, blobType BlobType, radosCalls *uint64) (*HandlerContext, error) {
	upper, lower, handle, config, err := h.connections.GetIOContextForRepo(repo, blobType, radosCalls)
	if err != nil {
		return nil, err
	}
	hctx := &HandlerContext{handle: handle}
	hctx.layers = append(hctx.layers, storageLayer{name: "upper", ioctx: upper, config: config})
	if lower != nil {
		hctx.layers = append(hctx.layers, storageLayer{name: "lower", ioctx: lower, config: config.Lower})
	}
	return hctx, nil
}

func (hctx *HandlerContext) Destroy() {
	for _, layer := range hctx.layers {
		layer.ioctx.Destroy()
	}
	hctx.handle.release()
}

func (hctx *HandlerContext) reportError(err error) {
	if hctx.handle != nil && err != nil {
		hctx.handle.manager.reconnectAfterError(hctx.handle.connection, err)
	}
}

func plainBackend(layer storageLayer, readBuf, writeBuf []byte, radosCalls *uint64) RadosIOContext {
	return NewRadosIO(layer.ioctx, layer.config.Prefix, layer.config.Alignment, readBuf, writeBuf, radosCalls)
}

func stripedBackend(layer storageLayer, readBuf, writeBuf []byte, radosCalls *uint64) RadosIOContext {
	return NewStripedIO(layer.ioctx, layer.config.Prefix, uint64(layer.config.MaxObjectSize), layer.config.Alignment, readBuf, writeBuf, radosCalls)
}

func probeLayer(layer storageLayer, name string, readBuf, writeBuf []byte, radosCalls *uint64) (storedObject, bool, error) {
	plain := plainBackend(layer, readBuf, writeBuf, radosCalls)
	stat, err := plain.Stat(name)
	if err == nil {
		return storedObject{backend: plain, stat: stat}, true, nil
	}
	if !errors.Is(err, rados.ErrNotFound) {
		return storedObject{}, false, err
	}

	striped := stripedBackend(layer, readBuf, writeBuf, radosCalls)
	stat, err = striped.Stat(name)
	if err == nil {
		return storedObject{backend: striped, stat: stat, striped: true}, true, nil
	}
	if errors.Is(err, rados.ErrNotFound) {
		return storedObject{}, false, nil
	}
	return storedObject{}, false, err
}

func (h *Handler) readObject(w *responseWriter, r *http.Request, repo string, blobType BlobType, objectID string, radosCalls *uint64) error {
	readBufPtr := h.readPool.Get()
	defer h.readPool.Put(readBufPtr)

	hctx, err := h.openHandlerContext(repo, blobType, radosCalls)
	if err != nil {
		return err
	}
	defer hctx.Destroy()

	name := objectName(blobType, objectID)
	var object storedObject
	found := false
	for _, layer := range hctx.layers {
		object, found, err = probeLayer(layer, name, *readBufPtr, nil, radosCalls)
		if err != nil {
			hctx.reportError(err)
			return fmt.Errorf("stat object %s in %s layer: %w", name, layer.name, err)
		}
		if found {
			break
		}
	}
	if !found {
		return rados.ErrNotFound
	}
	slog.Debug("reading blob", "object", name, "size", object.stat.Size, "striped", object.striped)
	if object.stat.Size > math.MaxInt64 {
		return fmt.Errorf("object %s size exceeds supported range: %d", name, object.stat.Size)
	}

	requested, err := parseRange(r.Header.Values("Range"), int64(object.stat.Size))
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", object.stat.Size))
		http.Error(w, "requested range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return nil
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(requested.length, 10))
	status := http.StatusOK
	if requested.partial {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", requested.start, requested.start+requested.length-1, object.stat.Size))
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead || requested.length == 0 {
		return nil
	}
	n, sum, err := object.backend.ReadObject(name, requested.start, requested.length, w)
	if err != nil {
		hctx.reportError(err)
		return fmt.Errorf("read object %s: %w", name, err)
	}
	if n != requested.length {
		return fmt.Errorf("read object %s: %w", name, io.ErrUnexpectedEOF)
	}
	if requested.start == 0 && requested.length == int64(object.stat.Size) {
		warnOnHashMismatch(name, objectID, sum)
	}
	return nil
}

func warnOnHashMismatch(name, objectID string, sum [32]byte) {
	if objectID == "" {
		return
	}
	actual := hex.EncodeToString(sum[:])
	if actual == objectID {
		return
	}
	slog.Warn("hash mismatch on read", "object", name, "expected", objectID, "actual", actual)
}

func parseRange(values []string, size int64) (byteRange, error) {
	if len(values) == 0 || size == 0 {
		return byteRange{length: size}, nil
	}
	raw := values[0]
	if !strings.HasPrefix(raw, "bytes=") || strings.Contains(strings.TrimPrefix(raw, "bytes="), ",") {
		return byteRange{}, errInvalidRange
	}
	spec := strings.TrimSpace(strings.TrimPrefix(raw, "bytes="))
	startText, endText, ok := strings.Cut(spec, "-")
	if !ok || strings.Contains(endText, "-") {
		return byteRange{}, errInvalidRange
	}
	if startText == "" {
		suffix, err := strconv.ParseInt(endText, 10, 64)
		if err != nil || suffix <= 0 || size == 0 {
			return byteRange{}, errInvalidRange
		}
		if suffix > size {
			suffix = size
		}
		return byteRange{start: size - suffix, length: suffix, partial: true}, nil
	}
	start, err := strconv.ParseInt(startText, 10, 64)
	if err != nil || start < 0 || start >= size {
		return byteRange{}, errInvalidRange
	}
	end := size - 1
	if endText != "" {
		end, err = strconv.ParseInt(endText, 10, 64)
		if err != nil || end < start {
			return byteRange{}, errInvalidRange
		}
		if end >= size {
			end = size - 1
		}
	}
	return byteRange{start: start, length: end - start + 1, partial: true}, nil
}

func (h *Handler) writeObject(r *http.Request, repo string, blobType BlobType, objectID string, radosCalls *uint64) error {
	writeBufPtr := h.writePool.Get()
	defer h.writePool.Put(writeBufPtr)

	hctx, err := h.openHandlerContext(repo, blobType, radosCalls)
	if err != nil {
		return err
	}
	defer hctx.Destroy()

	name := objectName(blobType, objectID)
	for _, layer := range hctx.layers {
		_, found, err := probeLayer(layer, name, nil, *writeBufPtr, radosCalls)
		if err != nil {
			hctx.reportError(err)
			return fmt.Errorf("stat object %s in %s layer: %w", name, layer.name, err)
		}
		if found {
			return errObjectExists
		}
	}

	upper := hctx.layers[0]
	stripingAllowed := upper.config.Striped && canStripeBlobType(blobType)
	maxObjectSize := upper.config.MaxObjectSize
	if !stripingAllowed {
		maxObjectSize = defaultMaxObjectSize
		if configured, err := hctx.handle.connection.conn.GetConfigOption("osd_max_object_size"); err == nil {
			maxObjectSize = parseClusterMaxObjectSize(configured)
		}
	}
	if maxObjectSize <= 0 {
		return fmt.Errorf("invalid maximum object size %d", maxObjectSize)
	}
	striped := false
	if r.ContentLength > maxObjectSize {
		if !stripingAllowed {
			return errContentTooLarge
		}
		striped = true
	}
	if r.ContentLength < 0 && stripingAllowed {
		return errLengthRequired
	}

	reader := io.Reader(r.Body)
	if !striped && r.ContentLength < 0 {
		reader = io.LimitReader(reader, maxObjectSize+1)
	}
	backend := plainBackend(upper, nil, *writeBufPtr, radosCalls)
	if striped {
		backend = stripedBackend(upper, nil, *writeBufPtr, radosCalls)
	}

	n, sum, err := backend.WriteObject(name, reader)
	if err != nil {
		hctx.reportError(err)
		if !errors.Is(err, errObjectExists) {
			h.cleanupWrite(hctx, backend, name, repo, blobType, err)
		}
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			return errClientAborted
		}
		return fmt.Errorf("write object %s: %w", name, err)
	}
	if r.ContentLength >= 0 && n != r.ContentLength {
		h.cleanupWrite(hctx, backend, name, repo, blobType, io.ErrUnexpectedEOF)
		return fmt.Errorf("write object %s: %w", name, io.ErrUnexpectedEOF)
	}
	if !striped && n > maxObjectSize {
		h.cleanupWrite(hctx, backend, name, repo, blobType, errContentTooLarge)
		return fmt.Errorf("write object %s: %w", name, errContentTooLarge)
	}
	if blobType != BlobTypeConfig && hex.EncodeToString(sum[:]) != objectID {
		h.cleanupWrite(hctx, backend, name, repo, blobType, errHashMismatch)
		return errHashMismatch
	}
	slog.Debug("created blob", "object", name, "size", n, "striped", striped)
	return nil
}

func (h *Handler) cleanupWrite(hctx *HandlerContext, backend RadosIOContext, name, repo string, blobType BlobType, cause error) {
	if err := backend.Remove(name); err != nil && !errors.Is(err, rados.ErrNotFound) {
		hctx.reportError(err)
		slog.Error("failed to clean up incomplete object", "repo", repo, "blob_type", blobType, "object", name, "cause", cause, "cleanup_error", err)
	}
}

func (h *Handler) deleteObject(repo string, blobType BlobType, objectID string, radosCalls *uint64) error {
	hctx, err := h.openHandlerContext(repo, blobType, radosCalls)
	if err != nil {
		return err
	}
	defer hctx.Destroy()

	name := objectName(blobType, objectID)
	var unsupportedStripedErr error
	removedSupportedRepresentation := false
	for i := len(hctx.layers) - 1; i >= 0; i-- {
		layer := hctx.layers[i]
		representations := []struct {
			name    string
			backend RadosIOContext
		}{
			{name: "plain", backend: plainBackend(layer, nil, nil, radosCalls)},
			{name: "striped", backend: stripedBackend(layer, nil, nil, radosCalls)},
		}
		for _, representation := range representations {
			if _, err := representation.backend.Stat(name); errors.Is(err, rados.ErrNotFound) {
				continue
			} else if err != nil {
				if errors.Is(err, errUnsupportedStriperLayout) {
					if unsupportedStripedErr == nil {
						unsupportedStripedErr = fmt.Errorf("stat %s %s representation in %s layer: %w", name, representation.name, layer.name, err)
					}
					continue
				}
				hctx.reportError(err)
				return fmt.Errorf("stat %s %s representation in %s layer: %w", name, representation.name, layer.name, err)
			}

			slog.Debug("removing object from layer", "repo", repo, "blob_type", blobType, "object", name, "layer", layer.name, "representation", representation.name)
			if err := representation.backend.Remove(name); err != nil && !errors.Is(err, rados.ErrNotFound) {
				if errors.Is(err, errUnsupportedStriperLayout) {
					if unsupportedStripedErr == nil {
						unsupportedStripedErr = fmt.Errorf("delete %s %s representation from %s layer: %w", name, representation.name, layer.name, err)
					}
					continue
				}
				hctx.reportError(err)
				return fmt.Errorf("delete %s %s representation from %s layer: %w", name, representation.name, layer.name, err)
			}
			removedSupportedRepresentation = true
		}
	}
	if unsupportedStripedErr != nil {
		if removedSupportedRepresentation {
			slog.Warn("leaving striped object with unsupported layout", "repo", repo, "blob_type", blobType, "object", name, "error", unsupportedStripedErr)
		}
		return unsupportedStripedErr
	}
	return nil
}

func (h *Handler) listObjects(repo string, blobType BlobType, v2 bool, radosCalls *uint64) ([]byte, error) {
	hctx, err := h.openHandlerContext(repo, blobType, radosCalls)
	if err != nil {
		return nil, err
	}
	defer hctx.Destroy()

	seen := make(map[string]struct{})
	blobs := make([]listedBlob, 0)
	for _, layer := range hctx.layers {
		candidates := make(map[string]blobRepresentations)
		prefix := layer.config.Prefix + string(blobType) + "/"
		err := visitPhysicalObjects(layer.ioctx, layer.config.Pool, layer.config.Namespace, radosCalls, func(object string) error {
			if !strings.HasPrefix(object, prefix) {
				return nil
			}
			id, kind, recognized := classifyPhysicalBlob(strings.TrimPrefix(object, prefix))
			if !recognized {
				slog.Warn("skipping unknown object", "repo", repo, "blob_type", blobType, "object", object)
				return nil
			}
			if kind == continuationStripe {
				return nil
			}
			representations := candidates[id]
			switch kind {
			case plainBlob:
				representations.plain = true
			case firstStripe:
				representations.striped = true
			}
			candidates[id] = representations
			return nil
		})
		if err != nil {
			hctx.reportError(err)
			return nil, fmt.Errorf("list %s objects in %s layer: %w", blobType, layer.name, err)
		}

		plain := plainBackend(layer, nil, nil, radosCalls)
		striped := stripedBackend(layer, nil, nil, radosCalls)
		for id, representations := range candidates {
			if _, exists := seen[id]; exists {
				continue
			}
			blob := listedBlob{Name: id}
			if !v2 {
				seen[id] = struct{}{}
				blobs = append(blobs, blob)
				continue
			}

			name := objectName(blobType, id)
			if representations.plain {
				stat, err := plain.Stat(name)
				if err == nil {
					seen[id] = struct{}{}
					blob.Size = stat.Size
					blobs = append(blobs, blob)
					continue
				}
				if !errors.Is(err, rados.ErrNotFound) {
					hctx.reportError(err)
					return nil, fmt.Errorf("stat plain object %s in %s layer: %w", name, layer.name, err)
				}
			}
			if representations.striped {
				stat, err := striped.Stat(name)
				if err != nil {
					if errors.Is(err, errUnsupportedStriperLayout) {
						seen[id] = struct{}{}
						slog.Warn("skipping striped object with unsupported layout", "repo", repo, "blob_type", blobType, "object", name, "error", err)
						continue
					}
					if errors.Is(err, rados.ErrNotFound) {
						continue
					}
					hctx.reportError(err)
					return nil, fmt.Errorf("stat striped object %s in %s layer: %w", name, layer.name, err)
				}
				seen[id] = struct{}{}
				blob.Size = stat.Size
				blobs = append(blobs, blob)
			}
		}
	}

	sort.Slice(blobs, func(i, j int) bool {
		return blobs[i].Name < blobs[j].Name
	})
	if v2 {
		return json.Marshal(blobs)
	}
	names := make([]string, len(blobs))
	for i, blob := range blobs {
		names[i] = blob.Name
	}
	return json.Marshal(names)
}

func classifyPhysicalBlob(name string) (string, physicalBlobKind, bool) {
	if isObjectID(name) {
		return name, plainBlob, true
	}
	if len(name) != 64+stripeSuffixLen || !isObjectID(name[:64]) || !isStripeSuffix(name[64:]) {
		return "", invalidPhysicalBlob, false
	}
	if name[64:] == firstStripeSuffix {
		return name[:64], firstStripe, true
	}
	return name[:64], continuationStripe, true
}

func isStripeSuffix(suffix string) bool {
	if len(suffix) != stripeSuffixLen || suffix[0] != '.' {
		return false
	}
	for _, c := range suffix[1:] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func visitPhysicalObjects(ioctx *rados.IOContext, pool, namespace string, radosCalls *uint64, visit func(string) error) error {
	slog.Debug("rados.Iter", "pool", pool, "namespace", namespace)
	atomic.AddUint64(radosCalls, 1)
	iter, err := ioctx.Iter()
	if err != nil {
		return fmt.Errorf("list objects: %w", err)
	}
	defer iter.Close()
	for iter.Next() {
		if err := visit(iter.Value()); err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterate objects: %w", err)
	}
	return nil
}

func (h *Handler) purgeRepository(ctx context.Context, repo string, radosCalls *uint64) error {
	scopes, err := h.purgeScopes(repo)
	if err != nil {
		return err
	}
	targets := collectPurgeTargets(scopes)

	for _, target := range targets {
		if err := h.checkPurgeGate(target, radosCalls); err != nil {
			return err
		}
	}

	deletedCount := 0
	foreignCount := 0
	for _, target := range targets {
		deleted, foreign, err := h.purgeTargetObjects(ctx, repo, target, radosCalls)
		deletedCount += deleted
		foreignCount += foreign
		if err != nil {
			return err
		}
	}
	if foreignCount > 0 {
		slog.Info("leaving foreign objects", "repo", repo, "count", foreignCount)
	}
	slog.Info("purged repository", "repo", repo, "objects", deletedCount)
	return nil
}

func (h *Handler) checkPurgeGate(target *purgeTarget, radosCalls *uint64) error {
	guarded := false
	for _, scope := range target.scopes {
		if scope.types[BlobTypeSnapshots] || scope.types[BlobTypeLocks] {
			guarded = true
		}
	}
	if !guarded {
		return nil
	}
	return h.visitPurgeTarget(target, radosCalls, func(object string) error {
		for _, scope := range target.scopes {
			if scope.types[BlobTypeSnapshots] && purgeTypeOwns(scope.prefix, BlobTypeSnapshots, object) {
				return errRepositoryHasSnap
			}
			if scope.types[BlobTypeLocks] && purgeTypeOwns(scope.prefix, BlobTypeLocks, object) {
				return errRepositoryLocked
			}
		}
		return nil
	})
}

func (h *Handler) purgeTargetObjects(ctx context.Context, repo string, target *purgeTarget, radosCalls *uint64) (int, int, error) {
	var deleted atomic.Uint64
	foreign := 0

	err := h.withPurgeTarget(target, radosCalls, func(ioctx *rados.IOContext) error {
		names := make(chan string, purgeDeleteWorkers)
		failures := make(chan error, 1)
		var stopped atomic.Bool
		var workers sync.WaitGroup
		for range purgeDeleteWorkers {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for object := range names {
					if stopped.Load() {
						continue
					}
					slog.Debug("rados.Delete", "object", object)
					atomic.AddUint64(radosCalls, 1)
					if err := ioctx.Delete(object); err != nil && !errors.Is(err, rados.ErrNotFound) {
						stopped.Store(true)
						select {
						case failures <- fmt.Errorf("delete object %s: %w", object, err):
						default:
						}
						continue
					}
					deleted.Add(1)
				}
			}()
		}

		visitErr := visitPhysicalObjects(ioctx, target.pool, target.namespace, radosCalls, func(object string) error {
			if stopped.Load() {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			owner := purgeTargetOwner(target, object)
			if owner == nil {
				foreign++
				return nil
			}
			slog.Debug("purging object", "repo", repo, "object", object, "layer", owner.layer)
			names <- object
			return nil
		})
		close(names)
		workers.Wait()

		select {
		case failure := <-failures:
			return failure
		default:
			return visitErr
		}
	})
	return int(deleted.Load()), foreign, err
}

func purgeTargetOwner(target *purgeTarget, object string) *purgeScope {
	for _, scope := range target.scopes {
		if purgeScopeOwns(scope, object) {
			return scope
		}
	}
	return nil
}

func (h *Handler) purgeScopes(repo string) ([]*purgeScope, error) {
	byKey := make(map[purgeScopeKey]*purgeScope)
	var missingPool error
	add := func(layer string, config *BlobPool, blobType BlobType) {
		key := purgeScopeKey{layer: layer, pool: config.Pool, namespace: config.Namespace, prefix: config.Prefix}
		scope := byKey[key]
		if scope == nil {
			scope = &purgeScope{
				layer:     layer,
				pool:      config.Pool,
				namespace: config.Namespace,
				prefix:    config.Prefix,
				types:     make(map[BlobType]bool),
			}
			byKey[key] = scope
		}
		scope.types[blobType] = true
	}

	for _, blobType := range AllBlobTypes {
		config, err := h.connections.GetBlobPoolForRepo(repo, blobType)
		if err != nil {
			if errors.Is(err, errPoolNotConfigured) {
				if missingPool == nil {
					missingPool = err
				}
				continue
			}
			return nil, err
		}
		if config.Lower != nil {
			add("lower", config.Lower, blobType)
		}
		add("upper", config, blobType)
	}
	if len(byKey) == 0 && missingPool != nil {
		return nil, missingPool
	}

	scopes := make([]*purgeScope, 0, len(byKey))
	for _, scope := range byKey {
		scopes = append(scopes, scope)
	}
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].layer != scopes[j].layer {
			return scopes[i].layer == "lower"
		}
		if scopes[i].pool != scopes[j].pool {
			return scopes[i].pool < scopes[j].pool
		}
		if scopes[i].namespace != scopes[j].namespace {
			return scopes[i].namespace < scopes[j].namespace
		}
		return scopes[i].prefix < scopes[j].prefix
	})
	return scopes, nil
}

func collectPurgeTargets(scopes []*purgeScope) []*purgeTarget {
	byKey := make(map[purgeTargetKey]*purgeTarget)
	var targets []*purgeTarget
	for _, scope := range scopes {
		key := purgeTargetKey{pool: scope.pool, namespace: scope.namespace}
		target := byKey[key]
		if target == nil {
			target = &purgeTarget{pool: scope.pool, namespace: scope.namespace}
			byKey[key] = target
			targets = append(targets, target)
		}
		target.scopes = append(target.scopes, scope)
	}
	return targets
}

func (h *Handler) withPurgeTarget(target *purgeTarget, radosCalls *uint64, fn func(*rados.IOContext) error) error {
	ioctx, handle, err := h.connections.OpenNamespaceContext(target.pool, target.namespace, radosCalls)
	if err != nil {
		return err
	}
	defer handle.release()
	defer ioctx.Destroy()

	err = fn(ioctx)
	if err != nil {
		handle.manager.reconnectAfterError(handle.connection, err)
	}
	return err
}

func (h *Handler) visitPurgeTarget(target *purgeTarget, radosCalls *uint64, visit func(string) error) error {
	return h.withPurgeTarget(target, radosCalls, func(ioctx *rados.IOContext) error {
		return visitPhysicalObjects(ioctx, target.pool, target.namespace, radosCalls, visit)
	})
}

func purgeScopeOwns(scope *purgeScope, object string) bool {
	for blobType := range scope.types {
		if purgeTypeOwns(scope.prefix, blobType, object) {
			return true
		}
	}
	return false
}

func purgeTypeOwns(prefix string, blobType BlobType, object string) bool {
	if blobType == BlobTypeConfig {
		name := prefix + "config"
		return object == name || strings.HasPrefix(object, name) && isStripeSuffix(object[len(name):])
	}
	return strings.HasPrefix(object, prefix+string(blobType)+"/")
}

func (h *Handler) respondError(w *responseWriter, r *http.Request, err error) {
	if w.headerWritten {
		h.logRequestError("request failed after response started", r, err)
		return
	}
	switch {
	case errors.Is(err, errHashMismatch):
		http.Error(w, "hash mismatch", http.StatusBadRequest)
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, errClientAborted):
		http.Error(w, "client aborted request", http.StatusBadRequest)
	case errors.Is(err, errContentTooLarge), hasRadosErrorCode(err, syscall.EFBIG):
		http.Error(w, "object size exceeds cluster limit", http.StatusRequestEntityTooLarge)
	case hasRadosErrorCode(err, syscall.EMSGSIZE):
		http.Error(w, "write chunk exceeds message limit", http.StatusRequestEntityTooLarge)
	case hasRadosErrorCode(err, syscall.EOPNOTSUPP):
		h.logRequestError("operation not supported", r, err)
		http.Error(w, "operation not supported", http.StatusInternalServerError)
	case hasRadosErrorCode(err, syscall.ENOSPC):
		h.logRequestError("insufficient storage", r, err)
		http.Error(w, "insufficient storage", http.StatusInsufficientStorage)
	case hasRadosErrorCode(err, syscall.EDQUOT):
		h.logRequestError("disk quota exceeded", r, err)
		http.Error(w, "insufficient storage", http.StatusInsufficientStorage)
	case errors.Is(err, errLengthRequired):
		http.Error(w, "content length required", http.StatusLengthRequired)
	case errors.Is(err, errObjectExists):
		http.Error(w, "object already exists", http.StatusForbidden)
	case errors.Is(err, errRepositoryHasSnap):
		http.Error(w, errRepositoryHasSnap.Error(), http.StatusConflict)
	case errors.Is(err, errRepositoryLocked):
		http.Error(w, errRepositoryLocked.Error(), http.StatusConflict)
	case errors.Is(err, errPoolNotConfigured):
		http.Error(w, errPoolNotConfigured.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, errConnectionUnavailable):
		http.Error(w, "ceph cluster unavailable", http.StatusServiceUnavailable)
	case errors.Is(err, rados.ErrNotFound):
		http.NotFound(w, r)
	default:
		h.logRequestError("request failed", r, err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func (h *Handler) logRequestError(message string, r *http.Request, err error) {
	repoName := ""
	path := r.URL.Path
	if repo, ok := h.resolveRepository(path); ok {
		repoName = repo.name
		path = repo.resourcePath
		if path == "" {
			path = "/"
		}
	}
	attrs := []any{"method", r.Method, "path", path, "error", err}
	if repoName != "" && repoName != "default" {
		attrs = append(attrs, "repo", repoName)
	}
	slog.Error(message, attrs...)
}

func hasRadosErrorCode(err error, codes ...syscall.Errno) bool {
	var coded interface{ ErrorCode() int }
	if !errors.As(err, &coded) {
		return false
	}
	for _, code := range codes {
		if coded.ErrorCode() == -int(code) {
			return true
		}
	}
	return false
}
