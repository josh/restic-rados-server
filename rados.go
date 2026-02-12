package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ceph/go-ceph/rados"
)

const (
	xattrStripeUnit  = "striper.layout.stripe_unit"
	xattrStripeCount = "striper.layout.stripe_count"
	xattrObjectSize  = "striper.layout.object_size"
	xattrSize        = "striper.size"
)

type RadosIOContext interface {
	Stat(object string) (StatInfo, error)
	Remove(object string) error
	ReadObject(object string, offset, length int64, w io.Writer) (n int64, sum [32]byte, err error)
	WriteObject(object string, r io.Reader) (n int64, sum [32]byte, err error)
}

type BufferPool struct {
	pool *sync.Pool
	size int64
}

type StatInfo struct {
	Size    uint64
	ModTime time.Time
}

type radosIOContextWrapper struct {
	ioctx       *rados.IOContext
	radosCalls  *uint64
	readBuffer  *BufferPool
	writeBuffer *BufferPool
	alignment   uint64
}

type striperIOContextWrapper struct {
	ioctx       *rados.IOContext
	objectSize  uint64
	radosCalls  *uint64
	readBuffer  *BufferPool
	writeBuffer *BufferPool
	alignment   uint64
}

func NewBufferPool(size int64) *BufferPool {
	return &BufferPool{
		pool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, size)
				return &buf
			},
		},
		size: size,
	}
}

func (bp *BufferPool) Get() *[]byte {
	return bp.pool.Get().(*[]byte)
}

func (bp *BufferPool) Put(bufPtr *[]byte) {
	bp.pool.Put(bufPtr)
}

func (bp *BufferPool) Size() int64 {
	return bp.size
}

func (s *striperIOContextWrapper) getObjectID(soid string, objectno uint64) string {
	return fmt.Sprintf("%s.%016x", soid, objectno)
}

func (r *radosIOContextWrapper) Stat(object string) (StatInfo, error) {
	slog.Debug("rados.Stat", "object", object)
	atomic.AddUint64(r.radosCalls, 1)
	stat, err := r.ioctx.Stat(object)
	return StatInfo{Size: stat.Size, ModTime: stat.ModTime}, err
}

func (s *striperIOContextWrapper) Stat(object string) (StatInfo, error) {
	firstObjID := s.getObjectID(object, 0)

	slog.Debug("rados.Stat", "object", firstObjID)
	atomic.AddUint64(s.radosCalls, 1)
	stat, err := s.ioctx.Stat(firstObjID)
	if err != nil {
		return StatInfo{}, err
	}

	sizeAttr := make([]byte, 32)
	slog.Debug("rados.GetXattr", "object", firstObjID, "xattr", xattrSize)
	atomic.AddUint64(s.radosCalls, 1)
	n, err := s.ioctx.GetXattr(firstObjID, xattrSize, sizeAttr)
	if err != nil {
		return StatInfo{}, fmt.Errorf("get size xattr: %w", err)
	}

	size, err := strconv.ParseUint(string(sizeAttr[:n]), 10, 64)
	if err != nil {
		return StatInfo{}, fmt.Errorf("parse size xattr: %w", err)
	}

	return StatInfo{Size: size, ModTime: stat.ModTime}, nil
}

func (r *radosIOContextWrapper) Remove(object string) error {
	slog.Debug("rados.Remove", "object", object)
	atomic.AddUint64(r.radosCalls, 1)
	return r.ioctx.Delete(object)
}

func (s *striperIOContextWrapper) Remove(object string) error {
	firstObjID := s.getObjectID(object, 0)

	sizeAttr := make([]byte, 32)
	slog.Debug("rados.GetXattr", "object", firstObjID, "xattr", xattrSize)
	atomic.AddUint64(s.radosCalls, 1)
	n, err := s.ioctx.GetXattr(firstObjID, xattrSize, sizeAttr)
	if err != nil {
		if err.Error() == rados.ErrNotFound.Error() {
			return rados.ErrNotFound
		}
		return fmt.Errorf("get size xattr: %w", err)
	}

	totalSize, err := strconv.ParseUint(string(sizeAttr[:n]), 10, 64)
	if err != nil {
		return fmt.Errorf("parse size: %w", err)
	}

	numObjects := uint64(1)
	if totalSize > 0 {
		numObjects = (totalSize + s.objectSize - 1) / s.objectSize
	}

	for i := int64(numObjects - 1); i >= 1; i-- {
		objectID := s.getObjectID(object, uint64(i))
		slog.Debug("rados.Delete", "object", objectID)
		atomic.AddUint64(s.radosCalls, 1)
		err := s.ioctx.Delete(objectID)
		if err != nil && err.Error() != rados.ErrNotFound.Error() {
			return fmt.Errorf("delete object %d: %w", i, err)
		}
	}

	slog.Debug("rados.Delete", "object", firstObjID)
	atomic.AddUint64(s.radosCalls, 1)
	err = s.ioctx.Delete(firstObjID)
	if err != nil {
		return fmt.Errorf("delete first object: %w", err)
	}

	for i := numObjects; ; i++ {
		objectID := s.getObjectID(object, i)
		_, err := s.ioctx.Stat(objectID)
		if err != nil {
			break
		}
		slog.Debug("rados.Delete", "object", objectID, "orphaned", true)
		atomic.AddUint64(s.radosCalls, 1)
		if delErr := s.ioctx.Delete(objectID); delErr != nil {
			return fmt.Errorf("delete orphaned object %d: %w", i, delErr)
		}
	}

	return nil
}

func (r *radosIOContextWrapper) ReadObject(object string, offset, length int64, w io.Writer) (n int64, sum [32]byte, err error) {
	bufPtr := r.readBuffer.Get()
	defer r.readBuffer.Put(bufPtr)
	buffer := *bufPtr

	hasher := sha256.New()

	totalWritten := int64(0)
	currentOffset := offset
	remaining := length

	for remaining > 0 {
		toRead := int64(len(buffer))
		if toRead > remaining {
			toRead = remaining
		}

		slog.Debug("rados.Read", "object", object, "offset", currentOffset, "size", toRead)
		atomic.AddUint64(r.radosCalls, 1)
		n, err := r.ioctx.Read(object, buffer[:toRead], uint64(currentOffset))
		if err != nil && err != io.EOF {
			return totalWritten, [32]byte{}, fmt.Errorf("read %s at offset %d: %w", object, currentOffset, err)
		}

		if n > 0 {
			hasher.Write(buffer[:n])
			written, err := w.Write(buffer[:n])
			totalWritten += int64(written)
			if err != nil {
				return totalWritten, [32]byte{}, err
			}
			if written != n {
				return totalWritten, [32]byte{}, io.ErrShortWrite
			}
			currentOffset += int64(n)
			remaining -= int64(n)
		}

		if err == io.EOF || n == 0 {
			break
		}
	}

	return totalWritten, [32]byte(hasher.Sum(nil)), nil
}

func (s *striperIOContextWrapper) ReadObject(object string, offset, length int64, w io.Writer) (n int64, sum [32]byte, err error) {
	firstObjID := s.getObjectID(object, 0)

	slog.Debug("rados.Stat", "object", firstObjID)
	atomic.AddUint64(s.radosCalls, 1)
	_, err = s.ioctx.Stat(firstObjID)
	if err != nil {
		return 0, [32]byte{}, err
	}

	sizeAttr := make([]byte, 32)
	slog.Debug("rados.GetXattr", "object", firstObjID, "xattr", xattrSize)
	atomic.AddUint64(s.radosCalls, 1)
	xn, err := s.ioctx.GetXattr(firstObjID, xattrSize, sizeAttr)
	if err != nil {
		return 0, [32]byte{}, fmt.Errorf("get size xattr: %w", err)
	}

	totalSize, err := strconv.ParseUint(string(sizeAttr[:xn]), 10, 64)
	if err != nil {
		return 0, [32]byte{}, fmt.Errorf("parse size xattr: %w", err)
	}

	if uint64(offset) >= totalSize {
		return 0, [32]byte{}, nil
	}

	readLen := uint64(length)
	if uint64(offset)+readLen > totalSize {
		readLen = totalSize - uint64(offset)
	}

	bufPtr := s.readBuffer.Get()
	defer s.readBuffer.Put(bufPtr)
	buffer := *bufPtr

	hasher := sha256.New()

	totalWritten := int64(0)
	currentOffset := uint64(offset)
	remaining := int64(readLen)

	for remaining > 0 {
		objectNo := currentOffset / s.objectSize
		objectOffset := currentOffset % s.objectSize

		availableInObject := s.objectSize - objectOffset
		toRead := remaining
		if uint64(toRead) > availableInObject {
			toRead = int64(availableInObject)
		}
		if toRead > int64(len(buffer)) {
			toRead = int64(len(buffer))
		}

		objectID := s.getObjectID(object, objectNo)
		slog.Debug("rados.Read", "object", objectID, "offset", objectOffset, "size", toRead)
		atomic.AddUint64(s.radosCalls, 1)
		rn, err := s.ioctx.Read(objectID, buffer[:toRead], objectOffset)
		if err != nil && err != io.EOF {
			return totalWritten, [32]byte{}, fmt.Errorf("read %s at offset %d: %w", object, currentOffset, err)
		}

		if rn > 0 {
			hasher.Write(buffer[:rn])
			written, err := w.Write(buffer[:rn])
			totalWritten += int64(written)
			if err != nil {
				return totalWritten, [32]byte{}, err
			}
			if written != rn {
				return totalWritten, [32]byte{}, io.ErrShortWrite
			}
			currentOffset += uint64(rn)
			remaining -= int64(rn)
		}

		if rn == 0 || err == io.EOF {
			break
		}
	}

	return totalWritten, [32]byte(hasher.Sum(nil)), nil
}

func (r *radosIOContextWrapper) WriteObject(object string, rd io.Reader) (n int64, sum [32]byte, err error) {
	bufPtr := r.writeBuffer.Get()
	defer r.writeBuffer.Put(bufPtr)
	buffer := *bufPtr

	hasher := sha256.New()

	op := rados.CreateWriteOp()
	defer op.Release()
	op.Create(rados.CreateExclusive)
	slog.Debug("rados.CreateWriteOp", "object", object)
	atomic.AddUint64(r.radosCalls, 1)
	err = op.Operate(r.ioctx, object, rados.OperationNoFlag)
	if err != nil && err != rados.ErrObjectExists {
		return 0, [32]byte{}, fmt.Errorf("create object: %w", err)
	}

	alignment := int(r.alignment)
	if alignment == 0 {
		alignment = 1
	}
	if len(buffer) < alignment {
		slog.Warn("write buffer smaller than alignment", "bufferSize", len(buffer), "alignment", alignment)
	}

	totalRead := int64(0)
	bufferFilled := 0

	for {
		rn, readErr := rd.Read(buffer[bufferFilled:])
		bufferFilled += rn
		totalRead += int64(rn)

		isEOF := readErr == io.EOF
		if readErr != nil && !isEOF {
			return totalRead, [32]byte{}, readErr
		}

		var flushSize int
		if isEOF {
			flushSize = bufferFilled
		} else if bufferFilled >= len(buffer) {
			flushSize = (bufferFilled / alignment) * alignment
		} else {
			continue
		}

		if flushSize > 0 {
			data := buffer[:flushSize]
			hasher.Write(data)

			slog.Debug("rados.Append", "object", object, "size", len(data), "aligned", len(data)%alignment == 0)
			atomic.AddUint64(r.radosCalls, 1)
			if err := r.ioctx.Append(object, data); err != nil {
				return totalRead, [32]byte{}, fmt.Errorf("append: %w", err)
			}

			carryover := bufferFilled - flushSize
			if carryover > 0 {
				copy(buffer[:carryover], buffer[flushSize:bufferFilled])
			}
			bufferFilled = carryover
		}

		if isEOF {
			return totalRead, [32]byte(hasher.Sum(nil)), nil
		}
	}
}

func (s *striperIOContextWrapper) WriteObject(object string, rd io.Reader) (n int64, sum [32]byte, err error) {
	bufPtr := s.writeBuffer.Get()
	defer s.writeBuffer.Put(bufPtr)
	buffer := *bufPtr

	hasher := sha256.New()

	firstObjID := s.getObjectID(object, 0)

	op := rados.CreateWriteOp()
	defer op.Release()
	op.Create(rados.CreateExclusive)
	objectSizeStr := strconv.FormatUint(s.objectSize, 10)
	op.SetXattr(xattrStripeUnit, []byte(objectSizeStr))
	op.SetXattr(xattrStripeCount, []byte("1"))
	op.SetXattr(xattrObjectSize, []byte(objectSizeStr))
	op.SetXattr(xattrSize, []byte("0"))
	slog.Debug("rados.CreateWriteOp", "object", firstObjID)
	atomic.AddUint64(s.radosCalls, 1)
	err = op.Operate(s.ioctx, firstObjID, rados.OperationNoFlag)
	if err != nil && err != rados.ErrObjectExists {
		return 0, [32]byte{}, fmt.Errorf("create object: %w", err)
	}

	alignment := int(s.alignment)
	if alignment == 0 {
		alignment = 1
	}
	if len(buffer) < alignment {
		slog.Warn("write buffer smaller than alignment", "bufferSize", len(buffer), "alignment", alignment)
	}

	totalRead := int64(0)
	totalWritten := uint64(0)
	bufferFilled := 0

	for {
		rn, readErr := rd.Read(buffer[bufferFilled:])
		bufferFilled += rn
		totalRead += int64(rn)

		isEOF := readErr == io.EOF
		if readErr != nil && !isEOF {
			return totalRead, [32]byte{}, readErr
		}

		var flushSize int
		if isEOF {
			flushSize = bufferFilled
		} else if bufferFilled >= len(buffer) {
			flushSize = (bufferFilled / alignment) * alignment
		} else {
			continue
		}

		if flushSize > 0 {
			data := buffer[:flushSize]
			hasher.Write(data)
			writeOffset := totalWritten

			for len(data) > 0 {
				objectNo := writeOffset / s.objectSize
				availableInObject := s.objectSize - (writeOffset % s.objectSize)

				toWrite := uint64(len(data))
				if toWrite > availableInObject {
					toWrite = availableInObject
				}

				objectID := s.getObjectID(object, objectNo)
				slog.Debug("rados.Append", "object", objectID, "size", toWrite, "aligned", toWrite%uint64(alignment) == 0)
				atomic.AddUint64(s.radosCalls, 1)
				if err := s.ioctx.Append(objectID, data[:toWrite]); err != nil {
					return totalRead, [32]byte{}, fmt.Errorf("append to %s: %w", objectID, err)
				}

				data = data[toWrite:]
				writeOffset += toWrite
			}
			totalWritten = writeOffset

			carryover := bufferFilled - flushSize
			if carryover > 0 {
				copy(buffer[:carryover], buffer[flushSize:bufferFilled])
			}
			bufferFilled = carryover
		}

		if isEOF {
			slog.Debug("rados.SetXattr", "object", firstObjID, "xattr", xattrSize)
			atomic.AddUint64(s.radosCalls, 1)
			if err := s.ioctx.SetXattr(firstObjID, xattrSize, []byte(strconv.FormatUint(totalWritten, 10))); err != nil {
				return totalRead, [32]byte{}, fmt.Errorf("set size xattr: %w", err)
			}
			return totalRead, [32]byte(hasher.Sum(nil)), nil
		}
	}
}
