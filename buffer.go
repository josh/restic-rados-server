package main

import "sync"

type BufferPool struct {
	pool *sync.Pool
	size int64
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
