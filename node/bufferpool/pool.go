package bufferpool

import "sync"

// Buffer pools for various sizes
var (
	SmallBufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 4096)
			return &buf
		},
	}

	MediumBufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 32768) // 32KB
			return &buf
		},
	}

	LargeBufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 262144) // 256KB
			return &buf
		},
	}

	XLargeBufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 1048576) // 1MB
			return &buf
		},
	}
)

// GetBuffer gets a buffer from the appropriate pool
func GetBuffer(size int) *[]byte {
	if size <= 4096 {
		return SmallBufferPool.Get().(*[]byte)
	} else if size <= 32768 {
		return MediumBufferPool.Get().(*[]byte)
	} else if size <= 262144 {
		return LargeBufferPool.Get().(*[]byte)
	}
	return XLargeBufferPool.Get().(*[]byte)
}

// PutBuffer returns a buffer to the appropriate pool
func PutBuffer(buf *[]byte) {
	if buf == nil {
		return
	}

	switch cap(*buf) {
	case 4096:
		SmallBufferPool.Put(buf)
	case 32768:
		MediumBufferPool.Put(buf)
	case 262144:
		LargeBufferPool.Put(buf)
	case 1048576:
		XLargeBufferPool.Put(buf)
	}
}
