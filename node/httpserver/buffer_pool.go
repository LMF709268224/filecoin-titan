package httpserver

import (
	"github.com/Filecoin-Titan/titan/node/bufferpool"
)

// GetBuffer gets a buffer from the pool based on size
// This is a wrapper around the node package buffer pool
func GetBuffer(size int) *[]byte {
	return bufferpool.GetBuffer(size)
}

// PutBuffer returns a buffer to the pool
// This is a wrapper around the node package buffer pool
func PutBuffer(buf *[]byte) {
	bufferpool.PutBuffer(buf)
}
