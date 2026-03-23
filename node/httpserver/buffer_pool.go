package httpserver

import (
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
)

// GetBuffer gets a buffer from the pool based on size
// This is a wrapper around the node package buffer pool
func GetBuffer(size int) *[]byte {
	return node.GetBuffer(size)
}

// PutBuffer returns a buffer to the pool
// This is a wrapper around the node package buffer pool
func PutBuffer(buf *[]byte) {
	node.PutBuffer(buf)
}
