package httpserver

import (
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"
)

// GetBuffer gets a buffer from the pool based on size
// This is a wrapper around the assets package buffer pool
func GetBuffer(size int) *[]byte {
	return assets.GetBuffer(size)
}

// PutBuffer returns a buffer to the pool
// This is a wrapper around the assets package buffer pool
func PutBuffer(buf *[]byte) {
	assets.PutBuffer(buf)
}
