package node

import (
	"math"
	"sync"

	"github.com/docker/go-units"
)

// BandwidthTracker represents the bandwidth tracker for a node
type BandwidthTracker struct {
	bandwidthDowns []int64
	bandwidthUps   []int64
	mu             sync.Mutex
	maxSize        int16
	downIndex      int16
	upIndex        int16
}

// NewBandwidthTracker creates a new NodeBandwidth instance
func NewBandwidthTracker(size int) *BandwidthTracker {
	nb := &BandwidthTracker{
		maxSize:        int16(size),
		bandwidthDowns: make([]int64, 0, size),
		bandwidthUps:   make([]int64, 0, size),
	}

	nb.PutBandwidthDown(units.GiB)
	nb.PutBandwidthUp(units.MiB)

	return nb
}

// PutBandwidthDown adds a new download speed to the tracker
func (nst *BandwidthTracker) PutBandwidthDown(speed int64) int64 {
	if speed <= 0 {
		return nst.getAverageBandwidthDown()
	}

	nst.mu.Lock()
	defer nst.mu.Unlock()

	if len(nst.bandwidthDowns) < int(nst.maxSize) {
		nst.bandwidthDowns = append(nst.bandwidthDowns, speed)
	} else {
		nst.bandwidthDowns[nst.downIndex] = speed
		nst.downIndex = (nst.downIndex + 1) % nst.maxSize
	}

	return nst.getAverageBandwidthDownLocked()
}

// getAverageBandwidthDown calculates and returns the average download speed
func (nst *BandwidthTracker) getAverageBandwidthDown() int64 {
	nst.mu.Lock()
	defer nst.mu.Unlock()

	return nst.getAverageBandwidthDownLocked()
}

func (nst *BandwidthTracker) getAverageBandwidthDownLocked() int64 {
	if len(nst.bandwidthDowns) == 0 {
		return 0
	}

	if len(nst.bandwidthDowns) == 1 {
		return nst.bandwidthDowns[0]
	}

	sum := int64(0)
	minSpeed := int64(math.MaxInt64)
	for _, speed := range nst.bandwidthDowns {
		if speed < minSpeed {
			minSpeed = speed
		}

		sum += speed
	}

	count := int64(len(nst.bandwidthDowns))

	adjustedSum := sum - minSpeed
	adjustedCount := count - 1

	return adjustedSum / adjustedCount
}

// PutBandwidthUp adds a new upload speed to the tracker
func (nst *BandwidthTracker) PutBandwidthUp(speed int64) int64 {
	if speed <= 0 {
		return nst.getAverageBandwidthUp()
	}

	nst.mu.Lock()
	defer nst.mu.Unlock()

	if len(nst.bandwidthUps) < int(nst.maxSize) {
		nst.bandwidthUps = append(nst.bandwidthUps, speed)
	} else {
		nst.bandwidthUps[nst.upIndex] = speed
		nst.upIndex = (nst.upIndex + 1) % nst.maxSize
	}

	return nst.getAverageBandwidthUpLocked()
}

// getAverageBandwidthUp calculates and returns the average upload speed
func (nst *BandwidthTracker) getAverageBandwidthUp() int64 {
	nst.mu.Lock()
	defer nst.mu.Unlock()

	return nst.getAverageBandwidthUpLocked()
}

func (nst *BandwidthTracker) getAverageBandwidthUpLocked() int64 {
	if len(nst.bandwidthUps) == 0 {
		return 0
	}

	if len(nst.bandwidthUps) == 1 {
		return nst.bandwidthUps[0]
	}

	sum := int64(0)
	minSpeed := int64(math.MaxInt64)
	for _, speed := range nst.bandwidthUps {
		if speed < minSpeed {
			minSpeed = speed
		}

		sum += speed
	}

	count := int64(len(nst.bandwidthUps))

	adjustedSum := sum - minSpeed
	adjustedCount := count - 1

	return adjustedSum / adjustedCount
}
