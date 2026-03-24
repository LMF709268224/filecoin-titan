package node

import (
	"sync"
	"sync/atomic"

	"github.com/Filecoin-Titan/titan/api/types"
)

// WorkerPool implements a simple goroutine pool to limit concurrency
type WorkerPool struct {
	sem chan struct{}
	wg  sync.WaitGroup
}

// NewWorkerPool creates a new worker pool with the specified max workers
func NewWorkerPool(maxWorkers int) *WorkerPool {
	return &WorkerPool{
		sem: make(chan struct{}, maxWorkers),
	}
}

// Submit submits a task to the WorkerPool
func (p *WorkerPool) Submit(task func()) {
	p.wg.Add(1)
	p.sem <- struct{}{} // acquire semaphore

	go func() {
		defer func() {
			<-p.sem // release semaphore
			p.wg.Done()
		}()
		task()
	}()
}

// Wait waits for all tasks to complete
func (p *WorkerPool) Wait() {
	p.wg.Wait()
}

// globalWorkerPool is a singleton WorkerPool shared across the node package
var (
	globalWorkerPool     *WorkerPool
	globalWorkerPoolOnce sync.Once
	globalPoolSize       int32 = 200 // default size
)

// GetGlobalWorkerPool returns the global WorkerPool instance
func GetGlobalWorkerPool() *WorkerPool {
	globalWorkerPoolOnce.Do(func() {
		size := int(atomic.LoadInt32(&globalPoolSize))
		globalWorkerPool = NewWorkerPool(size)
	})
	return globalWorkerPool
}

// Slice pool for string slices
var stringSlicePool = sync.Pool{
	New: func() interface{} {
		s := make([]string, 0, 100)
		return &s
	},
}

// GetStringSlice gets a string slice from the pool
func GetStringSlice() *[]string {
	return stringSlicePool.Get().(*[]string)
}

// PutStringSlice returns a string slice to the pool
func PutStringSlice(s *[]string) {
	if s != nil {
		*s = (*s)[:0] // reset length
		stringSlicePool.Put(s)
	}
}

// Buffer pools for various sizes
var (

	// Pool for NodeDynamicInfo slices
	DynamicInfoSlicePool = sync.Pool{
		New: func() interface{} {
			s := make([]types.NodeDynamicInfo, 0, 1000)
			return &s
		},
	}

	// Pool for ProfitDetails slices
	ProfitDetailsSlicePool = sync.Pool{
		New: func() interface{} {
			s := make([]*types.ProfitDetails, 0, 1000)
			return &s
		},
	}
)

// GetDynamicInfoSlice gets a NodeDynamicInfo slice from the pool
func GetDynamicInfoSlice() *[]types.NodeDynamicInfo {
	return DynamicInfoSlicePool.Get().(*[]types.NodeDynamicInfo)
}

// PutDynamicInfoSlice returns a NodeDynamicInfo slice to the pool
func PutDynamicInfoSlice(s *[]types.NodeDynamicInfo) {
	if s != nil {
		*s = (*s)[:0] // reset length
		DynamicInfoSlicePool.Put(s)
	}
}

// GetProfitDetailsSlice gets a ProfitDetails slice from the pool
func GetProfitDetailsSlice() *[]*types.ProfitDetails {
	return ProfitDetailsSlicePool.Get().(*[]*types.ProfitDetails)
}

// PutProfitDetailsSlice returns a ProfitDetails slice to the pool
func PutProfitDetailsSlice(s *[]*types.ProfitDetails) {
	if s != nil {
		*s = (*s)[:0] // reset length
		ProfitDetailsSlicePool.Put(s)
	}
}
