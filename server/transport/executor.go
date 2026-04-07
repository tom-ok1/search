package transport

import (
	"errors"
	"sync"
)

var ErrRejected = errors.New("executor rejected: queue full")

// Executor runs tasks in a controlled concurrency context.
type Executor interface {
	Execute(task func()) error
	Shutdown()
}

// DirectExecutor runs tasks inline on the calling goroutine.
type DirectExecutor struct{}

func (d *DirectExecutor) Execute(task func()) error {
	task()
	return nil
}

func (d *DirectExecutor) Shutdown() {}

// BoundedExecutor runs tasks on a fixed pool of worker goroutines
// with a bounded queue for backpressure.
type BoundedExecutor struct {
	queue chan func()
	wg    sync.WaitGroup
}

func NewBoundedExecutor(workers, queueSize int) *BoundedExecutor {
	e := &BoundedExecutor{
		queue: make(chan func(), queueSize),
	}
	e.wg.Add(workers)
	for range workers {
		go func() {
			defer e.wg.Done()
			for task := range e.queue {
				task()
			}
		}()
	}
	return e
}

func (e *BoundedExecutor) Execute(task func()) error {
	select {
	case e.queue <- task:
		return nil
	default:
		return ErrRejected
	}
}

func (e *BoundedExecutor) Shutdown() {
	close(e.queue)
	e.wg.Wait()
}

// PoolConfig configures a named executor pool.
// Workers == 0 means DirectExecutor (inline).
type PoolConfig struct {
	Workers   int
	QueueSize int
}

// ThreadPool manages named executor pools.
type ThreadPool struct {
	pools map[string]Executor
}

func NewThreadPool(configs map[string]PoolConfig) *ThreadPool {
	pools := make(map[string]Executor, len(configs))
	for name, cfg := range configs {
		if cfg.Workers == 0 {
			pools[name] = &DirectExecutor{}
		} else {
			pools[name] = NewBoundedExecutor(cfg.Workers, cfg.QueueSize)
		}
	}
	return &ThreadPool{pools: pools}
}

// Get returns the named executor, falling back to "generic" if not found.
func (tp *ThreadPool) Get(name string) Executor {
	if e, ok := tp.pools[name]; ok {
		return e
	}
	return tp.pools["generic"]
}

func (tp *ThreadPool) Shutdown() {
	for _, e := range tp.pools {
		e.Shutdown()
	}
}
