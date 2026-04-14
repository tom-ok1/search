package transport

import (
	"errors"
	"sync"
)

var ErrRejected = errors.New("worker pool rejected: queue full")

// PoolName identifies a named worker pool.
type PoolName string

const (
	PoolGeneric         PoolName = "generic"
	PoolSearch          PoolName = "search"
	PoolIndex           PoolName = "index"
	PoolTransportWorker PoolName = "transport_worker"
	PoolClusterState    PoolName = "cluster_state"
)

// PoolConfig configures a named worker pool.
// Workers == 0 means tasks run inline on the calling goroutine.
type PoolConfig struct {
	Workers   int
	QueueSize int
}

// WorkerPool manages named pools of worker goroutines backed by channels.
type WorkerPool struct {
	queues map[PoolName]chan func()
	wg     sync.WaitGroup
}

func NewWorkerPool(configs map[PoolName]PoolConfig) *WorkerPool {
	wp := &WorkerPool{
		queues: make(map[PoolName]chan func(), len(configs)),
	}
	for name, cfg := range configs {
		if cfg.Workers == 0 {
			wp.queues[name] = nil // nil means run inline
			continue
		}
		ch := make(chan func(), cfg.QueueSize)
		wp.queues[name] = ch
		wp.wg.Add(cfg.Workers)
		for range cfg.Workers {
			go func() {
				defer wp.wg.Done()
				for task := range ch {
					task()
				}
			}()
		}
	}
	return wp
}

// Submit sends a task to the named pool. Falls back to "generic" if the name
// is not found. Returns ErrRejected if the queue is full.
// If the pool is configured with Workers == 0, the task runs inline.
func (wp *WorkerPool) Submit(name PoolName, task func()) error {
	ch, ok := wp.queues[name]
	if !ok {
		ch = wp.queues[PoolGeneric]
	}
	if ch == nil {
		task()
		return nil
	}
	select {
	case ch <- task:
		return nil
	default:
		return ErrRejected
	}
}

func (wp *WorkerPool) Shutdown() {
	for _, ch := range wp.queues {
		if ch != nil {
			close(ch)
		}
	}
	wp.wg.Wait()
}
