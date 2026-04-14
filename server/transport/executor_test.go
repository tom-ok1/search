package transport

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool_InlineExecution(t *testing.T) {
	wp := NewWorkerPool(map[PoolName]PoolConfig{
		PoolGeneric: {Workers: 0},
	})
	defer wp.Shutdown()

	var executed bool
	err := wp.Submit(PoolGeneric, func() {
		executed = true
	})
	if err != nil {
		t.Fatalf("Submit() returned error: %v", err)
	}
	if !executed {
		t.Fatal("inline pool did not run task synchronously")
	}
}

func TestWorkerPool_ExecutesTasks(t *testing.T) {
	wp := NewWorkerPool(map[PoolName]PoolConfig{
		PoolGeneric: {Workers: 2, QueueSize: 10},
	})
	defer wp.Shutdown()

	var count atomic.Int32
	for range 5 {
		err := wp.Submit(PoolGeneric, func() {
			count.Add(1)
		})
		if err != nil {
			t.Fatalf("Submit() returned error: %v", err)
		}
	}

	// Wait for tasks to complete
	time.Sleep(50 * time.Millisecond)
	if got := count.Load(); got != 5 {
		t.Errorf("Expected 5 tasks to run, got %d", got)
	}
}

func TestWorkerPool_Backpressure(t *testing.T) {
	wp := NewWorkerPool(map[PoolName]PoolConfig{
		PoolGeneric: {Workers: 1, QueueSize: 1},
	})
	defer wp.Shutdown()

	// Block the worker
	unblock := make(chan struct{})
	workerStarted := make(chan struct{})
	err := wp.Submit(PoolGeneric, func() {
		close(workerStarted)
		<-unblock
	})
	if err != nil {
		t.Fatalf("First Submit() failed: %v", err)
	}

	// Wait for worker to start
	<-workerStarted

	// Fill the queue
	err = wp.Submit(PoolGeneric, func() {})
	if err != nil {
		t.Fatalf("Second Submit() failed: %v", err)
	}

	// Third task should be rejected
	err = wp.Submit(PoolGeneric, func() {})
	if err != ErrRejected {
		t.Errorf("Expected ErrRejected, got %v", err)
	}

	close(unblock)
}

func TestWorkerPool_ShutdownDrains(t *testing.T) {
	wp := NewWorkerPool(map[PoolName]PoolConfig{
		PoolGeneric: {Workers: 2, QueueSize: 10},
	})

	var count atomic.Int32
	for range 10 {
		wp.Submit(PoolGeneric, func() {
			time.Sleep(10 * time.Millisecond)
			count.Add(1)
		})
	}

	wp.Shutdown()
	if got := count.Load(); got != 10 {
		t.Errorf("Expected all 10 tasks to complete after Shutdown(), got %d", got)
	}
}

func TestWorkerPool_NamedPools(t *testing.T) {
	wp := NewWorkerPool(map[PoolName]PoolConfig{
		PoolGeneric: {Workers: 2, QueueSize: 10},
		PoolSearch:  {Workers: 4, QueueSize: 20},
	})
	defer wp.Shutdown()

	var wg sync.WaitGroup
	wg.Add(1)
	err := wp.Submit(PoolSearch, func() {
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Submit() on search pool failed: %v", err)
	}
	wg.Wait()
}

func TestWorkerPool_FallbackToGeneric(t *testing.T) {
	wp := NewWorkerPool(map[PoolName]PoolConfig{
		PoolGeneric: {Workers: 2, QueueSize: 10},
	})
	defer wp.Shutdown()

	var wg sync.WaitGroup
	wg.Add(1)
	err := wp.Submit("nonexistent", func() {
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Submit() on fallback pool failed: %v", err)
	}
	wg.Wait()
}

func TestWorkerPool_InlinePool(t *testing.T) {
	wp := NewWorkerPool(map[PoolName]PoolConfig{
		PoolGeneric:         {Workers: 2, QueueSize: 10},
		PoolTransportWorker: {Workers: 0},
	})
	defer wp.Shutdown()

	var executed bool
	err := wp.Submit(PoolTransportWorker, func() {
		executed = true
	})
	if err != nil {
		t.Fatalf("Submit() on transport_worker pool failed: %v", err)
	}
	if !executed {
		t.Fatal("transport_worker pool did not run task inline")
	}
}
