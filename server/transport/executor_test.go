package transport

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDirectExecutor(t *testing.T) {
	exec := &DirectExecutor{}
	var executed bool
	err := exec.Execute(func() {
		executed = true
	})
	if err != nil {
		t.Fatalf("DirectExecutor.Execute() returned error: %v", err)
	}
	if !executed {
		t.Fatal("DirectExecutor did not run task inline")
	}
	exec.Shutdown() // should be no-op
}

func TestBoundedExecutor_ExecutesTasks(t *testing.T) {
	exec := NewBoundedExecutor(2, 10)
	defer exec.Shutdown()

	var count atomic.Int32
	for range 5 {
		err := exec.Execute(func() {
			count.Add(1)
		})
		if err != nil {
			t.Fatalf("Execute() returned error: %v", err)
		}
	}

	// Wait for tasks to complete
	time.Sleep(50 * time.Millisecond)
	if got := count.Load(); got != 5 {
		t.Errorf("Expected 5 tasks to run, got %d", got)
	}
}

func TestBoundedExecutor_Backpressure(t *testing.T) {
	exec := NewBoundedExecutor(1, 1)
	defer exec.Shutdown()

	// Block the worker
	unblock := make(chan struct{})
	workerStarted := make(chan struct{})
	err := exec.Execute(func() {
		close(workerStarted)
		<-unblock
	})
	if err != nil {
		t.Fatalf("First Execute() failed: %v", err)
	}

	// Wait for worker to start
	<-workerStarted

	// Fill the queue
	err = exec.Execute(func() {})
	if err != nil {
		t.Fatalf("Second Execute() failed: %v", err)
	}

	// Third task should be rejected
	err = exec.Execute(func() {})
	if err != ErrRejected {
		t.Errorf("Expected ErrRejected, got %v", err)
	}

	close(unblock)
}

func TestBoundedExecutor_ShutdownDrains(t *testing.T) {
	exec := NewBoundedExecutor(2, 10)

	var count atomic.Int32
	for range 10 {
		exec.Execute(func() {
			time.Sleep(10 * time.Millisecond)
			count.Add(1)
		})
	}

	exec.Shutdown()
	if got := count.Load(); got != 10 {
		t.Errorf("Expected all 10 tasks to complete after Shutdown(), got %d", got)
	}
}

func TestThreadPool_Get(t *testing.T) {
	tp := NewThreadPool(map[string]PoolConfig{
		"generic": {Workers: 2, QueueSize: 10},
		"search":  {Workers: 4, QueueSize: 20},
	})
	defer tp.Shutdown()

	searchExec := tp.Get("search")
	if searchExec == nil {
		t.Fatal("Expected search executor to exist")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	err := searchExec.Execute(func() {
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Execute() on search pool failed: %v", err)
	}
	wg.Wait()
}

func TestThreadPool_GetFallback(t *testing.T) {
	tp := NewThreadPool(map[string]PoolConfig{
		"generic": {Workers: 2, QueueSize: 10},
	})
	defer tp.Shutdown()

	exec := tp.Get("nonexistent")
	if exec == nil {
		t.Fatal("Expected fallback to generic executor")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	err := exec.Execute(func() {
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Execute() on fallback pool failed: %v", err)
	}
	wg.Wait()
}

func TestThreadPool_TransportWorkerInline(t *testing.T) {
	tp := NewThreadPool(map[string]PoolConfig{
		"generic":          {Workers: 2, QueueSize: 10},
		"transport_worker": {Workers: 0, QueueSize: 0},
	})
	defer tp.Shutdown()

	exec := tp.Get("transport_worker")
	_, isDirectExec := exec.(*DirectExecutor)
	if !isDirectExec {
		t.Fatal("Expected transport_worker pool to be a DirectExecutor")
	}

	var executed bool
	err := exec.Execute(func() {
		executed = true
	})
	if err != nil {
		t.Fatalf("Execute() on transport_worker pool failed: %v", err)
	}
	if !executed {
		t.Fatal("transport_worker pool did not run task inline")
	}
}
