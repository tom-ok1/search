package index

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolGetAndReturn(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	dwpt1 := pool.getAndLock()
	if dwpt1 == nil {
		t.Fatal("expected non-nil DWPT")
	}
	pool.returnAndUnlock(dwpt1)

	dwpt2 := pool.getAndLock()
	if dwpt2 == nil {
		t.Fatal("expected non-nil DWPT")
	}
	pool.returnAndUnlock(dwpt2)
}

func TestPoolConcurrentCheckout(t *testing.T) {
	var counter atomic.Int32
	pool := newPerThreadPool(func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	}, newTestFieldAnalyzers(), newDeleteQueue())

	const N = 8
	var wg sync.WaitGroup
	dwpts := make([]*DocumentsWriterPerThread, N)

	// All goroutines checkout simultaneously
	barrier := make(chan struct{})
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-barrier
			dwpts[idx] = pool.getAndLock()
		}(i)
	}
	close(barrier)
	wg.Wait()

	// All should be distinct
	seen := make(map[*DocumentsWriterPerThread]bool)
	for i, d := range dwpts {
		if d == nil {
			t.Fatalf("dwpt[%d] is nil", i)
		}
		if seen[d] {
			t.Errorf("dwpt[%d] is a duplicate", i)
		}
		seen[d] = true
		pool.returnAndUnlock(d)
	}
}

func TestPoolRemove(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	dwpt := pool.getAndLock()
	pool.remove(dwpt)

	dwpt2 := pool.getAndLock()
	if dwpt2 == nil {
		t.Fatal("expected non-nil DWPT after remove")
	}
	pool.returnAndUnlock(dwpt2)
}

func TestPoolFullFlushOnlyFree(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	d1 := pool.getAndLock()
	d2 := pool.getAndLock()
	pool.returnAndUnlock(d1)
	pool.returnAndUnlock(d2)

	freed := pool.drainFreeAndMarkActive()
	if len(freed) != 0 {
		t.Fatalf("expected 0 freed DWPTs from sync.Pool drain, got %d", len(freed))
	}

	returned := pool.waitAndDrainActive()
	if len(returned) != 0 {
		t.Errorf("expected 0 returned DWPTs, got %d", len(returned))
	}
}

func TestPoolFullFlushWaitsForActive(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	// d1 is free, d2 is active
	d1 := pool.getAndLock()
	d2 := pool.getAndLock()
	pool.returnAndUnlock(d1)

	freed := pool.drainFreeAndMarkActive()
	if len(freed) != 0 {
		t.Fatalf("expected 0 freed DWPTs (sync.Pool), got %d", len(freed))
	}

	// waitAndDrainActive should block until d2 is returned
	done := make(chan []*DocumentsWriterPerThread, 1)
	go func() {
		done <- pool.waitAndDrainActive()
	}()

	// Should be blocked
	select {
	case <-done:
		t.Fatal("waitAndDrainActive should block while d2 is active")
	case <-time.After(50 * time.Millisecond):
	}

	// Return d2
	pool.returnAndUnlock(d2)

	select {
	case returned := <-done:
		if len(returned) != 1 {
			t.Errorf("expected 1 returned DWPT, got %d", len(returned))
		}
		if returned[0] != d2 {
			t.Error("expected returned DWPT to be d2")
		}
	case <-time.After(time.Second):
		t.Fatal("waitAndDrainActive timed out")
	}

	// fullFlush should be off; new getAndLock should work normally
	d3 := pool.getAndLock()
	pool.returnAndUnlock(d3)
}

func TestPoolFullFlushReturnRouting(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	// d1 active
	d1 := pool.getAndLock()

	pool.drainFreeAndMarkActive()

	// Return d1 during full flush — should NOT go to free list
	pool.returnAndUnlock(d1)

	returned := pool.waitAndDrainActive()
	if len(returned) != 1 {
		t.Fatalf("expected 1, got %d", len(returned))
	}

	d2 := pool.getAndLock()
	if d2 == nil {
		t.Fatal("expected non-nil DWPT after full flush")
	}
	pool.returnAndUnlock(d2)
}

func TestPoolFullFlushRemoveCountsAsReturn(t *testing.T) {
	counter := 0
	pool := newPerThreadPool(func() string {
		name := fmt.Sprintf("_seg%d", counter)
		counter++
		return name
	}, newTestFieldAnalyzers(), newDeleteQueue())

	d1 := pool.getAndLock()
	pool.drainFreeAndMarkActive()

	done := make(chan []*DocumentsWriterPerThread, 1)
	go func() {
		done <- pool.waitAndDrainActive()
	}()

	// remove (used when addDocument triggers individual flush) should also unblock
	pool.remove(d1)

	select {
	case returned := <-done:
		// d1 was removed, not returned, so it won't appear in flushOnReturn
		if len(returned) != 0 {
			t.Errorf("expected 0 returned (d1 was removed), got %d", len(returned))
		}
	case <-time.After(time.Second):
		t.Fatal("waitAndDrainActive timed out after remove")
	}
}

func TestPoolFullFlushIgnoresNewDWPTs(t *testing.T) {
	var counter atomic.Int32
	pool := newPerThreadPool(func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	}, newTestFieldAnalyzers(), newDeleteQueue())

	// d1 is active when full flush starts
	d1 := pool.getAndLock()
	pool.drainFreeAndMarkActive()

	done := make(chan []*DocumentsWriterPerThread, 1)
	go func() {
		done <- pool.waitAndDrainActive()
	}()

	// New DWPT created and returned during full flush — should go to sync.Pool, not block flush
	d2 := pool.getAndLock()
	pool.returnAndUnlock(d2)

	// Still blocked because d1 hasn't been returned
	select {
	case <-done:
		t.Fatal("waitAndDrainActive should still block — d1 is not returned yet")
	case <-time.After(50 * time.Millisecond):
	}

	// Return d1 — now full flush should complete
	pool.returnAndUnlock(d1)

	select {
	case returned := <-done:
		if len(returned) != 1 {
			t.Errorf("expected 1 returned DWPT, got %d", len(returned))
		}
		if returned[0] != d1 {
			t.Error("expected returned DWPT to be d1")
		}
	case <-time.After(time.Second):
		t.Fatal("waitAndDrainActive timed out")
	}

	// d2 should be reusable from sync.Pool
	d3 := pool.getAndLock()
	if d3 == nil {
		t.Fatal("expected non-nil DWPT")
	}
	pool.returnAndUnlock(d3)
}

func TestPoolSyncPoolFastPath(t *testing.T) {
	var counter atomic.Int32
	pool := newPerThreadPool(func() string {
		n := counter.Add(1)
		return fmt.Sprintf("_seg%d", n)
	}, newTestFieldAnalyzers(), newDeleteQueue())

	d1 := pool.getAndLock()
	if d1 == nil {
		t.Fatal("expected non-nil DWPT")
	}
	pool.returnAndUnlock(d1)

	d2 := pool.getAndLock()
	if d2 == nil {
		t.Fatal("expected non-nil DWPT from pool")
	}
	pool.returnAndUnlock(d2)
}
