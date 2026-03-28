package index

import (
	"sync"
	"testing"
)

func TestTicketQueueOrdering(t *testing.T) {
	q := newFlushTicketQueue()

	t1 := q.addTicket()
	t2 := q.addTicket()
	t3 := q.addTicket()

	// Complete out of order: #2, #1, #3
	q.markDone(t2, &SegmentCommitInfo{Name: "seg2"}, nil, nil)

	// Only t2 is done, but t1 is first — nothing should publish
	published := q.publishCompleted()
	if len(published) != 0 {
		t.Errorf("expected 0 published (t1 not done), got %d", len(published))
	}

	q.markDone(t1, &SegmentCommitInfo{Name: "seg1"}, nil, nil)

	// Now t1 and t2 are done — both should publish in order
	published = q.publishCompleted()
	if len(published) != 2 {
		t.Fatalf("expected 2 published, got %d", len(published))
	}
	if published[0].result.Name != "seg1" {
		t.Errorf("first published should be seg1, got %s", published[0].result.Name)
	}
	if published[1].result.Name != "seg2" {
		t.Errorf("second published should be seg2, got %s", published[1].result.Name)
	}

	// t3 not done yet
	published = q.publishCompleted()
	if len(published) != 0 {
		t.Errorf("expected 0 published (t3 not done), got %d", len(published))
	}

	q.markDone(t3, &SegmentCommitInfo{Name: "seg3"}, nil, nil)
	published = q.publishCompleted()
	if len(published) != 1 || published[0].result.Name != "seg3" {
		t.Error("expected seg3 to be published")
	}
}

func TestTicketQueueBlocksOnIncomplete(t *testing.T) {
	q := newFlushTicketQueue()

	t1 := q.addTicket()
	t2 := q.addTicket()

	// t2 done but t1 not
	q.markDone(t2, &SegmentCommitInfo{Name: "seg2"}, nil, nil)

	published := q.publishCompleted()
	if len(published) != 0 {
		t.Errorf("should not publish when head ticket incomplete, got %d", len(published))
	}

	// Now complete t1
	q.markDone(t1, &SegmentCommitInfo{Name: "seg1"}, nil, nil)

	published = q.publishCompleted()
	if len(published) != 2 {
		t.Errorf("expected 2 published after completing head, got %d", len(published))
	}
}

func TestTicketQueueConcurrentMarkDone(t *testing.T) {
	q := newFlushTicketQueue()

	const N = 10
	tickets := make([]*FlushTicket, N)
	for i := range N {
		tickets[i] = q.addTicket()
	}

	// Complete all tickets concurrently
	var wg sync.WaitGroup
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			q.markDone(tickets[idx], &SegmentCommitInfo{Name: "seg"}, nil, nil)
		}(i)
	}
	wg.Wait()

	// All should be publishable
	published := q.publishCompleted()
	if len(published) != N {
		t.Errorf("expected %d published, got %d", N, len(published))
	}
}
