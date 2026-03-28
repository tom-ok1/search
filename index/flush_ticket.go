package index

import "sync"

// FlushTicket represents a pending flush operation. Tickets are completed
// out of order but published in FIFO order to preserve segment ordering.
type FlushTicket struct {
	result        *SegmentCommitInfo
	globalUpdates *FrozenBufferedUpdates
	err           error
	done          chan struct{}
}

// FlushTicketQueue ensures flushed segments are published in the order
// they were enqueued, even when flushes complete out of order.
type FlushTicketQueue struct {
	mu      sync.Mutex
	tickets []*FlushTicket
}

func newFlushTicketQueue() *FlushTicketQueue {
	return &FlushTicketQueue{}
}

// addTicket creates and enqueues a new ticket. Returns the ticket.
func (q *FlushTicketQueue) addTicket() *FlushTicket {
	q.mu.Lock()
	defer q.mu.Unlock()

	ticket := &FlushTicket{
		done: make(chan struct{}),
	}
	q.tickets = append(q.tickets, ticket)
	return ticket
}

// markDone completes a ticket with the given result.
func (q *FlushTicketQueue) markDone(ticket *FlushTicket, info *SegmentCommitInfo, globalUpdates *FrozenBufferedUpdates, err error) {
	ticket.result = info
	ticket.globalUpdates = globalUpdates
	ticket.err = err
	close(ticket.done)
}

// publishCompleted returns all completed tickets from the head of the queue,
// stopping at the first incomplete ticket. This ensures FIFO ordering.
func (q *FlushTicketQueue) publishCompleted() []*FlushTicket {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.tickets) == 0 {
		return nil
	}

	var published []*FlushTicket
	for len(q.tickets) > 0 {
		ticket := q.tickets[0]
		select {
		case <-ticket.done:
			published = append(published, ticket)
			q.tickets = q.tickets[1:]
		default:
			return published
		}
	}
	return published
}
