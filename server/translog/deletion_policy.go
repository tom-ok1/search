package translog

import "sync"

// TranslogDeletionPolicy manages retention of translog generations.
// It tracks retention locks and the minimum required generation.
type TranslogDeletionPolicy struct {
	mu             sync.Mutex
	retentionLocks map[int64]int // generation -> refcount
	minRequiredGen int64
}

// NewTranslogDeletionPolicy creates a new deletion policy.
func NewTranslogDeletionPolicy(minGen int64) *TranslogDeletionPolicy {
	return &TranslogDeletionPolicy{
		retentionLocks: make(map[int64]int),
		minRequiredGen: minGen,
	}
}

// RetentionLock is a handle that holds a generation retained.
type RetentionLock struct {
	policy     *TranslogDeletionPolicy
	generation int64
}

// Release decrements the refcount for this lock's generation.
func (rl *RetentionLock) Release() {
	rl.policy.release(rl.generation)
}

// AcquireRetentionLock increments the refcount for the given generation
// and returns a releasable lock.
func (p *TranslogDeletionPolicy) AcquireRetentionLock(generation int64) *RetentionLock {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.retentionLocks[generation]++
	return &RetentionLock{policy: p, generation: generation}
}

func (p *TranslogDeletionPolicy) release(generation int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.retentionLocks[generation]--
	if p.retentionLocks[generation] <= 0 {
		delete(p.retentionLocks, generation)
	}
}

// SetMinRequiredGeneration advances the safe-to-delete boundary.
func (p *TranslogDeletionPolicy) SetMinRequiredGeneration(gen int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.minRequiredGen = gen
}

// MinTranslogGenRequired returns the minimum generation that must be kept.
// It's the minimum of minRequiredGen and the lowest locked generation.
func (p *TranslogDeletionPolicy) MinTranslogGenRequired() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	min := p.minRequiredGen
	for gen := range p.retentionLocks {
		if gen < min {
			min = gen
		}
	}
	return min
}
