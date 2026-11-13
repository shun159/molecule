package proc

import (
	"sync"
	"sync/atomic"
)

// mailbox is a FIFO queue, unbounded unless given a limit. Any goroutine
// may push; only the owning process pops.
//
// The owner takes the messages pushed in batches: under the lock once, it
// swaps the queue pushed to for its own, emptied, then pops from that
// without the lock until it is empty too.
type mailbox struct {
	mu    sync.Mutex
	q     []any // pushed, not yet taken
	max   int   // the most messages waiting, if > 0, see Self.SetMaxMessages
	taken []any // the batch the owner pops from, from head; set under mu
	// notify has capacity 1 and is signalled on every push, so a receiver
	// that found the queue empty never misses a wakeup.
	notify chan struct{}

	// head is the next of taken to pop, of the owner alone, on a cache line
	// of its own: the others read it only to count the messages waiting.
	_    [64]byte
	head atomic.Int64
	_    [56]byte
}

func newMailbox() *mailbox {
	return &mailbox{notify: make(chan struct{}, 1)}
}

// push queues msg, and reports whether the queue is now past its limit.
func (m *mailbox) push(msg any) (full bool) {
	m.mu.Lock()
	m.q = append(m.q, msg)
	// At most all of taken are left: the owner's head is read only if
	// that many would be past the limit.
	mayBeFull := m.max > 0 && len(m.q)+len(m.taken) > m.max
	full = mayBeFull && m.waitingLocked() > m.max
	m.mu.Unlock()
	m.wake()
	return full
}

// wake wakes the owner, if it waits on notify: of a push, or of its death.
func (m *mailbox) wake() {
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

// pop returns the next message, of the owner.
func (m *mailbox) pop() (any, bool) {
	h := int(m.head.Load())
	if h == len(m.taken) {
		if !m.take() {
			return nil, false
		}
		h = 0
	}
	msg := m.taken[h]
	m.taken[h] = nil
	m.head.Store(int64(h + 1))
	return msg, true
}

// take swaps the queue pushed to for taken, emptied, and reports whether
// it got any message.
func (m *mailbox) take() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.q) == 0 {
		return false
	}
	m.q, m.taken = m.taken[:0], m.q
	m.head.Store(0)
	return true
}

// remove deletes the queued messages for which match returns true, keeping
// the order of the others. Of the owner.
func (m *mailbox) remove(match func(any) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := int(m.head.Load())
	m.taken = m.taken[:h+len(filter(m.taken[h:], match))]
	m.q = filter(m.q, match)
}

// filter keeps in place the messages of msgs match returns false for, and
// returns them.
func filter(msgs []any, match func(any) bool) []any {
	kept := msgs[:0]
	for _, msg := range msgs {
		if !match(msg) {
			kept = append(kept, msg)
		}
	}
	clear(msgs[len(kept):])
	return kept
}

// setMax sets the limit, and returns the previous one.
func (m *mailbox) setMax(n int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	prev := m.max
	m.max = n
	return prev
}

// drop empties the queue of a dead process, for its messages to be freed
// before its goroutine notices the death: those not yet taken; the batch
// taken is the owner's, freed with it.
func (m *mailbox) drop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.q)
	m.q = nil
}

func (m *mailbox) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.waitingLocked()
}

// waitingLocked returns the number of messages waiting, under mu.
func (m *mailbox) waitingLocked() int {
	return len(m.q) + len(m.taken) - int(m.head.Load())
}
