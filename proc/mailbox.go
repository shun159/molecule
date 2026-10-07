package proc

import "sync"

// compactThreshold is the number of consumed slots after which the queue
// is shifted down so the backing array does not grow without bound.
const compactThreshold = 1024

// mailbox is an unbounded FIFO queue. Any goroutine may push; only the
// owning process pops.
type mailbox struct {
	mu   sync.Mutex
	q    []any
	head int
	// notify has capacity 1 and is signalled on every push, so a receiver
	// that found the queue empty never misses a wakeup.
	notify chan struct{}
}

func newMailbox() *mailbox {
	return &mailbox{notify: make(chan struct{}, 1)}
}

func (m *mailbox) push(msg any) {
	m.mu.Lock()
	m.q = append(m.q, msg)
	m.mu.Unlock()
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

func (m *mailbox) pop() (any, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.head == len(m.q) {
		return nil, false
	}
	msg := m.q[m.head]
	m.q[m.head] = nil
	m.head++
	switch {
	case m.head == len(m.q):
		m.q, m.head = m.q[:0], 0
	case m.head >= compactThreshold && m.head*2 >= len(m.q):
		n := copy(m.q, m.q[m.head:])
		clear(m.q[n:])
		m.q, m.head = m.q[:n], 0
	}
	return msg, true
}

// remove deletes the queued messages for which match returns true, keeping
// the order of the others.
func (m *mailbox) remove(match func(any) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.q[:m.head]
	for _, msg := range m.q[m.head:] {
		if !match(msg) {
			kept = append(kept, msg)
		}
	}
	clear(m.q[len(kept):])
	m.q = kept
}

func (m *mailbox) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.q) - m.head
}
