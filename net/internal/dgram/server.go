package dgram

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// Messages to the socket process from itself.
type (
	readFailed  struct{ err error }
	recvTimeout struct{ seq uint64 }
)

// datagram is one datagram received.
type datagram[A any] struct {
	from A
	data []byte
}

// server is the process of a socket: it owns the connection, and decides
// who gets the datagrams. The reader reads, in a goroutine of its own, only
// while someone wants a datagram, and hands each to the owner itself.
type server[A, S any] struct {
	k     *Kind[A, S]
	self  *proc.Self
	conn  Conn[A]
	sock  S
	max   int
	watch proc.Ref

	// mu orders what the reader hands out with what the process does: both
	// take it to look at who wants a datagram and to send to the owner, so
	// that the owner gets messages in the order they were decided.
	mu     sync.Mutex
	owner  proc.PID
	active Active
	recv   *pendingRecv
	// held is a datagram read for a demand gone by the time it arrived,
	// kept for the next. The reader reads only while nothing is held, so
	// there is at most one.
	held   *datagram[A]
	failed error         // reading failed: close once the owner is told
	wake   chan struct{} // tells the reader someone may want a datagram

	recvSeq uint64
}

type pendingRecv struct {
	from   actor.From
	seq    uint64
	cancel func()
}

// Serve runs the process of the socket sock over conn, owned by owner,
// active as active, dropping datagrams larger than max. It closes conn when
// it ends.
//
//abc:disable abc,cyclo the loop over every message of a socket
//abc:hot net/genudp/BenchmarkEcho
func Serve[A, S any](self *proc.Self, k *Kind[A, S], conn Conn[A], sock S, owner proc.PID, active Active, max int, label string) error {
	self.SetLabel(k.Name + " " + label)
	s := &server[A, S]{
		k:      k,
		self:   self,
		conn:   conn,
		sock:   sock,
		max:    max,
		owner:  owner,
		watch:  self.Monitor(owner),
		active: active,
		wake:   make(chan struct{}, 1),
	}
	// Closing the connection also ends a read in progress once the process
	// is dead.
	context.AfterFunc(self.Context(), func() { s.conn.Close() })
	defer s.conn.Close()
	go s.read()
	s.poke()

	for {
		msg, err := self.Receive(context.Background())
		if err != nil {
			return err
		}
		var stop bool
		switch m := msg.(type) {
		case actor.CallMsg:
			stop = s.call(m)
		case SendReq[A], SetActiveReq, CloseReq:
			// From the effects of a behaviour, with no one to reply to.
			_, _, stop = s.handle(actor.From{}, m)
		case readFailed:
			s.readFailed(m.err)
		case recvTimeout:
			s.recvTimedOut(m.seq)
		case proc.DownMsg:
			if m.Ref == s.watch {
				return nil // the owner is gone, and with it the socket
			}
		}
		if stop || s.deliver() {
			return nil
		}
	}
}

// call handles the call m, replying unless the reply comes later, and
// reports whether the socket is to close.
func (s *server[A, S]) call(m actor.CallMsg) (stop bool) {
	reply, deferred, stop := s.handle(m.From, m.Req)
	if !deferred {
		actor.SendReply(s.self, m.From, reply)
	}
	return stop
}

// readFailed notes the failure of reading, told once what came before is.
func (s *server[A, S]) readFailed(err error) {
	s.mu.Lock()
	s.failed = err
	s.mu.Unlock()
}

// recvTimedOut ends the Recv of seq, if still waiting, with ErrTimeout.
func (s *server[A, S]) recvTimedOut(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recv != nil && s.recv.seq == seq {
		actor.SendReply(s.self, s.recv.from, s.k.ErrTimeout)
		s.recv = nil
	}
}

// handle handles a request; requests sent as effects have a zero from. It
// returns the reply, whether it comes later, and whether the socket is to
// close.
func (s *server[A, S]) handle(from actor.From, req any) (reply any, deferred, stop bool) {
	switch r := req.(type) {
	case SendReq[A]:
		err := Write(s.k, s.conn, r.To, r.Data)
		if r.Then {
			s.setActive(r.Active)
		}
		return err, false, false
	case RecvReq:
		if err := s.waitRecv(from, r); err != nil {
			return err, false, false
		}
		return nil, true, false
	case SetActiveReq:
		s.setActive(r.Active)
		return nil, false, false
	case ControlReq:
		return s.control(from, r.Owner), false, false
	case CloseReq:
		return nil, false, true
	}
	return errors.New(s.k.Name + ": unknown request"), false, false
}

// waitRecv has the Recv of r, by the owner from, wait for a datagram,
// until its timeout, if any; one waits at a time, on a passive socket.
func (s *server[A, S]) waitRecv(from actor.From, r RecvReq) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case from.PID != s.owner:
		return s.k.ErrNotOwner
	case s.active != Passive:
		return s.k.ErrActive
	case s.recv != nil:
		return errors.New(s.k.Name + ": Recv already waiting")
	}
	s.recvSeq++
	s.recv = &pendingRecv{from: from, seq: s.recvSeq, cancel: func() {}}
	if r.Timeout > 0 {
		seq, n, pid := s.recvSeq, s.self.Node(), s.self.PID()
		t := time.AfterFunc(r.Timeout, func() { n.Send(pid, recvTimeout{seq}) })
		s.recv.cancel = func() { t.Stop() }
	}
	return nil
}

// control hands the socket to owner, asked by its owner from.
func (s *server[A, S]) control(from actor.From, owner proc.PID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if from.PID != s.owner {
		return s.k.ErrNotOwner
	}
	s.self.Demonitor(s.watch)
	s.owner, s.watch = owner, s.self.Monitor(owner)
	return nil
}

func (s *server[A, S]) setActive(a Active) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var passive bool
	if s.active, passive = s.active.set(a); passive {
		s.self.Send(s.owner, s.k.Passive(s.sock))
	}
}

// deliver hands the held datagram to whoever wants it, then the failure of
// reading, once there is nothing before it; else it wakes the reader if
// someone still wants a datagram. It reports when the socket is to close.
//
//abc:disable abc the path of every datagram to the owner
//abc:hot net/genudp/BenchmarkEcho
func (s *server[A, S]) deliver() (done bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held != nil && s.wanted() {
		dg := *s.held
		s.held = nil
		s.hand(s.self.Node(), dg)
	}
	if s.held != nil || !s.wanted() {
		return false
	}
	if s.failed != nil {
		if s.recv != nil {
			s.answer(s.self.Node(), RecvRep[A]{Err: s.failed})
		} else {
			s.self.Send(s.owner, s.k.Error(s.sock, s.failed))
			s.self.Send(s.owner, s.k.Closed(s.sock))
		}
		return true
	}
	s.poke()
	return false
}

// wanted reports whether the owner wants a datagram. Called with mu held.
func (s *server[A, S]) wanted() bool {
	return s.active != Passive || s.recv != nil
}

// hand gives dg to the owner, which wants it: as a data message in an
// active mode, or to the waiting Recv. Called with mu held.
func (s *server[A, S]) hand(n *proc.Node, dg datagram[A]) {
	if s.active == Passive {
		s.answer(n, RecvRep[A]{From: dg.from, Data: dg.data})
		return
	}
	n.Send(s.owner, s.k.Data(s.sock, dg.from, dg.data))
	var passive bool
	if s.active, passive = s.active.take(); passive {
		n.Send(s.owner, s.k.Passive(s.sock))
	}
}

func (s *server[A, S]) answer(n *proc.Node, rep RecvRep[A]) {
	s.recv.cancel()
	actor.SendReply(n, s.recv.from, rep)
	s.recv = nil
}

// poke wakes the reader, without blocking: a wake already pending covers
// this one.
func (s *server[A, S]) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// read reads datagrams while the owner wants them, in a goroutine of its
// own, and hands them over itself. It stops reading when no one wants one,
// until poked, and for good when reading fails, telling the process.
//
//abc:disable nesting the loop over every datagram read
//abc:hot net/genudp/BenchmarkEcho
func (s *server[A, S]) read() {
	n, pid, done := s.self.Node(), s.self.PID(), s.self.Done()
	buf := make([]byte, s.max+1) // one more: a datagram filling it is too large
	for {
		select {
		case <-s.wake:
		case <-done:
			return
		}
		for s.reading(done) {
			k, from, err := s.conn.ReadFrom(buf)
			if err != nil {
				select {
				case <-done: // closed by the process
				default:
					n.Send(pid, readFailed{err})
				}
				return
			}
			if k > s.max {
				continue // too large: dropped
			}
			s.offer(n, done, datagram[A]{from, append([]byte(nil), buf[:k]...)})
		}
	}
}

// reading reports whether the reader is to read another datagram: the
// process is alive, and the owner wants one that is not held already.
func (s *server[A, S]) reading(done <-chan struct{}) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-done:
		return false
	default:
	}
	return s.held == nil && s.failed == nil && s.wanted()
}

// offer hands dg to the owner if it still wants one, or holds it for the
// next demand. A process dead, its socket delivers nothing more: the reader
// checks under the lock, before any monitor can learn of the death.
func (s *server[A, S]) offer(n *proc.Node, done <-chan struct{}, dg datagram[A]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-done:
		return
	default:
	}
	if s.wanted() {
		s.hand(n, dg)
		return
	}
	s.held = &dg
}
