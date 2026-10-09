package genudp

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/proc"
)

// Messages to the socket process from itself.
type (
	readFailed  struct{ err error }
	recvTimeout struct{ seq uint64 }
)

// datagram is one datagram received.
type datagram struct {
	from netip.AddrPort
	data []byte
}

// server is the process of a socket: it owns the connection, and decides
// who gets the datagrams. The reader reads, in a goroutine of its own, only
// while someone wants a datagram, and hands each to the owner itself.
type server struct {
	self  *proc.Self
	conn  *net.UDPConn
	sock  Socket
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
	held   *datagram
	failed error         // reading failed: close once the owner is told
	wake   chan struct{} // tells the reader someone may want a datagram

	recvSeq uint64
}

type pendingRecv struct {
	from   molecule.From
	seq    uint64
	cancel func()
}

func serve(self *proc.Self, sock Socket, owner proc.PID, opts Options) error {
	self.SetLabel("genudp socket " + sock.LocalAddr.String())
	s := &server{
		self:   self,
		conn:   sock.conn,
		sock:   sock,
		max:    opts.packetSize(),
		owner:  owner,
		watch:  self.Monitor(owner),
		active: opts.Active,
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
		case molecule.CallMsg:
			var reply any
			var deferred bool
			reply, deferred, stop = s.handle(m.From, m.Req)
			if !deferred {
				molecule.SendReply(self, m.From, reply)
			}
		case sendReq, setActiveReq, closeReq:
			// From the effects of a behaviour, with no one to reply to.
			_, _, stop = s.handle(molecule.From{}, m)
		case readFailed:
			s.mu.Lock()
			s.failed = m.err
			s.mu.Unlock()
		case recvTimeout:
			s.mu.Lock()
			if s.recv != nil && s.recv.seq == m.seq {
				molecule.SendReply(self, s.recv.from, ErrTimeout)
				s.recv = nil
			}
			s.mu.Unlock()
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

// handle handles a request; requests sent as effects have a zero from. It
// returns the reply, whether it comes later, and whether the socket is to
// close.
func (s *server) handle(from molecule.From, req any) (reply any, deferred, stop bool) {
	switch r := req.(type) {
	case sendReq:
		err := write(s.conn, r.to, r.data)
		if r.then {
			s.setActive(r.active)
		}
		return err, false, false
	case recvReq:
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case from.PID != s.owner:
			return ErrNotOwner, false, false
		case s.active != Passive:
			return ErrActive, false, false
		case s.recv != nil:
			return errors.New("genudp: Recv already waiting"), false, false
		}
		s.recvSeq++
		s.recv = &pendingRecv{from: from, seq: s.recvSeq, cancel: func() {}}
		if r.timeout > 0 {
			seq, n, pid := s.recvSeq, s.self.Node(), s.self.PID()
			t := time.AfterFunc(r.timeout, func() { n.Send(pid, recvTimeout{seq}) })
			s.recv.cancel = func() { t.Stop() }
		}
		return nil, true, false
	case setActiveReq:
		s.setActive(r.active)
		return nil, false, false
	case controlReq:
		s.mu.Lock()
		defer s.mu.Unlock()
		if from.PID != s.owner {
			return ErrNotOwner, false, false
		}
		s.self.Demonitor(s.watch)
		s.owner, s.watch = r.owner, s.self.Monitor(r.owner)
		return nil, false, false
	case closeReq:
		return nil, false, true
	}
	return errors.New("genudp: unknown request"), false, false
}

func (s *server) setActive(a Active) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var passive bool
	if s.active, passive = s.active.set(a); passive {
		s.self.Send(s.owner, PassiveMsg{s.sock})
	}
}

// deliver hands the held datagram to whoever wants it, then the failure of
// reading, once there is nothing before it; else it wakes the reader if
// someone still wants a datagram. It reports when the socket is to close.
func (s *server) deliver() (done bool) {
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
			s.answer(s.self.Node(), recvRep{err: s.failed})
		} else {
			s.self.Send(s.owner, ErrorMsg{s.sock, s.failed})
			s.self.Send(s.owner, ClosedMsg{s.sock})
		}
		return true
	}
	s.poke()
	return false
}

// wanted reports whether the owner wants a datagram. Called with mu held.
func (s *server) wanted() bool {
	return s.active != Passive || s.recv != nil
}

// hand gives dg to the owner, which wants it: as a DataMsg in an active
// mode, or to the waiting Recv. Called with mu held.
func (s *server) hand(n *proc.Node, dg datagram) {
	if s.active == Passive {
		s.answer(n, recvRep{from: dg.from, data: dg.data})
		return
	}
	n.Send(s.owner, DataMsg{s.sock, dg.from, dg.data})
	var passive bool
	if s.active, passive = s.active.take(); passive {
		n.Send(s.owner, PassiveMsg{s.sock})
	}
}

func (s *server) answer(n *proc.Node, rep recvRep) {
	s.recv.cancel()
	molecule.SendReply(n, s.recv.from, rep)
	s.recv = nil
}

// poke wakes the reader, without blocking: a wake already pending covers
// this one.
func (s *server) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// read reads datagrams while the owner wants them, in a goroutine of its
// own, and hands them over itself. It stops reading when no one wants one,
// until poked, and for good when reading fails, telling the process.
func (s *server) read() {
	n, pid, done := s.self.Node(), s.self.PID(), s.self.Done()
	buf := make([]byte, s.max+1) // one more: a datagram filling it is too large
	for {
		select {
		case <-s.wake:
		case <-done:
			return
		}
		for s.reading(done) {
			k, from, err := s.conn.ReadFromUDPAddrPort(buf)
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
			from = netip.AddrPortFrom(from.Addr().Unmap(), from.Port())
			s.offer(n, done, datagram{from, append([]byte(nil), buf[:k]...)})
		}
	}
}

// reading reports whether the reader is to read another datagram: the
// process is alive, and the owner wants one that is not held already.
func (s *server) reading(done <-chan struct{}) bool {
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
func (s *server) offer(n *proc.Node, done <-chan struct{}, dg datagram) {
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
