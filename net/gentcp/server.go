package gentcp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/proc"
)

// Read buffers start at minReadBuffer, double while reads fill them, and
// halve while reads use less than a quarter, within the limits.
const (
	minReadBuffer = 2 << 10
	maxReadBuffer = 64 << 10
)

// Messages to the socket process from itself.
type (
	readResult struct {
		b   []byte
		err error
	}
	delivered   struct{} // the reader gave a read to the owner itself
	recvTimeout struct{ seq uint64 }
)

// server is the process of a socket: it owns the connection, reads only
// when the owner wants data, cuts packets, and delivers them.
type server struct {
	self  *proc.Self
	conn  net.Conn
	sock  Socket
	opts  Options
	owner proc.PID
	watch proc.Ref

	active  Active
	dec     decoder
	reads   chan struct{} // asks the reader for one read
	reading bool
	direct  direct

	eof     bool  // nothing more to read
	readErr error // why, if not the peer closing
	broken  bool  // the connection failed: close once the owner knows
	told    bool  // the owner got ClosedMsg

	recv    *pendingRecv
	recvSeq uint64
}

// direct lets the reader hand a read to the owner itself, sparing the
// hop through the process, when the read would be a packet of its own: a
// Raw socket, active, with nothing buffered. The process allows it for
// one read at a time, and takes it back under the lock when the owner
// changes, the socket turns passive or closes, so a read is delivered
// either before such a change or not at all by the reader.
type direct struct {
	mu    sync.Mutex
	ok    bool // the read in progress may go to owner
	owner proc.PID
}

type pendingRecv struct {
	from   molecule.From
	length int
	seq    uint64
	cancel func()
}

func serve(self *proc.Self, conn net.Conn, sock Socket, owner proc.PID, opts Options) error {
	s := &server{
		self:   self,
		conn:   conn,
		sock:   sock,
		opts:   opts,
		owner:  owner,
		watch:  self.Monitor(owner),
		active: opts.Active,
		dec:    decoder{packet: opts.Packet, max: opts.PacketSize},
		reads:  make(chan struct{}, 1),
	}
	// Closing the connection also ends a read or write in progress once
	// the process is dead.
	context.AfterFunc(self.Context(), func() { conn.Close() })
	defer conn.Close()
	go s.read()

	for {
		if done := s.deliver(); done {
			return nil
		}
		s.wantData()
		msg, err := self.Receive(context.Background())
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case readResult:
			s.reading = false
			if s.broken {
				break // what comes after a failure is dropped
			}
			if len(m.b) > 0 {
				s.dec.feed(m.b)
			}
			if m.err != nil {
				s.eof = true
				if !errors.Is(m.err, io.EOF) {
					s.readErr = m.err
				}
			}
		case delivered:
			s.reading = false
			// The reader tells before it sends the packet, so that the
			// owner asking for more comes after; the lock waits for the
			// packet, so that what the process sends from now on comes
			// after it.
			s.direct.mu.Lock()
			s.direct.mu.Unlock()
			var passive bool
			if s.active, passive = s.active.take(); passive {
				s.self.Send(s.owner, PassiveMsg{s.sock})
			}
		case molecule.CallMsg:
			reply, deferred, stop := s.handle(m.From, m.Req)
			if !deferred {
				molecule.SendReply(self, m.From, reply)
			}
			if stop {
				return nil
			}
		case sendFailed:
			s.fail(m.err)
		case sendReq, setActiveReq, closeReq:
			// From the effects of a behaviour, with no one to reply to.
			if _, _, stop := s.handle(molecule.From{}, m); stop {
				return nil
			}
		case recvTimeout:
			if s.recv != nil && s.recv.seq == m.seq {
				molecule.SendReply(self, s.recv.from, ErrTimeout)
				s.recv = nil
			}
		case proc.DownMsg:
			if m.Ref == s.watch {
				return nil // the owner is gone, and with it the socket
			}
		}
	}
}

// handle handles a request; requests sent as effects have a zero from. It returns the reply,
// whether it comes later, and whether the socket is to close.
func (s *server) handle(from molecule.From, req any) (reply any, deferred, stop bool) {
	switch r := req.(type) {
	case sendReq:
		err := s.send(r.data)
		if r.then {
			s.setActive(r.active)
		}
		return err, false, false
	case recvReq:
		switch {
		case from.PID != s.owner:
			return ErrNotOwner, false, false
		case s.active != Passive:
			return ErrActive, false, false
		case s.recv != nil:
			return errors.New("gentcp: Recv already waiting"), false, false
		}
		s.recvSeq++
		s.recv = &pendingRecv{from: from, length: r.length, seq: s.recvSeq, cancel: func() {}}
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
		if from.PID != s.owner {
			return ErrNotOwner, false, false
		}
		s.self.Demonitor(s.watch)
		s.owner, s.watch = r.owner, s.self.Monitor(r.owner)
		s.direct.mu.Lock()
		s.direct.owner = r.owner
		s.direct.mu.Unlock()
		return nil, false, false
	case shutdownReq:
		return s.shutdown(r.how), false, false
	case closeReq:
		s.revoke()
		return nil, false, true
	}
	return errors.New("gentcp: unknown request"), false, false
}

func (s *server) setActive(a Active) {
	var passive bool
	if s.active, passive = s.active.set(a); passive {
		s.self.Send(s.owner, PassiveMsg{s.sock})
	}
	if s.active == Passive {
		s.revoke()
	}
}

// revoke takes back from the reader the right to deliver.
func (s *server) revoke() {
	s.direct.mu.Lock()
	s.direct.ok = false
	s.direct.mu.Unlock()
}

// send writes for a Socket made by hand, which has no writer.
func (s *server) send(data []byte) error {
	failed, err := s.sock.w.write(data)
	if failed {
		s.fail(err)
	}
	return err
}

func (s *server) shutdown(how How) error {
	tc, ok := s.conn.(*net.TCPConn)
	if !ok {
		return errors.New("gentcp: not a TCP connection")
	}
	switch how {
	case Read:
		return tc.CloseRead()
	case Write:
		return tc.CloseWrite()
	}
	if err := tc.CloseWrite(); err != nil {
		return err
	}
	return tc.CloseRead()
}

// deliver hands out the packets the owner may have: as DataMsg in an
// active mode, or to a waiting Recv, and then the end of the connection,
// when the owner wants data and there is no more. It reports when the
// socket is to close.
func (s *server) deliver() (done bool) {
	for s.active != Passive {
		pkt, ok := s.next(0)
		if !ok {
			break
		}
		s.self.Send(s.owner, DataMsg{s.sock, pkt})
		var passive bool
		if s.active, passive = s.active.take(); passive {
			s.self.Send(s.owner, PassiveMsg{s.sock})
		}
	}
	if s.recv != nil {
		pkt, ok := s.next(s.recv.length)
		switch {
		case ok:
			s.answer(pkt)
		case s.eof:
			if s.readErr != nil {
				s.answer(s.readErr)
			} else {
				s.answer(ErrClosed)
			}
			return s.closing()
		}
	}
	if s.eof && s.active != Passive && !s.told {
		// What is left is not a whole packet: the connection is over.
		if s.readErr != nil {
			s.self.Send(s.owner, ErrorMsg{s.sock, s.readErr})
		}
		s.self.Send(s.owner, ClosedMsg{s.sock})
		s.told = true
		return s.closing()
	}
	return false
}

// next cuts the next packet. A bad one breaks the connection.
func (s *server) next(length int) ([]byte, bool) {
	pkt, ok, err := s.dec.next(length)
	if err != nil {
		s.fail(err)
		return nil, false
	}
	return pkt, ok
}

// fail breaks the connection with err: nothing more is received, what is
// buffered is dropped, and the owner is told when it next wants data.
func (s *server) fail(err error) {
	if s.broken {
		return
	}
	s.dec.buf = nil
	s.eof, s.readErr, s.broken = true, err, true
	s.revoke()
}

// closing reports whether the socket closes, now that the owner knows the
// connection is over for receiving.
func (s *server) closing() bool {
	return s.broken || !s.opts.HalfClosed
}

func (s *server) answer(v any) {
	s.recv.cancel()
	molecule.SendReply(s.self, s.recv.from, v)
	s.recv = nil
}

// wantData asks for a read when the owner waits for data that is not
// there whole yet.
func (s *server) wantData() {
	if s.reading || s.eof {
		return
	}
	want := s.active != Passive || s.recv != nil
	if !want {
		return
	}
	peek := s.dec
	length := 0
	if s.recv != nil {
		length = s.recv.length
	}
	if _, ok, err := peek.next(length); ok || err != nil {
		return
	}
	s.direct.mu.Lock()
	s.direct.ok = s.opts.Packet == Raw && s.active != Passive && len(s.dec.buf) == 0
	s.direct.owner = s.owner
	s.direct.mu.Unlock()
	s.reading = true
	s.reads <- struct{}{}
}

// read reads on demand, in a goroutine of its own, and sends what it reads
// to the process: a read filling most of the buffer hands the buffer over,
// a smaller one is copied out, so the buffer follows the reads in size.
func (s *server) read() {
	n, pid, done := s.self.Node(), s.self.PID(), s.self.Done()
	buf := make([]byte, minReadBuffer)
	for {
		select {
		case <-s.reads:
		case <-done:
			return
		}
		k, err := s.conn.Read(buf)
		var b []byte
		size := nextReadSize(len(buf), k)
		switch {
		case k > len(buf)/2:
			b = buf[:k:k]
			buf = make([]byte, size)
		case k > 0:
			b = append([]byte(nil), buf[:k]...)
			fallthrough
		default:
			if size != len(buf) {
				buf = make([]byte, size)
			}
		}
		if k > 0 && err == nil && s.handOver(n, pid, done, b) {
			continue
		}
		n.Send(pid, readResult{b, err})
		if err != nil {
			return
		}
	}
}

// handOver delivers b to the owner from the reader, if the process allows
// it and is alive, and tells the process. A process dead, its socket
// delivers nothing more: the process sees to it for its own sends, and
// the reader checks under the lock, before any monitor can learn of the
// death.
func (s *server) handOver(n *proc.Node, pid proc.PID, done <-chan struct{}, b []byte) bool {
	s.direct.mu.Lock()
	defer s.direct.mu.Unlock()
	select {
	case <-done:
		return false
	default:
	}
	if !s.direct.ok {
		return false
	}
	s.direct.ok = false
	n.Send(pid, delivered{})
	n.Send(s.direct.owner, DataMsg{s.sock, b})
	return true
}

func nextReadSize(size, n int) int {
	switch {
	case n == size && size < maxReadBuffer:
		return size * 2
	case n < size/4 && size > minReadBuffer:
		return size / 2
	}
	return size
}
