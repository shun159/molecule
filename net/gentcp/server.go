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

	// mu orders what the reader delivers itself with what the process
	// does: the reader takes it to read and write active, reading and
	// direct, and to send to the owner. The process holds it whenever it
	// touches those, or sends to the owner, so that the owner gets
	// messages in the order they were decided.
	mu      sync.Mutex
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

// direct lets the reader hand reads to the owner itself, sparing the hop
// through the process, while each read is a packet of its own: a Raw
// socket, active, with nothing buffered. The reader takes the allowance of
// the active mode as it delivers, and reads on while some is left, without
// telling the process; it stops, and reading turns false, when none is.
// The process takes the right back when the socket turns passive, fails or
// closes; the reads after that go through the process.
type direct struct {
	ok    bool // reads may go to owner, until taken back
	owner proc.PID
}

type pendingRecv struct {
	from   molecule.From
	length int
	seq    uint64
	cancel func()
}

func serve(self *proc.Self, conn net.Conn, sock Socket, owner proc.PID, opts Options) error {
	self.SetLabel("gentcp socket " + sock.RemoteAddr.String())
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
			s.mu.Lock()
			s.reading = false
			if s.broken {
				s.mu.Unlock()
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
			s.mu.Unlock()
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
		s.mu.Lock()
		defer s.mu.Unlock()
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
		s.mu.Lock()
		defer s.mu.Unlock()
		if from.PID != s.owner {
			return ErrNotOwner, false, false
		}
		s.self.Demonitor(s.watch)
		s.owner, s.watch = r.owner, s.self.Monitor(r.owner)
		s.direct.owner = r.owner
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
	s.mu.Lock()
	defer s.mu.Unlock()
	var passive bool
	if s.active, passive = s.active.set(a); passive {
		s.self.Send(s.owner, PassiveMsg{s.sock})
	}
	if s.active == Passive {
		s.direct.ok = false
	}
}

// revoke takes back from the reader the right to deliver.
func (s *server) revoke() {
	s.mu.Lock()
	s.direct.ok = false
	s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
		s.failLocked(err)
		return nil, false
	}
	return pkt, ok
}

// fail breaks the connection with err: nothing more is received, what is
// buffered is dropped, and the owner is told when it next wants data.
func (s *server) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failLocked(err)
}

// failLocked is also used by the decoder while delivery holds mu.
func (s *server) failLocked(err error) {
	if s.broken {
		return
	}
	s.dec.buf = nil
	s.eof, s.readErr, s.broken = true, err, true
	s.direct.ok = false
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
//
// It sends on reads under mu, which must not block: while reading is
// false, the reader neither holds a request nor reads on by itself, as it
// turns reading false only when it stops, and the process only on a
// readResult, sent once the reader stopped.
func (s *server) wantData() {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.direct.ok = s.opts.Packet == Raw && s.active != Passive && len(s.dec.buf) == 0
	s.direct.owner = s.owner
	s.reading = true
	s.reads <- struct{}{}
}

// read reads on demand, in a goroutine of its own, and sends what it reads
// to the process: a read filling most of the buffer hands the buffer over,
// a smaller one is copied out, so the buffer follows the reads in size.
func (s *server) read() {
	n, pid, done := s.self.Node(), s.self.PID(), s.self.Done()
	buf := make([]byte, minReadBuffer)
	more := false
	for {
		if !more {
			select {
			case <-s.reads:
			case <-done:
				return
			}
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
		if k > 0 && err == nil {
			if handed, again := s.handOver(n, done, b); handed {
				more = again
				continue
			}
		}
		more = false
		n.Send(pid, readResult{b, err})
		if err != nil {
			return
		}
	}
}

// handOver delivers b to the owner from the reader, if the process allows
// it and is alive, consuming one active allowance. A process dead, its socket
// delivers nothing more: the process sees to it for its own sends, and
// the reader checks under the lock, before any monitor can learn of the
// death.
func (s *server) handOver(n *proc.Node, done <-chan struct{}, b []byte) (handed, again bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-done:
		return false, false
	default:
	}
	if !s.direct.ok {
		return false, false
	}
	n.Send(s.direct.owner, DataMsg{s.sock, b})
	var passive bool
	if s.active, passive = s.active.take(); passive {
		n.Send(s.direct.owner, PassiveMsg{s.sock})
	}
	// Once and exhausted N stop here. Only another activation may permit
	// the next Read; Always and remaining N need no socket-process wakeup.
	s.reading = s.active != Passive
	s.direct.ok = s.reading
	return true, s.reading
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
