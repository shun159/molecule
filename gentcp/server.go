package gentcp

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/shun159/molecule/gen"
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

	active  Active
	dec     decoder
	reads   chan struct{} // asks the reader for one read
	reading bool

	eof     bool  // nothing more to read
	readErr error // why, if not the peer closing
	told    bool  // the owner got ClosedMsg

	recv    *pendingRecv
	recvSeq uint64
}

type pendingRecv struct {
	from   gen.From
	length int
	seq    uint64
	cancel func()
}

func serve(self *proc.Self, conn net.Conn, sock Socket, owner proc.PID, opts Options) error {
	// Closing the connection also ends a read or write in progress once
	// the process is dead.
	context.AfterFunc(self.Context(), func() { conn.Close() })
	defer conn.Close()

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
	go s.read()
	self.InitAck(nil)

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
			if len(m.b) > 0 {
				s.dec.feed(m.b)
			}
			if m.err != nil {
				s.eof = true
				if !errors.Is(m.err, io.EOF) {
					s.readErr = m.err
				}
			}
		case gen.CallMsg:
			reply, deferred, stop := s.handle(m.From, m.Req)
			if !deferred {
				gen.SendReply(self, m.From, reply)
			}
			if stop {
				return nil
			}
		case gen.CastMsg:
			if _, _, stop := s.handle(gen.From{}, m.Req); stop {
				return nil
			}
		case recvTimeout:
			if s.recv != nil && s.recv.seq == m.seq {
				gen.SendReply(self, s.recv.from, ErrTimeout)
				s.recv = nil
			}
		case proc.DownMsg:
			if m.Ref == s.watch {
				return nil // the owner is gone, and with it the socket
			}
		}
	}
}

// handle handles a request; casts have a zero from. It returns the reply,
// whether it comes later, and whether the socket is to close.
func (s *server) handle(from gen.From, req any) (reply any, deferred, stop bool) {
	switch r := req.(type) {
	case sendReq:
		return s.send(r.data), false, false
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
		var passive bool
		if s.active, passive = s.active.set(r.active); passive {
			s.self.Send(s.owner, PassiveMsg{s.sock})
		}
		return nil, false, false
	case controlReq:
		if from.PID != s.owner {
			return ErrNotOwner, false, false
		}
		s.self.Demonitor(s.watch)
		s.owner, s.watch = r.owner, s.self.Monitor(r.owner)
		return nil, false, false
	case shutdownReq:
		return s.shutdown(r.how), false, false
	case closeReq:
		return nil, false, true
	}
	return errors.New("gentcp: unknown request"), false, false
}

func (s *server) send(data []byte) error {
	hdr, err := s.opts.Packet.header(len(data))
	if err != nil {
		return err
	}
	if s.opts.SendTimeout > 0 {
		s.conn.SetWriteDeadline(time.Now().Add(s.opts.SendTimeout))
	}
	bufs := net.Buffers{hdr, data}
	_, err = bufs.WriteTo(s.conn)
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

// deliver hands out the packets the owner may have: as Data messages in
// an active mode, or to a waiting Recv. It reports when the socket is to
// close, the connection being over.
func (s *server) deliver() (done bool) {
	for s.active != Passive {
		pkt, ok, err := s.dec.next(0)
		if err != nil {
			return s.fail(err)
		}
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
		pkt, ok, err := s.dec.next(s.recv.length)
		switch {
		case err != nil:
			s.answer(err)
			return s.fail(err)
		case ok:
			s.answer(pkt)
		case s.eof:
			if s.readErr != nil {
				s.answer(s.readErr)
			} else {
				s.answer(ErrClosed)
			}
			return !s.opts.HalfClosed
		}
	}
	if s.eof && s.active != Passive && !s.told {
		// What is left is not a whole packet: the connection is over.
		if s.readErr != nil {
			s.self.Send(s.owner, ErrorMsg{s.sock, s.readErr})
		}
		s.self.Send(s.owner, ClosedMsg{s.sock})
		s.told = true
		return !s.opts.HalfClosed
	}
	return false
}

func (s *server) answer(v any) {
	s.recv.cancel()
	gen.SendReply(s.self, s.recv.from, v)
	s.recv = nil
}

// fail ends the connection on a bad packet.
func (s *server) fail(err error) bool {
	if s.active != Passive {
		s.self.Send(s.owner, ErrorMsg{s.sock, err})
		s.self.Send(s.owner, ClosedMsg{s.sock})
	}
	return true
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
		n.Send(pid, readResult{b, err})
		if err != nil {
			return
		}
	}
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
