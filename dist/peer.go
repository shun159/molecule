package dist

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/shun159/molecule/net/gentcp"
	"github.com/shun159/molecule/proc"
)

// Messages to the process of a connection, besides the frames to send.
type (
	// wake tells that a dial or an accept has ended: a connection may be
	// ready to take.
	wake struct{}
	// waitUp asks to be told on ref once connected.
	waitUp struct{ ref proc.Ref }
	// tick is the time to check the connection lives.
	tick struct{}
	// stop drops the connection.
	stop struct{}
)

// runPeer is the process of the connection to a node. Connecting, it keeps
// the frames to send; connected, it sends them in order, and hands the
// frames received to the node. When it ends, the links and monitors
// through it fail.
func (d *Dist) runPeer(self *proc.Self, p *peer) error {
	self.SetLabel("dist connection " + p.node)
	self.SetMaxMessages(d.cfg.MaxQueue)
	if p.dialing {
		go d.dial(p)
	}
	conn, w, err := d.await(self, p)
	if conn == nil {
		return err
	}
	c := d.connected(self, p, conn, w.waiters)
	if !c.send(w.queue...) {
		return c.close()
	}
	return c.run()
}

// connecting is what the process of a connection keeps until connected:
// the frames to send, and the refs of those waiting for it.
type connecting struct {
	queue   []frame
	waiters []proc.Ref
}

// keep keeps f to send once connected, and reports whether the frames kept
// are still within max, if max > 0.
func (w *connecting) keep(f frame, max int) bool {
	w.queue = append(w.queue, f)
	return max <= 0 || len(w.queue) <= max
}

// await waits for the connection to p, keeping what comes meanwhile. It
// returns the connection, or, with none, the end of the process.
func (d *Dist) await(self *proc.Self, p *peer) (net.Conn, connecting, error) {
	var w connecting
	for {
		msg, err := self.Receive(context.Background())
		if err != nil {
			return nil, w, d.lost(p, err)
		}
		switch m := msg.(type) {
		case frame:
			if !w.keep(m, d.cfg.MaxQueue) {
				return nil, w, d.lost(p, proc.MailboxFull)
			}
		case waitUp:
			w.waiters = append(w.waiters, m.ref)
		case wake:
			conn, gone := d.woken(p)
			if gone {
				return nil, w, d.down(p)
			}
			if conn != nil {
				return conn, w, nil
			}
		case stop:
			return nil, w, d.drop(p)
		}
	}
}

// woken takes the connection made to p, if any, once a dial or an accept
// has ended. With none, and none to come, p is gone: forgotten.
func (d *Dist) woken(p *peer) (conn net.Conn, gone bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if p.ready != nil {
		conn, p.ready, p.up = p.ready, nil, true
		return conn, false
	}
	if p.coming() {
		return nil, false
	}
	d.forget(p)
	return nil, true
}

// coming reports whether a connection to p may still come: by its dial,
// one accepted, or one the node is to dial.
func (p *peer) coming() bool { return p.dialing || p.incoming || p.awaiting }

// drop forgets p, and has its node down.
func (d *Dist) drop(p *peer) error {
	d.mu.Lock()
	d.forget(p)
	d.mu.Unlock()
	return d.down(p)
}

// connected starts the connection to p on conn: its socket, its ticks, the
// node up, and those waiting for it told.
func (d *Dist) connected(self *proc.Self, p *peer, conn net.Conn, waiters []proc.Ref) *connection {
	conn, reads := counted(conn)
	sock := gentcp.Start(d.n, conn, self.PID(), gentcp.Options{
		Packet:      gentcp.Packet4,
		PacketSize:  d.cfg.MaxFrame,
		Active:      gentcp.Always,
		SendTimeout: d.cfg.TickTime,
	})
	c := &connection{d: d, self: self, p: p, sock: sock, enc: d.cfg.Codec.NewEncoder(), dec: d.cfg.Codec.NewDecoder(),
		reads: reads, heard: time.Now()}
	go ticker(d.n, self.PID(), self.Done(), min(d.cfg.TickTime, d.cfg.Silent)/ticksPerTime)
	d.remote.NodeUp(p.node)
	for _, ref := range waiters {
		d.n.SendAlias(ref, nil)
	}
	return c
}

// run sends the frames given to the process, gathered, and hands those
// received to the node, until the connection ends.
//
//abc:disable abc,cyclo the loop over every message of a connection
func (c *connection) run() error {
	d, p, self := c.d, c.p, c.self
	var next any // a message taken while gathering frames
	for {
		msg := next
		if next = nil; msg == nil {
			var err error
			if msg, err = self.Receive(context.Background()); err != nil {
				return d.lost(p, err)
			}
		}
		switch m := msg.(type) {
		case frame:
			var frames []frame
			frames, next = c.gather(m)
			if !c.send(frames...) {
				return c.close()
			}
		case gentcp.DataMsg:
			c.hear()
			c.receive(m.Bytes)
		case tick:
			if err := c.tick(); err != nil {
				d.n.Logger().Warn("dist: connection dropped", "node", p.node, "error", err)
				return c.close()
			}
		case gentcp.ErrorMsg:
			if errors.Is(m.Err, gentcp.ErrPacketTooLarge) {
				d.n.Logger().Warn("dist: frame past MaxFrame, connection dropped", "node", p.node)
			}
		case gentcp.ClosedMsg:
			return c.close()
		case waitUp:
			d.n.SendAlias(m.ref, nil)
		case stop:
			return c.close()
		}
	}
}

// forget removes p from the connections, so that the next send to its
// node connects anew. d.mu is held.
func (d *Dist) forget(p *peer) {
	if d.peers[p.node] == p {
		delete(d.peers, p.node)
	}
	if p.ready != nil {
		p.ready.Close()
		p.ready = nil
	}
}

// down fails the links and monitors through the connection to the node of
// p, once forgotten.
func (d *Dist) down(p *peer) error {
	d.remote.NodeDown(p.node)
	return nil
}

// lost forgets p, its process ended by an exit signal, and fails the
// links and monitors through it, as a connection closing does. Its
// socket, if any, closes with it, its owner.
func (d *Dist) lost(p *peer, err error) error {
	if errors.Is(err, proc.MailboxFull) {
		d.n.Logger().Warn("dist: frames past MaxQueue, connection dropped", "node", p.node)
	}
	d.mu.Lock()
	d.forget(p)
	d.mu.Unlock()
	d.down(p)
	return err
}

// connection is a connection up.
type connection struct {
	d    *Dist
	self *proc.Self
	p    *peer
	sock gentcp.Socket
	enc  Encoder
	dec  Decoder
	buf  []byte

	frames  []frame  // gathered, to send at once
	ends    []int    // where each frame ends in buf
	packets [][]byte // of the frames, in buf

	reads  *atomic.Uint64 // the reads of the socket that got bytes
	read   uint64         // as of the last tick
	heard  time.Time      // when the node was heard from last
	silent bool           // told silent
	said   bool           // since the last tick
}

// A connection checks it lives ticksPerTime times per TickTime, or per
// Silent if shorter.
const ticksPerTime = 4

func ticker(n *proc.Node, pid proc.PID, done <-chan struct{}, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			n.Send(pid, tick{})
		case <-done:
			return
		}
	}
}

// errSilent is the error of a connection silent for TickTime.
var errSilent = errors.New("silent for TickTime")

// tick sends a tick if nothing was sent since the last, and tells the node
// silent if not heard from within Silent. It fails if the node was not
// heard from within TickTime, or the tick could not be sent. Any byte read
// is heard, of a frame whole or not.
func (c *connection) tick() error {
	if read := c.reads.Load(); read != c.read {
		c.read = read
		c.hear()
	}
	silence := time.Since(c.heard)
	if silence >= c.d.cfg.TickTime {
		return errSilent
	}
	if silence >= c.d.cfg.Silent && !c.silent {
		c.silent = true
		c.d.remote.NodeSilent(c.p.node)
	}
	if !c.said {
		c.buf = appendFrame(c.buf[:0], frame{op: opTick})
		if err := c.sock.Send(context.Background(), c.self, c.buf); err != nil {
			return err
		}
	}
	c.said = false
	return nil
}

// hear notes the node heard from now, and tells it heard if it was told
// silent.
func (c *connection) hear() {
	c.heard = time.Now()
	if c.silent {
		c.silent = false
		c.d.remote.NodeHeard(c.p.node)
	}
}

// maxGathered is the most frames sent at once.
const maxGathered = 256

// gathered is a context done already: Receive with it takes a message
// only if one is waiting.
var gathered = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// gather returns f and the frames waiting after it, to send at once, and
// the first other message it took, if any. Finding none waiting, it lets
// the other goroutines run once first: the processes sending to the node,
// as members of many groups, often have frames on their way, which the
// one write takes too.
func (c *connection) gather(f frame) ([]frame, any) {
	c.frames = append(c.frames[:0], f)
	yielded := false
	for len(c.frames) < maxGathered {
		msg, err := c.self.Receive(gathered)
		if err != nil {
			if yielded {
				break
			}
			runtime.Gosched()
			yielded = true
			continue
		}
		f, ok := msg.(frame)
		if !ok {
			return c.frames, msg
		}
		c.frames = append(c.frames, f)
	}
	return c.frames, nil
}

// send sends frames, in one write, and reports whether the connection is
// still up. A message that fails to encode is dropped.
func (c *connection) send(frames ...frame) bool {
	c.appendFrames(frames)
	clear(c.frames) // keep no message
	if len(c.packets) == 0 {
		return true
	}
	c.said = true
	err := c.sock.SendAll(context.Background(), c.self, c.packets...)
	clear(c.packets)
	if cap(c.buf) > 1<<20 {
		c.buf = nil // keep no large buffer
	}
	return err == nil
}

// appendFrames appends frames to the buffer, their messages encoded, and
// makes each its packet. A message that fails to encode is dropped.
func (c *connection) appendFrames(frames []frame) {
	c.buf, c.ends, c.packets = c.buf[:0], c.ends[:0], c.packets[:0]
	for _, f := range frames {
		if f.op.carriesMsg() && !c.encode(&f) {
			continue
		}
		c.buf = appendFrame(c.buf, f)
		c.ends = append(c.ends, len(c.buf))
	}
	start := 0
	for _, end := range c.ends {
		c.packets = append(c.packets, c.buf[start:end])
		start = end
	}
}

// encode encodes the message of f as its payload, and reports whether it
// could; a message that could not is logged.
func (c *connection) encode(f *frame) bool {
	b, err := c.enc.Encode(f.msg)
	if err != nil {
		c.d.n.Logger().Warn("dist: message not sent", "node", c.p.node, "type", typeName(f.msg), "error", err)
		return false
	}
	f.payload = b
	return true
}

// receive hands the frame b to the node. The other node speaks only for
// itself: a frame claiming a process or reference of a third is dropped.
func (c *connection) receive(b []byte) {
	f, err := parseFrame(b)
	if err != nil {
		c.d.n.Logger().Warn("dist: bad frame", "node", c.p.node, "error", err)
		return
	}
	var msg any
	if f.op.carriesMsg() {
		if msg, err = c.dec.Decode(f.payload); err != nil {
			c.d.n.Logger().Warn("dist: message not received", "node", c.p.node, "error", err)
			return
		}
	}
	if c.own(f) {
		c.deliver(f, msg)
	}
}

// carriesMsg reports whether a frame of o carries a message.
func (o op) carriesMsg() bool { return o == opSend || o == opSendName || o == opSendAlias }

// own reports whether f speaks for the other node alone: of a process or
// a reference of its own, where it names one.
func (c *connection) own(f frame) bool {
	switch f.op {
	case opExit, opExitLink, opLink, opUnlink:
		return f.from.Node() == c.p.node
	case opMonitor, opDemonitor, opMonitorName, opDemonitorName:
		return f.ref.Node() == c.p.node
	case opDown:
		return f.to.Node() == c.p.node
	}
	return true
}

// deliver hands f, with its message, to the node.
func (c *connection) deliver(f frame, msg any) {
	r := c.d.remote
	switch f.op {
	case opSend:
		r.Send(f.to, msg)
	case opSendName:
		r.SendName(f.name, msg)
	case opSendAlias:
		r.SendAlias(f.ref, msg)
	case opExit, opExitLink:
		r.Exit(f.from, f.to, f.reason, f.op == opExitLink)
	case opLink:
		r.Link(f.from, f.to)
	case opUnlink:
		r.Unlink(f.from, f.to)
	case opMonitor:
		r.Monitor(f.ref, f.to)
	case opDemonitor:
		r.Demonitor(f.ref, f.to)
	case opMonitorName:
		r.MonitorName(f.ref, f.name)
	case opDemonitorName:
		r.DemonitorName(f.ref, f.name)
	case opDown:
		r.Down(f.ref, f.to, f.reason)
	}
}

func typeName(v any) string { return fmt.Sprintf("%T", v) }

// close drops the connection.
func (c *connection) close() error {
	c.d.mu.Lock()
	c.d.forget(c.p)
	c.d.mu.Unlock()
	c.sock.Close(context.Background(), c.self)
	return c.d.down(c.p)
}
