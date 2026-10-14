package dist

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	var (
		queue   []frame
		waiters []proc.Ref
		conn    net.Conn
	)
	for conn == nil {
		msg, err := self.Receive(context.Background())
		if err != nil {
			return d.lost(p, err)
		}
		switch m := msg.(type) {
		case frame:
			queue = append(queue, m)
			if d.cfg.MaxQueue > 0 && len(queue) > d.cfg.MaxQueue {
				return d.lost(p, proc.MailboxFull)
			}
		case waitUp:
			waiters = append(waiters, m.ref)
		case wake:
			d.mu.Lock()
			switch {
			case p.ready != nil:
				conn, p.ready, p.up = p.ready, nil, true
			case !p.dialing && !p.incoming && !p.awaiting:
				// Neither way connected.
				d.forget(p)
				d.mu.Unlock()
				return d.down(p)
			}
			d.mu.Unlock()
		case stop:
			d.mu.Lock()
			d.forget(p)
			d.mu.Unlock()
			return d.down(p)
		}
	}

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
	for _, f := range queue {
		if !c.send(f) {
			return c.close()
		}
	}
	for {
		msg, err := self.Receive(context.Background())
		if err != nil {
			return d.lost(p, err)
		}
		switch m := msg.(type) {
		case frame:
			if !c.send(m) {
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

// send sends f, and reports whether the connection is still up. A message
// that fails to encode is dropped.
func (c *connection) send(f frame) bool {
	switch f.op {
	case opSend, opSendName, opSendAlias:
		b, err := c.enc.Encode(f.msg)
		if err != nil {
			c.d.n.Logger().Warn("dist: message not sent", "node", c.p.node, "type", typeName(f.msg), "error", err)
			return true
		}
		f.payload = b
	}
	c.buf = appendFrame(c.buf[:0], f)
	c.said = true
	return c.sock.Send(context.Background(), c.self, c.buf) == nil
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
	switch f.op {
	case opSend, opSendName, opSendAlias:
		if msg, err = c.dec.Decode(f.payload); err != nil {
			c.d.n.Logger().Warn("dist: message not received", "node", c.p.node, "error", err)
			return
		}
	}
	r, node := c.d.remote, c.p.node
	switch f.op {
	case opSend:
		r.Send(f.to, msg)
	case opSendName:
		r.SendName(f.name, msg)
	case opSendAlias:
		r.SendAlias(f.ref, msg)
	case opExit, opExitLink:
		if f.from.Node() == node {
			r.Exit(f.from, f.to, f.reason, f.op == opExitLink)
		}
	case opLink:
		if f.from.Node() == node {
			r.Link(f.from, f.to)
		}
	case opUnlink:
		if f.from.Node() == node {
			r.Unlink(f.from, f.to)
		}
	case opMonitor:
		if f.ref.Node() == node {
			r.Monitor(f.ref, f.to)
		}
	case opDemonitor:
		if f.ref.Node() == node {
			r.Demonitor(f.ref, f.to)
		}
	case opMonitorName:
		if f.ref.Node() == node {
			r.MonitorName(f.ref, f.name)
		}
	case opDemonitorName:
		if f.ref.Node() == node {
			r.DemonitorName(f.ref, f.name)
		}
	case opDown:
		if f.to.Node() == node {
			r.Down(f.ref, f.to, f.reason)
		}
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
