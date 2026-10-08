package gen

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/shun159/molecule/proc"
)

// run runs b in the current process: it registers the name, starts the
// runner, acknowledges the start, and feeds the runner the mailbox.
func run[S any](self *proc.Self, b Behaviour[S], args any, o options) error {
	if o.name != nil {
		if err := o.name.Register(self.Node(), self.PID()); err != nil {
			if pid, ok := o.name.WhereIs(self.Node()); ok && pid != self.PID() {
				err = &AlreadyStartedError{PID: pid}
			}
			self.InitAck(err)
			return nil
		}
	}

	r := newRuntime(b, procEnv{self})
	if err := r.Init(args); err != nil {
		self.InitAck(err)
		if errors.Is(err, ErrIgnore) {
			return nil
		}
		return err
	}
	self.InitAck(nil)
	if done, reason := r.Flush(); done {
		return reason
	}
	for {
		msg, err := self.Receive(context.Background())
		if err != nil {
			r.Abort()
			return err // killed: no Terminate, as in Erlang
		}
		if done, reason := r.Deliver(msg); done {
			return reason
		}
	}
}

// procEnv is the Env of a proc process. Once the process is dead, killed
// while its goroutine still runs, it acts on nothing anymore.
type procEnv struct {
	self *proc.Self
}

func (e procEnv) dead() bool { return e.self.ExitReason() != nil }

func (e procEnv) Self() proc.PID   { return e.self.PID() }
func (e procEnv) Parent() proc.PID { return e.self.Parent() }

func (e procEnv) Resolve(dest Dest) (proc.PID, bool) { return dest.WhereIs(e.self.Node()) }

func (e procEnv) Send(to proc.PID, msg any) {
	if !e.dead() {
		e.self.Send(to, msg)
	}
}

func (e procEnv) SendName(node, name string, msg any) {
	if !e.dead() {
		e.self.Node().SendName(node, name, msg)
	}
}

func (e procEnv) SendAlias(ref proc.Ref, msg any) {
	if !e.dead() {
		e.self.Node().SendAlias(ref, msg)
	}
}

func (e procEnv) Monitor(pid proc.PID) proc.Ref { return e.self.Monitor(pid) }
func (e procEnv) Demonitor(ref proc.Ref)        { e.self.Demonitor(ref) }

func (e procEnv) MonitorName(node, name string) proc.Ref { return e.self.MonitorName(node, name) }
func (e procEnv) Link(pid proc.PID)                      { e.self.Link(pid) }
func (e procEnv) Unlink(pid proc.PID)                    { e.self.Unlink(pid) }
func (e procEnv) TrapExit(on bool)                       { e.self.TrapExit(on) }

func (e procEnv) SendAfter(d time.Duration, msg any) func() {
	n, self := e.self.Node(), e.self.PID()
	t := time.AfterFunc(d, func() { n.Send(self, msg) })
	return func() { t.Stop() }
}

func (e procEnv) Request(pid proc.PID, reply func(proc.Ref, proc.AliasMsg) any) (proc.Ref, func()) {
	return e.request(e.self.Node().MonitorAlias(pid), reply)
}

func (e procEnv) RequestName(node, name string, reply func(proc.Ref, proc.AliasMsg) any) (proc.Ref, func()) {
	return e.request(e.self.Node().MonitorAliasName(node, name), reply)
}

// request hands what arrives on a to the process, as reply makes it.
func (e procEnv) request(a proc.Alias, reply func(proc.Ref, proc.AliasMsg) any) (proc.Ref, func()) {
	n, self, dead := e.self.Node(), e.self.PID(), e.self.Done()
	released := make(chan struct{})
	go func() {
		defer a.Release()
		select {
		case m := <-a.C:
			n.Send(self, reply(a.Ref, m))
		case <-released:
		case <-dead:
		}
	}()
	return a.Ref, sync.OnceFunc(func() { close(released) })
}

func (e procEnv) Logger() *slog.Logger { return e.self.Node().Logger() }
