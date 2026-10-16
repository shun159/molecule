package gen

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// run runs b in the current process: it registers the name, starts the
// runner, acknowledges the start, and feeds the runner the mailbox.
func run[S any](self *proc.Self, b Behaviour[S], args any, o actor.StartOptions) error {
	self.SetLabel(labelOf(b))
	self.SetMaxMessages(o.MaxMessages)
	if err := register(self, o.Name); err != nil {
		self.InitAck(err)
		return nil
	}
	r := newRunner(b, procEnv{self}, o)
	if err := r.Init(args); err != nil {
		self.InitAck(err)
		if errors.Is(err, actor.ErrIgnore) {
			return nil
		}
		return err
	}
	if done, reason := r.Flush(); done {
		return reason
	}
	return loop(self, r)
}

// register registers the process under name, if any. A name another
// process has fails the start with AlreadyStartedError.
func register(self *proc.Self, name actor.Name) error {
	if name == nil {
		return nil
	}
	err := name.Register(self.Node(), self.PID())
	if err == nil {
		return nil
	}
	if pid, ok := name.WhereIs(self.Node()); ok && pid != self.PID() {
		return &actor.AlreadyStartedError{PID: pid}
	}
	return err
}

// loop hands the runner the messages of the mailbox, until the behaviour
// terminates, or the process is killed: then without Terminate, as in
// Erlang.
func loop[S any](self *proc.Self, r *runner[S]) error {
	for {
		msg, err := self.Receive(context.Background())
		if err != nil {
			r.Abort()
			return err
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

func (e procEnv) Resolve(dest actor.Dest) (proc.PID, bool) { return dest.WhereIs(e.self.Node()) }

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
func (e procEnv) MonitorNodes(on bool)                   { e.self.MonitorNodes(on) }

func (e procEnv) Exit(to proc.PID, reason error) { e.self.Exit(to, reason) }
func (e procEnv) Ack(err error)                  { e.self.InitAck(err) }

func (e procEnv) StartLink(child Starter) (proc.PID, error) {
	return child.StartLink(context.Background(), e.self)
}

func (e procEnv) Now() time.Time { return time.Now() }

func (e procEnv) SendAfter(d time.Duration, msg any) func() {
	n, self := e.self.Node(), e.self.PID()
	t := time.AfterFunc(d, func() { n.Send(self, msg) })
	return func() { t.Stop() }
}

// Async runs run in a goroutine, its ctx done with the process too, and
// once run returns: a context of the process's not cancelled stays with it
// until the process ends.
func (e procEnv) Async(run func(context.Context) (any, error), done func(any, error) any) func() {
	n, self := e.self.Node(), e.self.PID()
	ctx, cancel := context.WithCancel(e.self.Context())
	go func() {
		v, err := RunAsync(ctx, run)
		cancel()
		n.Send(self, done(v, err))
	}()
	return cancel
}

// RunAsync runs run with ctx, its panic its error, for Envs.
func RunAsync(ctx context.Context, run func(context.Context) (any, error)) (v any, err error) {
	defer func() {
		if p := recover(); p != nil {
			v, err = nil, &proc.PanicError{Value: p, Stack: debug.Stack()}
		}
	}()
	return run(ctx)
}

func (e procEnv) Request(pid proc.PID, reply func(proc.Ref, proc.AliasMsg) any) (proc.Ref, func()) {
	a := e.self.Node().MonitorAliasFunc(pid, e.deliver(reply))
	return a.Ref, a.Release
}

func (e procEnv) RequestName(node, name string, reply func(proc.Ref, proc.AliasMsg) any) (proc.Ref, func()) {
	a := e.self.Node().MonitorAliasNameFunc(node, name, e.deliver(reply))
	return a.Ref, a.Release
}

// deliver puts what arrives for a request in the mailbox of the process,
// as reply makes it, in the goroutine of whoever sends it: none waits for
// it. The runtime releases the alias, answered or abandoned, and those
// left as the process ends.
func (e procEnv) deliver(reply func(proc.Ref, proc.AliasMsg) any) func(proc.Ref, proc.AliasMsg) {
	n, self := e.self.Node(), e.self.PID()
	return func(ref proc.Ref, m proc.AliasMsg) { n.Send(self, reply(ref, m)) }
}

func (e procEnv) Logger() *slog.Logger { return e.self.Node().Logger() }

func (e procEnv) Emit(event any) {
	if !e.dead() {
		e.self.Node().Emit(event)
	}
}
