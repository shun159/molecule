package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// App is an application: a tree of processes started and stopped as a
// whole, from its top, usually a supervisor.
type App struct {
	Name  string
	Start supervisor.Starter
	// Restart is what the end of its top does: a Permanent application
	// takes the others down with it, a Temporary one is only reported.
	Restart Restart
	// Shutdown is how long its top is given to stop, before it is killed;
	// zero waits however long it takes, as a supervisor stopping its
	// children in turn may need.
	Shutdown time.Duration
}

// Restart is the type of an application, as in OTP.
type Restart int

const (
	// Permanent: when its top ends, all the applications stop.
	Permanent Restart = iota
	// Temporary: when its top ends, it is reported, and the others run on.
	Temporary
)

// Running are applications started.
type Running struct {
	n    *proc.Node
	apps []*running

	once   sync.Once
	done   chan struct{}
	reason error
}

type running struct {
	app    App
	master proc.PID
	top    proc.PID
}

// Messages to the master of an application.
type (
	stopApp struct{}
	// killTop kills the top that would not stop in time.
	killTop struct{}
)

// Start starts apps on n, in order, and returns once they all have
// started. If one fails to start, those already started are stopped, in
// reverse order, and the error returned.
func Start(ctx context.Context, n *proc.Node, apps ...App) (*Running, error) {
	r := &Running{n: n, done: make(chan struct{})}
	for _, app := range apps {
		a, err := r.start(ctx, app)
		if err != nil {
			r.stopAll(context.Background())
			return nil, fmt.Errorf("application %s: %w", app.Name, err)
		}
		r.apps = append(r.apps, a)
	}
	return r, nil
}

// start starts the master of app, which starts its top, linked to it.
func (r *Running) start(ctx context.Context, app App) (*running, error) {
	a := &running{app: app}
	tops := make(chan proc.PID, 1)
	pid, err := r.n.Start(ctx, func(self *proc.Self) error {
		self.TrapExit(true)
		top, err := app.Start.StartLink(ctx, self)
		if err != nil {
			self.InitAck(err)
			return nil
		}
		tops <- top
		self.SetLabel("application master " + app.Name)
		self.InitAck(nil)
		return r.master(self, a, top)
	})
	if err != nil {
		return nil, err
	}
	a.master, a.top = pid, <-tops
	return a, nil
}

// master watches the top of an application until it ends, or stops it
// when asked.
func (r *Running) master(self *proc.Self, a *running, top proc.PID) error {
	var stoppers []molecule.From
	for {
		msg, err := self.Receive(context.Background())
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case proc.ExitMsg:
			if m.From != top {
				continue
			}
			if stoppers != nil {
				for _, from := range stoppers {
					molecule.SendReply(self, from, nil)
				}
				return nil
			}
			r.ended(a, m.Reason)
			return nil
		case molecule.CallMsg:
			if _, ok := m.Req.(stopApp); !ok {
				continue
			}
			if stoppers == nil {
				self.Exit(top, proc.Shutdown)
				if d := a.app.Shutdown; d > 0 {
					n, pid := self.Node(), self.PID()
					time.AfterFunc(d, func() { n.Send(pid, killTop{}) })
				}
			}
			stoppers = append(stoppers, m.From)
		case killTop:
			self.Exit(top, proc.Kill)
		}
	}
}

// ended handles the end of the top of a, not stopped.
func (r *Running) ended(a *running, reason error) {
	level := slog.LevelInfo
	if proc.IsAbnormal(reason) {
		level = slog.LevelError
	}
	kind := "permanent"
	if a.app.Restart == Temporary {
		kind = "temporary"
	}
	r.n.Logger().Log(context.Background(), level, "application exited",
		slog.String("application", a.app.Name), slog.String("reason", reason.Error()), slog.String("type", kind))
	if a.app.Restart == Permanent {
		r.finish(fmt.Errorf("application %s exited: %w", a.app.Name, reason))
	}
}

func (r *Running) finish(reason error) {
	r.once.Do(func() {
		r.reason = reason
		close(r.done)
	})
}

// Done is closed when a permanent application has ended, or the
// applications are stopped.
func (r *Running) Done() <-chan struct{} { return r.done }

// Err returns why the applications ended: the end of a permanent one, or
// nil once stopped.
func (r *Running) Err() error {
	select {
	case <-r.done:
		return r.reason
	default:
		return nil
	}
}

// Apps returns the names of the applications, in the order they started.
func (r *Running) Apps() []string {
	var names []string
	for _, a := range r.apps {
		names = append(names, a.app.Name)
	}
	return names
}

// Top returns the top process of the application name.
func (r *Running) Top(name string) (proc.PID, bool) {
	for _, a := range r.apps {
		if a.app.Name == name {
			return a.top, true
		}
	}
	return proc.PID{}, false
}

// Stop stops the applications, in reverse order, each once its top has
// stopped, and returns once they all have, or ctx is done.
func (r *Running) Stop(ctx context.Context) error {
	r.finish(nil)
	return r.stopAll(ctx)
}

func (r *Running) stopAll(ctx context.Context) error {
	var errs []error
	for i := len(r.apps) - 1; i >= 0; i-- {
		a := r.apps[i]
		_, err := molecule.Call(ctx, r.n, a.master, stopApp{})
		var exit *molecule.ExitError
		if err != nil && !errors.As(err, &exit) { // a master gone has nothing to stop
			errs = append(errs, fmt.Errorf("application %s: %w", a.app.Name, err))
		}
	}
	return errors.Join(errs...)
}

// Run starts apps on n, and runs them until ctx is done, the program gets
// SIGINT or SIGTERM, or a permanent application ends; then it stops them,
// in reverse order, and returns: nil, or the end of the permanent
// application. It is what main does.
func Run(ctx context.Context, n *proc.Node, apps ...App) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	r, err := Start(ctx, n, apps...)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
	case <-r.Done():
	}
	reason := r.Err()
	if err := r.stopAll(context.Background()); err != nil && reason == nil {
		reason = err
	}
	r.finish(nil)
	return reason
}
