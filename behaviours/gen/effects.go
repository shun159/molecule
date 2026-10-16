package gen

import (
	"context"
	"fmt"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// apply performs effs, in order.
func (r *runner[S]) apply(effs []actor.Effect) {
	for _, e := range effs {
		r.perform(e)
	}
}

// perform performs e: on other processes, on what the runner keeps track
// of, on the process itself, on the loop of the runner, or outside.
func (r *runner[S]) perform(e actor.Effect) {
	switch e := e.(type) {
	// On other processes.
	case actor.Send:
		r.send(e.To, e.Msg)
	case actor.Cast:
		r.send(e.To, actor.CastMsg{Req: e.Req})
	case actor.Reply:
		r.env.SendAlias(e.To.Tag, e.Value)
	case actor.Exit:
		if pid, ok := r.env.Resolve(e.To); ok {
			r.env.Exit(pid, e.Reason)
		}

	// On what the runner keeps track of.
	case actor.Monitor:
		r.monitors.monitor(e)
	case actor.Demonitor:
		r.monitors.demonitor(e.Tag)
	case actor.StartTimer:
		r.timers.start(e)
	case actor.CancelTimer:
		r.timers.cancel(e.Key)
	case actor.Async:
		r.asyncs.start(e)
	case actor.CancelAsync:
		r.asyncs.cancel(e.Key)
		r.asyncs.runWaiting()
	case actor.SendRequest:
		r.requests.send(e)

	// On the process itself.
	case actor.Link:
		r.env.Link(e.PID)
	case actor.Unlink:
		r.env.Unlink(e.PID)
	case actor.TrapExit:
		r.env.TrapExit(e.On)
	case actor.MonitorNodes:
		r.env.MonitorNodes(e.On)
	case StartChild:
		pid, err := r.env.StartLink(e.Child)
		r.continues = append(r.continues, Started{Tag: e.Tag, PID: pid, Err: err})
	case AckLater:
		r.ackLater = true
	case Ack:
		r.env.Ack(e.Err)

	// On the loop of the runner.
	case actor.Continue:
		r.continues = append(r.continues, e.Msg)
	case actor.Stop:
		r.stop(e.Reason)

	// Outside.
	case Log:
		r.env.Logger().Log(context.Background(), e.Level, e.Msg, e.Attrs...)
	case Emit:
		r.env.Emit(e.Event)
	case Performer:
		e.Perform(r.env)

	default:
		panic(fmt.Sprintf("gen: unknown effect %T", e))
	}
}

// send sends msg to to: by its name, if Remote, or to the process it
// resolves to, if any.
func (r *runner[S]) send(to actor.Dest, msg any) {
	if rem, ok := to.(actor.Remote); ok {
		r.env.SendName(rem.Node, rem.Name, msg)
	} else if pid, ok := r.env.Resolve(to); ok {
		r.env.Send(pid, msg)
	}
}

// stop has the behaviour stop with reason, Normal if nil, once the effects
// before are performed. The first Stop is the one.
func (r *runner[S]) stop(reason error) {
	if r.stopping {
		return
	}
	if reason == nil {
		reason = proc.Normal
	}
	r.stopping, r.stopReason = true, reason
}
