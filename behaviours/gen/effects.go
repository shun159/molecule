package gen

import (
	"context"
	"fmt"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

func (r *runner[S]) apply(effs []actor.Effect) {
	for _, e := range effs {
		switch e := e.(type) {
		case actor.Continue:
			r.continues = append(r.continues, e.Msg)
		case actor.Reply:
			r.env.SendAlias(e.To.Tag, e.Value)
		case actor.Send:
			if to, ok := e.To.(actor.Remote); ok {
				r.env.SendName(to.Node, to.Name, e.Msg)
			} else if pid, ok := r.env.Resolve(e.To); ok {
				r.env.Send(pid, e.Msg)
			}
		case actor.Cast:
			if to, ok := e.To.(actor.Remote); ok {
				r.env.SendName(to.Node, to.Name, actor.CastMsg{Req: e.Req})
			} else if pid, ok := r.env.Resolve(e.To); ok {
				r.env.Send(pid, actor.CastMsg{Req: e.Req})
			}
		case actor.Stop:
			if !r.stopping {
				r.stopping = true
				r.stopReason = e.Reason
				if r.stopReason == nil {
					r.stopReason = proc.Normal
				}
			}
		case actor.Monitor:
			r.monitors.monitor(e)
		case actor.Demonitor:
			r.monitors.demonitor(e.Tag)
		case actor.StartTimer:
			r.timers.start(e)
		case actor.CancelTimer:
			r.timers.cancel(e.Key)
		case actor.SendRequest:
			r.requests.send(e)
		case actor.Async:
			r.asyncs.start(e)
		case actor.CancelAsync:
			r.asyncs.cancel(e.Key)
			r.asyncs.runWaiting()
		case actor.Link:
			r.env.Link(e.PID)
		case actor.Unlink:
			r.env.Unlink(e.PID)
		case actor.TrapExit:
			r.env.TrapExit(e.On)
		case actor.MonitorNodes:
			r.env.MonitorNodes(e.On)
		case actor.Exit:
			if pid, ok := r.env.Resolve(e.To); ok {
				r.env.Exit(pid, e.Reason)
			}
		case StartChild:
			pid, err := r.env.StartLink(e.Child)
			r.continues = append(r.continues, Started{Tag: e.Tag, PID: pid, Err: err})
		case Log:
			r.env.Logger().Log(context.Background(), e.Level, e.Msg, e.Attrs...)
		case Emit:
			r.env.Emit(e.Event)
		case AckLater:
			r.ackLater = true
		case Ack:
			r.env.Ack(e.Err)
		case Performer:
			e.Perform(r.env)
		default:
			panic(fmt.Sprintf("gen: unknown effect %T", e))
		}
	}
}
