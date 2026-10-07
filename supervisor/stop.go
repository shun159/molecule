package supervisor

import (
	"context"
	"time"

	"github.com/shun159/molecule/proc"
)

// stopAll stops the running children among cs at once: each is asked to
// stop with proc.Shutdown, or killed if its Shutdown is Brutal, then killed
// if still alive once its Shutdown has passed. It returns when all are
// dead, or when self dies. The children are left without a PID; their
// EXIT messages, arriving later, are from unknown PIDs.
func stopAll(self *proc.Self, cs []*child) {
	type stopping struct {
		pid      proc.PID
		down     context.Context
		release  func()
		deadline time.Time // zero: wait however long it takes
	}
	n, now := self.Node(), time.Now()
	var all []stopping
	for _, c := range cs {
		if c.pid.IsZero() {
			continue
		}
		st := stopping{pid: c.pid}
		st.down, st.release = n.Watch(context.Background(), c.pid)
		c.pid = proc.PID{}

		switch d := c.spec.shutdown(); d {
		case Brutal:
			self.Exit(st.pid, proc.Kill)
		case Infinity:
			self.Exit(st.pid, proc.Shutdown)
		default:
			self.Exit(st.pid, proc.Shutdown)
			st.deadline = now.Add(d)
		}
		all = append(all, st)
	}

	for _, st := range all {
		defer st.release()
		if !st.deadline.IsZero() {
			t := time.NewTimer(time.Until(st.deadline))
			select {
			case <-st.down.Done():
				t.Stop()
				continue
			case <-self.Context().Done():
				t.Stop()
				return
			case <-t.C:
				self.Exit(st.pid, proc.Kill)
			}
		}
		select {
		case <-st.down.Done():
		case <-self.Context().Done():
			return
		}
	}
}
