package gen

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// downMsg is a Down the runner sends itself, for a Monitor of a process
// not found.
type downMsg struct{ actor.Down }

func (r *runner[S]) monitor(e actor.Monitor) {
	r.demonitor(e.Tag)
	if to, ok := e.Target.(actor.Remote); ok && to.Node != r.env.Self().Node() {
		ref := r.env.MonitorName(to.Node, to.Name)
		r.monitors[e.Tag] = ref
		r.tags[ref] = e.Tag
		return
	}
	pid, ok := r.env.Resolve(e.Target)
	if !ok {
		r.env.Send(r.env.Self(), downMsg{actor.Down{Tag: e.Tag, Reason: proc.NoProc}})
		return
	}
	ref := r.env.Monitor(pid)
	r.monitors[e.Tag] = ref
	r.tags[ref] = e.Tag
}

func (r *runner[S]) demonitor(tag any) {
	if ref, ok := r.monitors[tag]; ok {
		r.env.Demonitor(ref)
		delete(r.monitors, tag)
		delete(r.tags, ref)
	}
}
