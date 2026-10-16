package gen

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// downMsg is a Down the runner sends itself, for a Monitor of a process
// not found.
type downMsg struct{ actor.Down }

// monitors are the monitors of a behaviour, by the tag of their Monitor,
// and the tags by ref, for their DownMsg.
type monitors struct {
	env  Env
	refs map[any]proc.Ref // tag -> ref
	tags map[proc.Ref]any // ref -> tag
}

func newMonitors(env Env) monitors {
	return monitors{env: env, refs: make(map[any]proc.Ref), tags: make(map[proc.Ref]any)}
}

// monitor monitors the target of e, in place of one under its tag.
func (m *monitors) monitor(e actor.Monitor) {
	m.demonitor(e.Tag)
	if to, ok := e.Target.(actor.Remote); ok && to.Node != m.env.Self().Node() {
		ref := m.env.MonitorName(to.Node, to.Name)
		m.refs[e.Tag] = ref
		m.tags[ref] = e.Tag
		return
	}
	pid, ok := m.env.Resolve(e.Target)
	if !ok {
		m.env.Send(m.env.Self(), downMsg{actor.Down{Tag: e.Tag, Reason: proc.NoProc}})
		return
	}
	ref := m.env.Monitor(pid)
	m.refs[e.Tag] = ref
	m.tags[ref] = e.Tag
}

func (m *monitors) demonitor(tag any) {
	if ref, ok := m.refs[tag]; ok {
		m.env.Demonitor(ref)
		delete(m.refs, tag)
		delete(m.tags, ref)
	}
}

// down returns the Down of the monitor of d, by its tag, or d as it is,
// of a monitor not made by a Monitor effect.
func (m *monitors) down(d proc.DownMsg) Msg {
	tag, ok := m.tags[d.Ref]
	if !ok {
		return InfoMsg{Msg: d}
	}
	delete(m.tags, d.Ref)
	delete(m.refs, tag)
	return InfoMsg{Msg: actor.Down{Tag: tag, PID: d.PID, Reason: d.Reason}}
}
