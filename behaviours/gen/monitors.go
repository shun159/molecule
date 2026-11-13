package gen

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// downMsg is a Down the runner sends itself, for a Monitor of a process
// not found, of its gen.
type downMsg struct {
	actor.Down
	gen uint64
}

// monitors are the monitors of a behaviour, by the tag of their Monitor,
// and the tags by ref, for their DownMsg. Those of a process not found
// are by tag in missing, with the gen of their downMsg: one removed or
// replaced since has its downMsg dropped, as a timer cancelled.
type monitors struct {
	env     Env
	refs    map[any]proc.Ref // tag -> ref
	tags    map[proc.Ref]any // ref -> tag
	missing map[any]uint64   // tag -> gen
	gen     uint64
}

func newMonitors(env Env) monitors {
	return monitors{env: env, refs: make(map[any]proc.Ref), tags: make(map[proc.Ref]any), missing: make(map[any]uint64)}
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
		m.gen++
		m.missing[e.Tag] = m.gen
		m.env.Send(m.env.Self(), downMsg{actor.Down{Tag: e.Tag, Reason: proc.NoProc}, m.gen})
		return
	}
	ref := m.env.Monitor(pid)
	m.refs[e.Tag] = ref
	m.tags[ref] = e.Tag
}

func (m *monitors) demonitor(tag any) {
	delete(m.missing, tag)
	if ref, ok := m.refs[tag]; ok {
		m.env.Demonitor(ref)
		delete(m.refs, tag)
		delete(m.tags, ref)
	}
}

// notFound returns the Down of d, of a monitor of a process not found,
// and false if that monitor was removed or replaced since.
func (m *monitors) notFound(d downMsg) (Msg, bool) {
	if gen, ok := m.missing[d.Tag]; !ok || gen != d.gen {
		return nil, false
	}
	delete(m.missing, d.Tag)
	return InfoMsg{Msg: d.Down}, true
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
