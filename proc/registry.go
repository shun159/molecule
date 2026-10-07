package proc

import (
	"errors"
	"maps"
	"slices"
)

// Registration errors.
var (
	ErrEmptyName = errors.New("empty name")
	ErrNameTaken = errors.New("name already registered")
	ErrHasName   = errors.New("process already registered under another name")
	ErrNotLocal  = errors.New("process is not on this node")
)

// Register associates name with the local process pid, like
// erlang:register/2. A process has at most one name, and the name is
// removed when the process dies, before its links and monitors are
// notified. It fails with NoProc if pid is not alive.
//
// Registration and death are ordered by the registry lock: death cancels
// the process before taking the lock to remove the name, so a Register
// that still sees the process alive under the lock is undone by that
// removal, and one that comes after sees it dead.
func (n *Node) Register(name string, pid PID) error {
	if name == "" {
		return ErrEmptyName
	}
	if !n.isLocal(pid) {
		return ErrNotLocal
	}
	n.regMu.Lock()
	defer n.regMu.Unlock()
	p := n.lookup(pid)
	switch {
	case p == nil || p.dead.Load():
		return NoProc
	case n.names[name] == pid:
		return nil
	case !n.names[name].IsZero():
		return ErrNameTaken
	case p.name != "":
		return ErrHasName
	}
	n.names[name] = pid
	p.name = name
	return nil
}

// Unregister removes name, reporting whether it was registered.
func (n *Node) Unregister(name string) bool {
	n.regMu.Lock()
	defer n.regMu.Unlock()
	pid, ok := n.names[name]
	if !ok {
		return false
	}
	delete(n.names, name)
	if p := n.lookup(pid); p != nil {
		p.name = ""
	}
	return true
}

// WhereIs returns the process registered under name.
func (n *Node) WhereIs(name string) (PID, bool) {
	n.regMu.Lock()
	defer n.regMu.Unlock()
	pid, ok := n.names[name]
	return pid, ok
}

// Registered returns the registered names, sorted.
func (n *Node) Registered() []string {
	n.regMu.Lock()
	defer n.regMu.Unlock()
	return slices.Sorted(maps.Keys(n.names))
}

// unregisterDead removes the name of p, which has just died. The name may
// have been unregistered and taken by another process meanwhile, so it is
// only removed while it still refers to p.
func (n *Node) unregisterDead(p *process) {
	n.regMu.Lock()
	defer n.regMu.Unlock()
	if p.name != "" && n.names[p.name] == p.pid {
		delete(n.names, p.name)
	}
	p.name = ""
}
