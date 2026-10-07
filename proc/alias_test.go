package proc

import "testing"

func aliasCount(n *Node) int {
	c := 0
	n.aliases.Range(func(any, any) bool { c++; return true })
	return c
}

func TestAlias(t *testing.T) {
	n := NewNode("")
	ref, msgs, unalias := n.Alias()
	defer unalias()

	n.SendAlias(ref, "first")
	if m := <-msgs; m != "first" {
		t.Errorf("got %v", m)
	}
	n.SendAlias(ref, "second") // the alias is gone after the first
	select {
	case m := <-msgs:
		t.Errorf("one-shot alias delivered %v", m)
	default:
	}
	if aliasCount(n) != 0 {
		t.Error("alias still active after delivery")
	}
}

func TestUnalias(t *testing.T) {
	n := NewNode("")
	ref, msgs, unalias := n.Alias()
	unalias()
	n.SendAlias(ref, "late")
	select {
	case m := <-msgs:
		t.Errorf("inactive alias delivered %v", m)
	default:
	}
	if aliasCount(n) != 0 {
		t.Error("alias not released")
	}
	unalias() // idempotent
}

func TestAliasForeign(t *testing.T) {
	n := NewNode("a@host")
	other := NewNode("b@host")
	ref, msgs, unalias := other.Alias()
	defer unalias()

	n.SendAlias(ref, "remote") // TODO(dist): dropped for now
	stale := ref
	stale.creation++
	other.SendAlias(stale, "stale")
	select {
	case m := <-msgs:
		t.Errorf("delivered %v", m)
	default:
	}
}

func TestPIDWhereIs(t *testing.T) {
	n := NewNode("")
	pid := n.NewPIDForTest(42)
	if got, ok := pid.WhereIs(n); !ok || got != pid {
		t.Errorf("WhereIs = %v, %v", got, ok)
	}
	if _, ok := (PID{}).WhereIs(n); ok {
		t.Error("zero PID resolved")
	}
	if n.Node() != n {
		t.Error("Node() is not the node itself")
	}
}
