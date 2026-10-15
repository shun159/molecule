package observer

import (
	"context"
	"testing"
	"time"

	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// A supervisor listed, gone as it is asked, is a child without children,
// not an error of the whole tree.
func TestChildGone(t *testing.T) {
	n := proc.NewNode("")
	gone := n.Spawn(func(*proc.Self) error { return nil })
	time.Sleep(10 * time.Millisecond)
	c := supervisor.ChildInfo{ID: "inner", PID: gone, Type: supervisor.Supervisor, Restarts: 2}
	sub, err := child(context.Background(), n, c)
	if err != nil || sub.ID != "inner" || sub.PID != gone || sub.Restarts != 2 || len(sub.Children) != 0 {
		t.Errorf("child %+v, %v; want inner as listed, without children", sub, err)
	}
}
