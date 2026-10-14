package kvs

import (
	"context"
	"testing"

	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// The table of a Child is open while it runs, for Opened, and closed once
// it stops.
func TestChild(t *testing.T) {
	dir := t.TempDir()
	if _, err := Opened[string, int](dir); err == nil {
		t.Fatal("opened before the child started")
	}

	n := proc.NewNode("a")
	ctx := context.Background()
	sup, err := supervisor.Start(ctx, n, supervisor.Spec{
		Children: []supervisor.ChildSpec{{ID: "table", Start: Child(dir, Options[string, int]{})}},
	})
	if err != nil {
		t.Fatal(err)
	}
	table, err := Opened[string, int](dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Put("k", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := Opened[string, string](dir); err == nil {
		t.Error("opened as a table of another type")
	}
	if err := supervisor.Stop(ctx, n, sup); err != nil {
		t.Fatal(err)
	}
	if _, err := Opened[string, int](dir); err == nil {
		t.Error("opened once the child stopped")
	}

	table, err = Open(dir, Options[string, int]{}) // only once closed
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	if v, ok := table.Get("k"); !ok || v != 1 {
		t.Errorf("got %v, %v from the table opened again", v, ok)
	}
}
