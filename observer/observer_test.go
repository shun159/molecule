package observer_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/internal/testlog"
	"github.com/shun159/molecule/observer"
	"github.com/shun159/molecule/proc"
)

// crasher crashes on a cast.
type crasher struct{ genserver.Default[struct{}] }

func (crasher) HandleCast(struct{}, string) (struct{}, []molecule.Effect) { panic("crash") }

func startApp(t *testing.T, n *proc.Node) *application.Running {
	t.Helper()
	inner := supervisor.Spec{Children: []supervisor.ChildSpec{{ID: "c", Start: genserver.Child(crasher{})}}}
	r, err := application.Start(context.Background(), n, application.App{Name: "app", Start: supervisor.Child(supervisor.Spec{
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "a", Start: genserver.Child(crasher{})},
			{ID: "inner", Start: supervisor.Child(inner), Type: supervisor.Supervisor},
			{ID: "pool", Start: supervisor.DynamicChild(supervisor.DynamicSpec{}), Type: supervisor.Supervisor},
		},
	})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Stop(context.Background()) })
	return r
}

func treeOf(t *testing.T, n *proc.Node, r *application.Running) observer.Tree {
	t.Helper()
	top, _ := r.Top("app")
	tree, err := observer.TreeOf(context.Background(), n, top)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func TestTree(t *testing.T) {
	_, logger := testlog.New()
	n := proc.NewNode("", proc.WithLogger(logger))
	r := startApp(t, n)
	a := treeOf(t, n, r).Children[0].PID
	n.Send(a, molecule.CastMsg{Req: "crash"})
	deadline := time.Now().Add(5 * time.Second)
	for treeOf(t, n, r).Children[0].Restarts == 0 {
		if time.Now().After(deadline) {
			t.Fatal("a not restarted")
		}
		time.Sleep(time.Millisecond)
	}
	tree := treeOf(t, n, r)
	var b strings.Builder
	if err := observer.WriteTree(&b, tree); err != nil {
		t.Fatal(err)
	}
	c := tree.Children[1].Children[0]
	want := tree.PID.String() + "  supervisor\n" +
		"├─ a  " + tree.Children[0].PID.String() + "  genserver observer_test.crasher  restarted 1\n" +
		"├─ inner  " + tree.Children[1].PID.String() + "  supervisor\n" +
		"│  └─ c  " + c.PID.String() + "  genserver observer_test.crasher\n" +
		"└─ pool  " + tree.Children[2].PID.String() + "  dynamic supervisor\n"
	if b.String() != want {
		t.Errorf("tree\n%s\nwant\n%s", b.String(), want)
	}
}

func TestProcesses(t *testing.T) {
	n := proc.NewNode("")
	labeled := make(chan struct{}, 2)
	idle := n.Spawn(func(s *proc.Self) error {
		s.SetLabel("idle")
		labeled <- struct{}{}
		_, err := s.Receive(context.Background())
		return err
	})
	behind := n.Spawn(func(s *proc.Self) error {
		s.SetLabel("behind")
		labeled <- struct{}{}
		<-make(chan struct{})
		return nil
	})
	<-labeled
	<-labeled
	for range 3 {
		n.Send(behind, "work")
	}
	infos := observer.Processes(n)
	if len(infos) != 2 || infos[0].PID != behind || infos[0].MessageQueueLen != 3 || infos[1].PID != idle {
		t.Fatalf("processes %+v", infos)
	}
	var b strings.Builder
	observer.WriteProcesses(&b, infos)
	lines := strings.Split(strings.TrimSpace(b.String()), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "PID") || !strings.Contains(lines[1], " 3 ") || !strings.HasSuffix(lines[1], "behind") {
		t.Errorf("table\n%s", b.String())
	}
	n.Send(idle, "stop")
}

func get(t *testing.T, h *httptest.Server, path string) string {
	t.Helper()
	resp, err := h.Client().Get(h.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestHandler(t *testing.T) {
	n := proc.NewNode("obs")
	r := startApp(t, n)
	h := httptest.NewServer(observer.Handler(n, r))
	defer h.Close()

	if s := get(t, h, "/"); !strings.HasPrefix(s, "node obs: ") || !strings.Contains(s, "application app\n") || !strings.Contains(s, "└─ pool") {
		t.Errorf("/:\n%s", s)
	}
	if s := get(t, h, "/processes"); !strings.Contains(s, "dynamic supervisor") || !strings.Contains(s, "application master app") {
		t.Errorf("/processes:\n%s", s)
	}
	// The PIDs, as text, are for people: the JSON is read back without them.
	type tree struct {
		ID       string
		Label    string
		Children []tree
	}
	var trees []struct {
		App  string
		Tree tree
	}
	if err := json.Unmarshal([]byte(get(t, h, "/tree?format=json")), &trees); err != nil {
		t.Fatal(err)
	}
	if len(trees) != 1 || trees[0].App != "app" || len(trees[0].Tree.Children) != 3 || trees[0].Tree.Children[1].Children[0].ID != "c" {
		t.Errorf("/tree json %+v", trees)
	}
	var procs []map[string]any
	if err := json.Unmarshal([]byte(get(t, h, "/processes?format=json")), &procs); err != nil {
		t.Fatal(err)
	}
	if len(procs) == 0 || !strings.HasPrefix(procs[0]["PID"].(string), "<obs") {
		t.Errorf("/processes json %v", procs)
	}
	var root struct{ Stats proc.NodeStats }
	if err := json.Unmarshal([]byte(get(t, h, "/?format=json")), &root); err != nil {
		t.Fatal(err)
	}
	if root.Stats.Processes == 0 || root.Stats.Spawned < uint64(root.Stats.Processes) {
		t.Errorf("stats %+v", root.Stats)
	}
}
