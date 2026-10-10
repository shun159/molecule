package application_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/internal/testlog"
	"github.com/shun159/molecule/proc"
)

// events records what the workers tell, in order.
type events struct {
	mu   sync.Mutex
	list []string
}

func (e *events) add(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.list = append(e.list, s)
}

func (e *events) get() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.list)
}

// worker tells when it starts and stops, and crashes on a cast.
type worker struct {
	genserver.Default[struct{}]
	id  string
	log *events
}

func (w worker) Init(proc.PID) (struct{}, []molecule.Effect, error) {
	w.log.add("start " + w.id)
	return struct{}{}, molecule.Do(molecule.TrapExit{On: true}), nil
}

func (worker) HandleCast(struct{}, string) (struct{}, []molecule.Effect) { panic("crash") }

func (w worker) Terminate(_ struct{}, reason error) []molecule.Effect {
	if !proc.IsAbnormal(reason) {
		w.log.add("stop " + w.id)
	}
	return nil
}

func app(name string, log *events, ids ...string) application.App {
	var children []supervisor.ChildSpec
	for _, id := range ids {
		children = append(children, supervisor.ChildSpec{ID: id, Start: genserver.Child(worker{id: id, log: log})})
	}
	return application.App{Name: name, Start: supervisor.Child(supervisor.Spec{Children: children})}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStartStop(t *testing.T) {
	n := proc.NewNode("")
	var log events
	r, err := application.Start(context.Background(), n, app("one", &log, "a", "b"), app("two", &log, "c"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start a", "start b", "start c", "stop c", "stop b", "stop a"}
	if got := log.get(); !slices.Equal(got, want) {
		t.Errorf("events %v, want %v", got, want)
	}
	if r.Err() != nil {
		t.Errorf("Err after Stop: %v", r.Err())
	}
}

// child returns the child id of the top of the application name.
func child(t *testing.T, n *proc.Node, r *application.Running, name, id string) proc.PID {
	t.Helper()
	top, _ := r.Top(name)
	cs, err := supervisor.WhichChildren(context.Background(), n, top)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cs {
		if c.ID == id {
			return c.PID
		}
	}
	t.Fatalf("no child %s", id)
	return proc.PID{}
}

// crash crashes the child id of the application name, once it runs again
// after its crash before, prev, and waits for it to be down. A process is
// dead to IsAlive before its supervisor learns of it, so the supervisor may
// still report prev for a moment: a crash sent there would be lost.
func crash(t *testing.T, n *proc.Node, r *application.Running, name, id string, prev proc.PID) proc.PID {
	t.Helper()
	var c proc.PID
	eventually(t, id+" restarted", func() bool {
		c = child(t, n, r, name, id)
		return c != prev && n.IsAlive(c)
	})
	n.Send(c, molecule.CastMsg{Req: "crash"})
	eventually(t, id+" down", func() bool { return !n.IsAlive(c) })
	return c
}

func TestPermanentEnds(t *testing.T) {
	rec, logger := testlog.New()
	n := proc.NewNode("", proc.WithLogger(logger))
	var log events
	r, err := application.Start(context.Background(), n, app("one", &log, "a"), app("two", &log, "b"))
	if err != nil {
		t.Fatal(err)
	}
	// Two crashes in a row are one too many: "two" gives up.
	var b proc.PID
	for range 2 {
		b = crash(t, n, r, "two", "b", b)
	}
	select {
	case <-r.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("not done")
	}
	if err := r.Err(); err == nil || !strings.Contains(err.Error(), "application two exited") || !errors.Is(err, supervisor.ErrMaxIntensity) {
		t.Errorf("Err %v", err)
	}
	if rs := rec.Records("application exited"); len(rs) != 1 || rs[0].Attrs["application"] != "two" {
		t.Errorf("reports %+v", rs)
	}
	r.Stop(context.Background())
	if got := log.get(); got[len(got)-1] != "stop a" {
		t.Errorf("one not stopped: %v", got)
	}
}

func TestTemporaryEnds(t *testing.T) {
	n := proc.NewNode("")
	var log events
	temp := app("two", &log, "b")
	temp.Restart = application.Temporary
	r, err := application.Start(context.Background(), n, app("one", &log, "a"), temp)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	top, _ := r.Top("two")
	var b proc.PID
	for range 2 {
		b = crash(t, n, r, "two", "b", b)
	}
	eventually(t, "two down", func() bool { return !n.IsAlive(top) })
	select {
	case <-r.Done():
		t.Error("done with a temporary application")
	case <-time.After(20 * time.Millisecond):
	}
	if a := child(t, n, r, "one", "a"); !n.IsAlive(a) {
		t.Error("one not running")
	}
}

func TestStartFails(t *testing.T) {
	n := proc.NewNode("")
	var log events
	bad := application.App{Name: "bad", Start: supervisor.StartFunc(func(context.Context, *proc.Self) (proc.PID, error) {
		return proc.PID{}, errors.New("no")
	})}
	_, err := application.Start(context.Background(), n, app("one", &log, "a"), bad)
	if err == nil || !strings.Contains(err.Error(), "application bad") {
		t.Errorf("Start: %v", err)
	}
	if got := log.get(); !slices.Equal(got, []string{"start a", "stop a"}) {
		t.Errorf("events %v", got)
	}
}

func TestShutdownTime(t *testing.T) {
	n := proc.NewNode("")
	// A top that ignores being asked to stop.
	stubborn := supervisor.StartFunc(func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
		return parent.StartLink(ctx, func(s *proc.Self) error {
			s.TrapExit(true)
			s.InitAck(nil)
			for {
				if _, err := s.Receive(context.Background()); err != nil {
					return err
				}
			}
		})
	})
	r, err := application.Start(context.Background(), n, application.App{Name: "s", Start: stubborn, Shutdown: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	top, _ := r.Top("s")
	start := time.Now()
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n.IsAlive(top) || time.Since(start) < 20*time.Millisecond {
		t.Errorf("alive %v after %v", n.IsAlive(top), time.Since(start))
	}
}

func TestRun(t *testing.T) {
	n := proc.NewNode("")
	var log events
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx, n, app("one", &log, "a")) }()
	eventually(t, "a started", func() bool { return len(log.get()) == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
	if got := log.get(); !slices.Equal(got, []string{"start a", "stop a"}) {
		t.Errorf("events %v", got)
	}
}

// Applications started later stop before those started earlier.
func TestStartLater(t *testing.T) {
	n := proc.NewNode("")
	var log events
	r, err := application.Start(context.Background(), n, app("one", &log, "a"))
	if err != nil {
		t.Fatal(err)
	}
	log.add("work in between")
	if err := r.Start(context.Background(), app("two", &log, "b"), app("three", &log, "c")); err != nil {
		t.Fatal(err)
	}
	if got := r.Apps(); !slices.Equal(got, []string{"one", "two", "three"}) {
		t.Errorf("Apps %v", got)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start a", "work in between", "start b", "start c", "stop c", "stop b", "stop a"}
	if got := log.get(); !slices.Equal(got, want) {
		t.Errorf("events %v, want %v", got, want)
	}
}

// A later start failing stops what it started, and leaves the others.
func TestStartLaterFails(t *testing.T) {
	n := proc.NewNode("")
	var log events
	r, err := application.Start(context.Background(), n, app("one", &log, "a"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	bad := application.App{Name: "bad", Start: supervisor.StartFunc(func(context.Context, *proc.Self) (proc.PID, error) {
		return proc.PID{}, errors.New("no")
	})}
	if err := r.Start(context.Background(), app("two", &log, "b"), bad); err == nil || !strings.Contains(err.Error(), "application bad") {
		t.Fatalf("Start: %v", err)
	}
	if got := log.get(); !slices.Equal(got, []string{"start a", "start b", "stop b"}) {
		t.Errorf("events %v", got)
	}
	if got := r.Apps(); !slices.Equal(got, []string{"one"}) {
		t.Errorf("Apps %v", got)
	}
}

func TestStartAfterStop(t *testing.T) {
	n := proc.NewNode("")
	var log events
	r, err := application.Start(context.Background(), n, app("one", &log, "a"))
	if err != nil {
		t.Fatal(err)
	}
	r.Stop(context.Background())
	if err := r.Start(context.Background(), app("two", &log, "b")); !errors.Is(err, application.ErrStopped) {
		t.Fatalf("Start after Stop: %v", err)
	}
	if got := log.get(); !slices.Equal(got, []string{"start a", "stop a"}) {
		t.Errorf("events %v", got)
	}
}
