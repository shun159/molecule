package proc

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/shun159/molecule/internal/testlog"
)

func crashReports(rec *testlog.Recorder) []testlog.Record { return rec.Records("crash report") }

func TestCrashReport(t *testing.T) {
	rec, logger := testlog.New()
	n := NewNode("", WithLogger(logger))

	parent := make(chan PID, 1)
	n.spawn(func(s *Self) error {
		parent <- s.PID()
		child := s.Spawn(func(c *Self) error {
			if err := c.Node().Register("worker", c.PID()); err != nil {
				return err
			}
			return errBoom
		})
		<-n.lookup(child).ctx.Done()
		return nil
	})
	parentPID := <-parent
	// The parent waits for the child, so once it is gone, all is logged.
	<-n.lookup(parentPID).ctx.Done()

	reports := crashReports(rec)
	if len(reports) != 1 {
		t.Fatalf("crash reports = %+v", reports)
	}
	r := reports[0]
	for key, want := range map[string]string{
		"reason":          errBoom.Error(),
		"registered_name": "worker",
		"parent":          parentPID.String(),
	} {
		if r.Attrs[key] != want {
			t.Errorf("%s = %q, want %q", key, r.Attrs[key], want)
		}
	}
	if !strings.Contains(r.Attrs["initial_call"], "TestCrashReport") || r.Level != slog.LevelError {
		t.Errorf("report = %+v", r)
	}
}

func TestCrashReportPanic(t *testing.T) {
	rec, logger := testlog.New()
	n := NewNode("", WithLogger(logger))
	exitReason(t, n.spawn(func(*Self) error { panic("oops") }))
	reports := crashReports(rec)
	if len(reports) != 1 || !strings.Contains(reports[0].Attrs["stack"], "TestCrashReportPanic") {
		t.Errorf("crash reports = %+v", reports)
	}
}

func TestNoCrashReport(t *testing.T) {
	rec, logger := testlog.New()
	n := NewNode("", WithLogger(logger))
	for _, reason := range []error{nil, Normal, Shutdown, fmt.Errorf("%w: done", Shutdown)} {
		exitReason(t, n.spawn(func(*Self) error { return reason }))
	}

	// Killed: no code of its own failed.
	victim, _ := actor(n, nil)
	do(n, func(s *Self) { s.Exit(victim.pid, Kill) })
	exitReason(t, victim)

	if reports := crashReports(rec); len(reports) != 0 {
		t.Errorf("crash reports = %+v", reports)
	}
}

// TestCrashReportLinkCascade checks that of two linked processes, only the
// one that failed is reported, not the one taken down with it.
func TestCrashReportLinkCascade(t *testing.T) {
	rec, logger := testlog.New()
	n := NewNode("", WithLogger(logger))
	a, _ := actor(n, nil)
	b, _ := actor(n, func(s *Self) { s.Link(a.pid) })

	n.Send(a.pid, exitWith{errBoom})
	exitReason(t, a)
	if r := exitReason(t, b); r != errBoom {
		t.Fatalf("b exit reason = %v", r)
	}
	reports := crashReports(rec)
	if len(reports) != 1 || reports[0].Attrs["pid"] != a.pid.String() {
		t.Errorf("crash reports = %+v", reports)
	}
}

func TestDefaultLogger(t *testing.T) {
	rec, logger := testlog.New()
	old := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(old)

	n := NewNode("") // no logger: the default at the time of the report
	exitReason(t, n.spawn(func(*Self) error { return errBoom }))
	if len(crashReports(rec)) != 1 {
		t.Error("crash report not sent to slog.Default()")
	}
}
