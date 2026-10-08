package supervisor_test

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

func (w *world) reports(msg string) []map[string]string {
	w.t.Helper()
	var out []map[string]string
	for _, r := range w.log.Records(msg) {
		out = append(out, r.Attrs)
	}
	return out
}

func TestSupervisorReports(t *testing.T) {
	inWorld(t, func(w *world) {
		sup := w.start(supervisor.Spec{
			Intensity: 1,
			Period:    time.Minute,
			Children: []supervisor.ChildSpec{
				w.worker("a", supervisor.Transient),
				w.worker("b", supervisor.Permanent),
			},
		})
		w.expect(started{"a"}, started{"b"})
		progress := w.log.Records("supervisor child started")
		if len(progress) != 2 || progress[0].Level != slog.LevelDebug || progress[1].Attrs["child_id"] != "b" {
			t.Errorf("progress reports = %+v", progress)
		}

		w.call(sup, "a", stopNormal{}) // normal: not reported
		w.expect(stopped{"a", proc.Normal})
		if r := w.reports("supervisor child terminated"); len(r) != 0 {
			t.Errorf("normal exit reported: %+v", r)
		}

		w.call(sup, "b", crash{})
		w.expect(stopped{"b", errBoom}, started{"b"})
		r := w.reports("supervisor child terminated")
		if len(r) != 1 || r[0]["child_id"] != "b" || r[0]["reason"] != errBoom.Error() ||
			r[0]["restart"] != "permanent" || r[0]["supervisor"] != sup.String() {
			t.Errorf("termination reports = %+v", r)
		}

		w.call(sup, "b", crash{}) // over the intensity
		w.expect(stopped{"b", errBoom})
		synctest.Wait()
		if r := w.reports("supervisor shutting down"); len(r) != 1 || r[0]["reason"] != supervisor.ErrMaxIntensity.Error() {
			t.Errorf("shutdown reports = %+v", r)
		}
	})
}

func TestStartFailedReports(t *testing.T) {
	inWorld(t, func(w *world) {
		var fail atomic.Int32
		sup := w.start(supervisor.Spec{Intensity: 5, Children: []supervisor.ChildSpec{
			{ID: "a", Start: genserver.StartLinkFunc(worker{id: "a", observer: w.observer, failInit: &fail})},
		}})
		w.expect(started{"a"})
		fail.Store(2)
		w.call(sup, "a", crash{})
		w.expect(stopped{"a", errBoom}, started{"a"})
		r := w.reports("supervisor child start failed")
		if len(r) != 2 || r[0]["child_id"] != "a" || r[0]["reason"] != errInit.Error() {
			t.Errorf("start failure reports = %+v", r)
		}
	})
}

func TestDynamicReports(t *testing.T) {
	inWorld(t, func(w *world) {
		sup := w.startDynamic(supervisor.DynamicSpec{})
		pid := w.startChild(sup, w.worker("a", supervisor.Temporary))
		w.expect(started{"a"})
		w.crash(pid, crash{})
		w.expect(stopped{"a", errBoom})
		synctest.Wait()
		r := w.reports("supervisor child terminated")
		if len(r) != 1 || r[0]["child_pid"] != pid.String() || r[0]["restart"] != "temporary" {
			t.Errorf("termination reports = %+v", r)
		}
		if _, ok := r[0]["child_id"]; ok {
			t.Error("dynamic child reported with an ID")
		}
	})
}
