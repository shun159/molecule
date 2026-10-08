package supervisor

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/shun159/molecule/proc"
)

func TestShouldRestart(t *testing.T) {
	boom := errors.New("boom")
	shutdownWithInfo := fmt.Errorf("%w: reconfigured", proc.Shutdown)
	for _, tt := range []struct {
		restart Restart
		reason  error
		want    bool
	}{
		{Permanent, proc.Normal, true},
		{Permanent, boom, true},
		{Transient, proc.Normal, false},
		{Transient, proc.Shutdown, false},
		{Transient, shutdownWithInfo, false},
		{Transient, proc.Killed, true},
		{Transient, boom, true},
		{Temporary, boom, false},
		{Temporary, proc.Normal, false},
	} {
		if got := shouldRestart(tt.restart, tt.reason); got != tt.want {
			t.Errorf("shouldRestart(%v, %v) = %v", tt.restart, tt.reason, got)
		}
	}
}

func TestIntensity(t *testing.T) {
	t0 := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }

	for _, tt := range []struct {
		name  string
		times []float64 // restarts, the last of which is checked
		ok    bool
	}{
		{"first", []float64{0}, true},
		{"at the limit", []float64{0, 1, 2}, true},
		{"over the limit", []float64{0, 1, 2, 3}, false},
		{"old ones expire", []float64{0, 1, 2, 5.5}, true},
		{"expiry is exclusive of the boundary", []float64{0, 1, 2, 5}, true},
		{"just inside the period", []float64{0, 1, 2, 4.9}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := intensity{max: 3, period: 5 * time.Second}
			var ok bool
			for _, s := range tt.times {
				in, ok = in.add(at(s))
			}
			if ok != tt.ok {
				t.Errorf("ok = %v, want %v (restarts %v)", ok, tt.ok, in.restarts)
			}
		})
	}
}

func TestIntensityIsPure(t *testing.T) {
	in := intensity{max: 1, period: time.Second, restarts: []time.Time{{}}}
	before := cloneTimes(in.restarts)
	in.add(time.Unix(100, 0))
	if !reflect.DeepEqual(in.restarts, before) {
		t.Error("add modified its receiver")
	}
}

func cloneTimes(ts []time.Time) []time.Time { return append([]time.Time(nil), ts...) }

func TestPlan(t *testing.T) {
	for _, tt := range []struct {
		strategy    Strategy
		failed      int
		stop, start []int
	}{
		{OneForOne, 1, nil, []int{1}},
		{OneForAll, 1, []int{3, 2, 0}, []int{0, 1, 2, 3}},
		{OneForAll, 0, []int{3, 2, 1}, []int{0, 1, 2, 3}},
		{RestForOne, 1, []int{3, 2}, []int{1, 2, 3}},
		{RestForOne, 3, nil, []int{3}},
	} {
		stop, start := plan(tt.strategy, tt.failed, 4)
		if !reflect.DeepEqual(stop, tt.stop) || !reflect.DeepEqual(start, tt.start) {
			t.Errorf("plan(%v, %d, 4) = stop %v start %v; want stop %v start %v",
				tt.strategy, tt.failed, stop, start, tt.stop, tt.start)
		}
	}
}
