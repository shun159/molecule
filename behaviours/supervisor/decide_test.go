package supervisor

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

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
