package supervisor

import (
	"errors"

	"github.com/shun159/molecule/proc"
)

// The decisions of a supervisor are pure functions of their inputs, so
// they are tested apart from the processes they act on.

// shouldRestart reports whether a child that exited with reason is to be
// restarted.
func shouldRestart(r Restart, reason error) bool {
	switch r {
	case Permanent:
		return true
	case Transient:
		return !errors.Is(reason, proc.Normal) && !errors.Is(reason, proc.Shutdown)
	}
	return false
}

// plan returns, for a failure of the child at index failed among n, the
// children to stop, in that order, and those to start, in that order.
func plan(s Strategy, failed, n int) (stop, start []int) {
	switch s {
	case OneForAll:
		for i := n - 1; i >= 0; i-- {
			if i != failed {
				stop = append(stop, i)
			}
		}
		return stop, seq(0, n)
	case RestForOne:
		for i := n - 1; i > failed; i-- {
			stop = append(stop, i)
		}
		return stop, seq(failed, n)
	}
	return nil, []int{failed}
}

func seq(from, to int) []int {
	s := make([]int, 0, to-from)
	for i := from; i < to; i++ {
		s = append(s, i)
	}
	return s
}
