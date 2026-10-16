package gen

import (
	"fmt"

	"github.com/shun159/molecule/actor"
	"log/slog"
)

// report logs that the behaviour terminates abnormally, with the message
// it was handling and its state, like the report of a terminating
// gen_server. The crash report of the process follows.
func (r *runner[S]) report(state S, reason error) {
	st := actor.Status{State: state, Message: r.last, Reason: reason}
	if f, ok := any(r.b).(actor.StatusFormatter); ok {
		st = f.FormatStatus(st)
	}
	attrs := []any{
		slog.String("pid", r.env.Self().String()),
		slog.String("behaviour", fmt.Sprintf("%T", r.b)),
		slog.String("last_message", brief(st.Message)),
		slog.String("state", brief(st.State)),
	}
	if st.Reason != nil {
		attrs = append(attrs, slog.String("reason", st.Reason.Error()))
	}
	r.env.Logger().Error("behaviour terminating", attrs...)
}

// briefLimit bounds the size of values in reports.
const briefLimit = 1024

// brief formats v for a report, cut short if long.
func brief(v any) string {
	s := fmt.Sprintf("%+v", v)
	if len(s) > briefLimit {
		return s[:briefLimit] + "..."
	}
	return s
}
