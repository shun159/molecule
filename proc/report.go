package proc

import (
	"errors"
	"log/slog"
	"reflect"
	"runtime"
)

// Option configures a Node.
type Option func(*Node)

// WithLogger makes the node log its reports to l rather than to
// slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(n *Node) { n.logger = l }
}

// Logger returns the logger for reports about the node's processes:
// crash reports here, and those of behaviours and supervisors.
func (n *Node) Logger() *slog.Logger {
	if n.logger != nil {
		return n.logger
	}
	return slog.Default()
}

// IsAbnormal reports whether a process exiting with reason failed:
// whether reason is neither Normal nor Shutdown, wrapped or not. Abnormal
// exits are the ones reported, as in Erlang.
func IsAbnormal(reason error) bool {
	return !errors.Is(reason, Normal) && !errors.Is(reason, Shutdown)
}

// crashReport logs that fn, the function of p, ended with an abnormal
// reason, like the crash reports of proc_lib.
func (p *process) crashReport(fn func(*Self) error, reason error) {
	n := p.node
	n.regMu.Lock()
	name := p.name
	n.regMu.Unlock()

	attrs := []any{
		slog.String("pid", p.pid.String()),
		slog.String("initial_call", funcName(fn)),
		slog.String("reason", reason.Error()),
		slog.Int("message_queue_len", p.mbox.len()),
	}
	if name != "" {
		attrs = append(attrs, slog.String("registered_name", name))
	}
	if !p.parent.IsZero() {
		attrs = append(attrs, slog.String("parent", p.parent.String()))
	}
	var pe *PanicError
	if errors.As(reason, &pe) {
		attrs = append(attrs, slog.String("stack", string(pe.Stack)))
	}
	n.Logger().Error("crash report", attrs...)
}

func funcName(fn any) string {
	if f := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()); f != nil {
		return f.Name()
	}
	return "unknown"
}
