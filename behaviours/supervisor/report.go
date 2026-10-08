package supervisor

import (
	"log/slog"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Supervisor reports, as in OTP: errors when a child terminates
// abnormally, fails to start, or the supervisor gives up; progress, at
// debug level, when a child starts. They are effects, as all a supervisor
// does.

func report(self proc.PID, level slog.Level, msg string, attrs ...any) []molecule.Effect {
	attrs = append([]any{slog.String("supervisor", self.String())}, attrs...)
	return molecule.Do(gen.Log{Level: level, Msg: msg, Attrs: attrs})
}

func childAttrs(id string, pid proc.PID) []any {
	attrs := []any{slog.String("child_pid", pid.String())}
	if id != "" {
		attrs = append(attrs, slog.String("child_id", id))
	}
	return attrs
}

func reportStarted(self proc.PID, id string, pid proc.PID) []molecule.Effect {
	return report(self, slog.LevelDebug, "supervisor child started", childAttrs(id, pid)...)
}

func reportTerminated(self proc.PID, id string, pid proc.PID, r Restart, reason error) []molecule.Effect {
	if !proc.IsAbnormal(reason) {
		return nil
	}
	attrs := append(childAttrs(id, pid), slog.String("restart", r.String()), slog.String("reason", reason.Error()))
	return report(self, slog.LevelError, "supervisor child terminated", attrs...)
}

func reportStartFailed(self proc.PID, id string, reason error) []molecule.Effect {
	if id == "" && reason == molecule.ErrIgnore {
		return nil
	}
	attrs := []any{slog.String("reason", reason.Error())}
	if id != "" {
		attrs = append(attrs, slog.String("child_id", id))
	}
	return report(self, slog.LevelError, "supervisor child start failed", attrs...)
}

func reportShutdown(self proc.PID, reason error) []molecule.Effect {
	return report(self, slog.LevelError, "supervisor shutting down", slog.String("reason", reason.Error()))
}
