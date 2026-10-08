package supervisor

import (
	"context"
	"log/slog"

	"github.com/shun159/molecule/proc"
)

// Supervisor reports, as in OTP: errors when a child terminates
// abnormally, fails to start, or the supervisor gives up; progress, at
// debug level, when a child starts.

func report(self *proc.Self, level slog.Level, msg string, attrs ...any) {
	attrs = append([]any{slog.String("supervisor", self.PID().String())}, attrs...)
	self.Node().Logger().Log(context.Background(), level, msg, attrs...)
}

func childAttrs(id string, pid proc.PID) []any {
	attrs := []any{slog.String("child_pid", pid.String())}
	if id != "" {
		attrs = append(attrs, slog.String("child_id", id))
	}
	return attrs
}

func reportStarted(self *proc.Self, id string, pid proc.PID) {
	report(self, slog.LevelDebug, "supervisor child started", childAttrs(id, pid)...)
}

func reportTerminated(self *proc.Self, id string, pid proc.PID, r Restart, reason error) {
	if !proc.IsAbnormal(reason) {
		return
	}
	attrs := append(childAttrs(id, pid), slog.String("restart", r.String()), slog.String("reason", reason.Error()))
	report(self, slog.LevelError, "supervisor child terminated", attrs...)
}

func reportStartFailed(self *proc.Self, id string, reason error) {
	attrs := []any{slog.String("reason", reason.Error())}
	if id != "" {
		attrs = append(attrs, slog.String("child_id", id))
	}
	report(self, slog.LevelError, "supervisor child start failed", attrs...)
}

func reportShutdown(self *proc.Self, reason error) {
	report(self, slog.LevelError, "supervisor shutting down", slog.String("reason", reason.Error()))
}
