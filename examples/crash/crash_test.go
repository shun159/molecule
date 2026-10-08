package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/shun159/molecule/internal/testlog"
)

func TestScenario(t *testing.T) {
	rec, logger := testlog.New()
	var out bytes.Buffer
	if err := run(&out, logger); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	want := []string{
		"value: 42",
		"crash 1: call failed: molecule: call to calc: panic: runtime error: integer divide by zero",
		"crash 1: restarted, value back to 0",
		"crash 2: call failed: molecule: call to calc: panic: runtime error: integer divide by zero",
		"crash 2: restarted, value back to 0",
		"crash 3: call failed: molecule: call to calc: panic: runtime error: integer divide by zero",
		"crash 3: restarted, value back to 0",
		"crash 4: supervisor gave up: supervisor: reached maximum restart intensity: shutdown",
		"calc is gone: molecule: call to calc: noproc",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Errorf("output:\n%s\nwant:\n%s", out.String(), strings.Join(want, "\n"))
	}

	for msg, count := range map[string]int{
		"behaviour terminating":       4,
		"crash report":                4,
		"supervisor child terminated": 4,
		"supervisor child started":    4, // the first start and 3 restarts
		"supervisor shutting down":    1,
	} {
		if got := len(rec.Records(msg)); got != count {
			t.Errorf("%d %q reports, want %d", got, msg, count)
		}
	}
	for _, r := range rec.Records("crash report") {
		if r.Attrs["registered_name"] != "calc" || !strings.Contains(r.Attrs["stack"], "CalcServer.HandleCall") {
			t.Errorf("crash report = %v", r.Attrs)
		}
	}
}

func TestShortStack(t *testing.T) {
	stack := "goroutine 7 [running]:\nruntime/debug.Stack()\n\t/usr/lib/go/src/runtime/debug/stack.go:26\n" +
		"main.CalcServer.HandleCall(...)\n\t/x/molecule/examples/crash/calc_server.go:40 +0x1c\n" +
		"github.com/shun159/molecule/gen.(*runtime).handle()\n\t/x/molecule/gen/loop.go:146\n"
	got := shortStack(nil, slog.String("stack", stack)).Value.String()
	if want := "main.CalcServer.HandleCall(...) at calc_server.go:40"; got != want {
		t.Errorf("shortStack = %q, want %q", got, want)
	}
}
