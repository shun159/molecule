package genudp

import (
	"context"
	"testing"
	"time"

	"github.com/shun159/molecule/proc"
)

// failRead opens a socket owned by s and closes its connection behind the
// socket's back, so that its reading fails.
func failRead(t *testing.T, s *proc.Self, opts Options) Socket {
	t.Helper()
	sock, err := Open(context.Background(), s, "127.0.0.1:0", opts)
	if err != nil {
		t.Fatal(err)
	}
	sock.conn.Close()
	return sock
}

func awaitExit(t *testing.T, s *proc.Self, sock Socket) {
	t.Helper()
	ctx, cancel := s.Node().Watch(context.Background(), sock.PID)
	defer cancel()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Error("socket process still alive")
	}
}

func inProc(t *testing.T, fn func(s *proc.Self)) {
	t.Helper()
	done := make(chan struct{})
	proc.NewNode("").Spawn(func(s *proc.Self) error {
		defer close(done)
		fn(s)
		return nil
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("test process did not finish")
	}
}

func TestReadFailsActive(t *testing.T) {
	inProc(t, func(s *proc.Self) {
		sock := failRead(t, s, Options{Active: Always})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		msg, err := s.Receive(ctx)
		if m, ok := msg.(ErrorMsg); err != nil || !ok || m.Err == nil {
			t.Fatalf("got %#v, %v, want ErrorMsg", msg, err)
		}
		if msg, err = s.Receive(ctx); err != nil {
			t.Fatal(err)
		}
		if _, ok := msg.(ClosedMsg); !ok {
			t.Fatalf("got %#v, want ClosedMsg", msg)
		}
		awaitExit(t, s, sock)
	})
}

// A passive socket reads only for a Recv, which then gets the failure.
func TestReadFailsPassive(t *testing.T) {
	inProc(t, func(s *proc.Self) {
		sock := failRead(t, s, Options{})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, _, err := sock.Recv(ctx, s); err == nil || err == ErrClosed {
			t.Fatalf("Recv = %v, want the read error", err)
		}
		awaitExit(t, s, sock)
	})
}
