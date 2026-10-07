package gentcp

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

func TestNextReadSize(t *testing.T) {
	for _, tt := range []struct{ size, n, want int }{
		{minReadBuffer, minReadBuffer, 2 * minReadBuffer}, // full: grow
		{maxReadBuffer, maxReadBuffer, maxReadBuffer},     // full at the top: stay
		{8 << 10, 4 << 10, 8 << 10},                       // half: stay
		{8 << 10, 1 << 10, 4 << 10},                       // under a quarter: shrink
		{minReadBuffer, 0, minReadBuffer},                 // at the bottom: stay
		{16 << 10, 16<<10 - 1, 16 << 10},                  // nearly full: stay
	} {
		if got := nextReadSize(tt.size, tt.n); got != tt.want {
			t.Errorf("nextReadSize(%d, %d) = %d, want %d", tt.size, tt.n, got, tt.want)
		}
	}
}

// TestHandsOverFullReads reads chunks filling the buffer, which are handed
// over without a copy: the next read must go to a new buffer.
func TestHandsOverFullReads(t *testing.T) {
	n := proc.NewNode("")
	got := make(chan DataMsg, 2)
	owner := n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			if m, ok := msg.(DataMsg); ok {
				got <- m
			}
		}
	})
	server, peer := net.Pipe()
	defer peer.Close()
	Start(n, server, owner, Options{Active: N(2)})
	chunks := [][]byte{bytes.Repeat([]byte("a"), minReadBuffer), bytes.Repeat([]byte("b"), minReadBuffer)}
	go func() {
		for _, c := range chunks {
			peer.Write(c)
		}
	}()
	var recvd [][]byte
	for range chunks {
		select {
		case m := <-got:
			recvd = append(recvd, m.Bytes)
		case <-time.After(5 * time.Second):
			t.Fatal("nothing delivered")
		}
	}
	if !bytes.Equal(recvd[0], chunks[0]) || !bytes.Equal(recvd[1], chunks[1]) {
		t.Errorf("first read now %q..., second %q...", recvd[0][:4], recvd[1][:4])
	}
}

// TestSendActiveFailed fails the send of a passive socket: the mode still
// changes, so that the owner hears of the failure.
func TestSendActiveFailed(t *testing.T) {
	n := proc.NewNode("")
	got := make(chan any, 4)
	owner := n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			got <- msg
		}
	})
	server, peer := net.Pipe()
	defer peer.Close()
	// The write deadline has passed before the write starts.
	sock := Start(n, server, owner, Options{SendTimeout: time.Nanosecond})
	n.Send(sock.PID, sock.SendActiveEffect([]byte("x"), Once).(gen.Send).Msg)
	for _, want := range []string{"ErrorMsg", "ClosedMsg"} {
		select {
		case m := <-got:
			switch m := m.(type) {
			case ErrorMsg:
				if want != "ErrorMsg" || !errors.Is(m.Err, os.ErrDeadlineExceeded) {
					t.Errorf("got %#v, want %s", m, want)
				}
			case ClosedMsg:
				if want != "ClosedMsg" {
					t.Errorf("got %#v, want %s", m, want)
				}
			default:
				t.Errorf("got %#v, want %s", m, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no %s", want)
		}
	}
}
