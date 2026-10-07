package gen

import (
	"context"
	"errors"
	"testing"
)

// TestAwaitReplyReplyWins hands awaitReply a reply together with the death
// of the server, as when the caller is slow to be scheduled after a server
// replied and crashed. The reply must win.
func TestAwaitReplyReplyWins(t *testing.T) {
	for range 100 {
		replies := make(chan any, 1)
		replies <- "bye"
		down, cancel := context.WithCancelCause(context.Background())
		cancel(errors.New("boom"))
		v, err := awaitReply(context.Background(), context.Background(), Local("srv"), replies, down)
		if v != "bye" || err != nil {
			t.Fatalf("awaitReply = %v, %v; want the reply", v, err)
		}
	}
}
