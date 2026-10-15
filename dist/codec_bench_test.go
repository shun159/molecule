package dist

import (
	"testing"

	"github.com/shun159/molecule/proc"
)

// The messages of a Raft group, as quorum sends them: an entry carrying a
// command of the user's, in an interface, and the reply.
type (
	benchEntry struct {
		Term, Index uint64
		Cmd         any
	}
	benchAppend struct {
		Term            uint64
		Leader, To      proc.PID
		PrevIndex, Prev uint64
		Entries         []benchEntry
		Commit          uint64
	}
	benchReply struct {
		Term    uint64
		From    proc.PID
		Success bool
		Match   uint64
	}
)

func init() { Register(benchAppend{}, benchReply{}) }

// BenchmarkCodec encodes and decodes an append of one entry and its reply,
// on one stream, as a connection does.
func BenchmarkCodec(b *testing.B) {
	n := proc.NewNode("a@localhost")
	msgs := []any{
		benchAppend{Term: 3, Leader: n.NewPID(), To: n.NewPID(), PrevIndex: 41, Prev: 3, Entries: []benchEntry{{Term: 3, Index: 42, Cmd: 1}}, Commit: 40},
		benchReply{Term: 3, From: n.NewPID(), Success: true, Match: 42},
	}
	enc, dec := gobCodec{}.NewEncoder(), gobCodec{}.NewDecoder()
	b.ReportAllocs()
	for b.Loop() {
		for _, m := range msgs {
			p, err := enc.Encode(m)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := dec.Decode(p); err != nil {
				b.Fatal(err)
			}
		}
	}
}
