package dist

import (
	"reflect"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

type (
	codecEmpty  struct{}
	codecHidden struct{ n int }
)

func init() { Register(codecEmpty{}, codecHidden{}) }

// A message of a type registered goes by the ID of its type, one without
// exported fields by its ID alone; another, a call carrying one, or a
// value of a built-in type, in an envelope; all come back as they went,
// on one stream.
func TestCodec(t *testing.T) {
	n := proc.NewNode("a@localhost")
	reply := benchReply{Term: 3, From: n.NewPID(), Success: true, Match: 42}
	msgs := []any{
		reply,
		benchAppend{Term: 3, Entries: []benchEntry{{Term: 3, Index: 42, Cmd: "x"}}},
		codecEmpty{},
		codecHidden{},
		actor.CallMsg{Req: reply},
		"text",
		reply,
	}
	enc, dec := gobCodec{}.NewEncoder(), gobCodec{}.NewDecoder()
	for i, m := range msgs {
		b, err := enc.Encode(m)
		if err != nil {
			t.Fatalf("%d %T: %v", i, m, err)
		}
		_, registered := typeOf(m)
		if typedFlag := b[0]&typed != 0; typedFlag != registered {
			t.Errorf("%d %T: typed %v, registered %v", i, m, typedFlag, registered)
		}
		got, err := dec.Decode(b)
		if err != nil || !reflect.DeepEqual(got, m) {
			t.Errorf("%d: %#v, %v; want %#v", i, got, err, m)
		}
	}
}
