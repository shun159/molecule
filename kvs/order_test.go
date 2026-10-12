package kvs

import (
	"net/netip"
	"testing"
	"time"
)

type mac [6]byte

type name string

// reversed orders its keys by its own Compare, over that of its kind.
type reversed int

func (r reversed) Compare(o reversed) int { return int(o) - int(r) }

func order[K comparable](t *testing.T, ks ...K) {
	t.Helper()
	c, ok := compareOf[K]()
	if !ok {
		t.Fatalf("no order of %T", ks[0])
	}
	for i := range ks {
		for j := range ks {
			want := 0
			if i < j {
				want = -1
			} else if i > j {
				want = 1
			}
			if got := c(ks[i], ks[j]); got < 0 && want >= 0 || got > 0 && want <= 0 || got == 0 && want != 0 {
				t.Fatalf("%T: Compare(%v, %v) = %d, want %d", ks[0], ks[i], ks[j], got, want)
			}
		}
	}
}

// compareOf finds the order of keys of a method Compare, and of strings,
// numbers and arrays of bytes, named or not, in increasing order here.
func TestCompareOf(t *testing.T) {
	order(t, "", "a", "ab", "b")
	order[name](t, "x", "y", "yy")
	order(t, -3, 0, 7)
	order[int8](t, -128, 0, 127)
	order[int64](t, -1<<62, -1, 1<<62)
	order[uint8](t, 0, 1, 255)
	order[uint64](t, 0, 1<<63, 1<<64-1)
	order(t, -1.5, 0, 2.25)
	order[float32](t, -1, 0.5, 1)
	order(t, mac{0, 0, 0, 0, 0, 1}, mac{0, 0, 0, 0, 1, 0}, mac{0xff, 0, 0, 0, 0, 0})
	order(t, netip.MustParseAddr("10.0.0.2"), netip.MustParseAddr("10.0.0.10"), netip.MustParseAddr("192.168.1.1"))
	t0 := time.Now()
	order(t, t0, t0.Add(time.Nanosecond), t0.Add(time.Hour))
	order[reversed](t, 3, 2, 1)

	if _, ok := compareOf[struct{ A, B int }](); ok {
		t.Fatal("an order of a struct")
	}
	if _, ok := compareOf[[2]int](); ok {
		t.Fatal("an order of an array of ints")
	}
}

func TestOpenType(t *testing.T) {
	if _, err := Open(t.TempDir(), Options[struct{ A int }, int]{Type: OrderedSet}); err == nil {
		t.Fatal("an OrderedSet of keys without an order opened")
	}
	if _, err := Open(t.TempDir(), Options[int, int]{Compare: func(a, b int) int { return a - b }}); err == nil {
		t.Fatal("a Set with Compare opened")
	}
	if _, err := Open(t.TempDir(), Options[int, int]{Type: 7}); err == nil {
		t.Fatal("a table of Type 7 opened")
	}
	// A struct key, given its order.
	type pair struct{ A, B int }
	tab, err := Open(t.TempDir(), Options[pair, int]{Type: OrderedSet, Compare: func(x, y pair) int {
		if x.A != y.A {
			return x.A - y.A
		}
		return x.B - y.B
	}, KeyCodec: Gob[pair]()})
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	tab.Put(pair{2, 1}, 0)
	tab.Put(pair{1, 9}, 0)
	tab.Put(pair{2, 0}, 0)
	var got []pair
	for k := range tab.All() {
		got = append(got, k)
	}
	if len(got) != 3 || got[0] != (pair{1, 9}) || got[1] != (pair{2, 0}) || got[2] != (pair{2, 1}) {
		t.Fatalf("All = %v", got)
	}
}
