package kvs_test

import (
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/shun159/molecule/kvs"
)

// modes are the four ways a table bears concurrency.
var modes = []struct {
	name        string
	read, write bool
}{
	{"plain", false, false},
	{"read", true, false},
	{"write", false, true},
	{"readwrite", true, true},
}

func open[K comparable, V any](t testing.TB, dir string, o kvs.Options[K, V]) *kvs.Table[K, V] {
	t.Helper()
	tab, err := kvs.Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	return tab
}

func contents[K comparable, V any](tab *kvs.Table[K, V]) map[K]V {
	return maps.Collect(tab.All())
}

func TestPutGetDelete(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			dir := t.TempDir()
			o := kvs.Options[string, int]{ReadConcurrency: m.read, WriteConcurrency: m.write, Shards: 4}
			tab := open(t, dir, o)
			want := map[string]int{}
			for i := range 1000 {
				k := fmt.Sprint("k", i%300)
				if i%7 == 0 {
					if err := tab.Delete(k); err != nil {
						t.Fatal(err)
					}
					delete(want, k)
					continue
				}
				if err := tab.Put(k, i); err != nil {
					t.Fatal(err)
				}
				want[k] = i
			}
			if err := tab.Delete("absent"); err != nil {
				t.Fatal(err)
			}
			for k, v := range want {
				if got, ok := tab.Get(k); !ok || got != v {
					t.Fatalf("Get(%s) = %d %v, want %d", k, got, ok, v)
				}
			}
			if _, ok := tab.Get("absent"); ok {
				t.Fatal("Get(absent) found")
			}
			if n := tab.Len(); n != len(want) {
				t.Fatalf("Len = %d, want %d", n, len(want))
			}
			if got := contents(tab); !maps.Equal(got, want) {
				t.Fatalf("All = %v, want %v", got, want)
			}
			info := tab.Info()
			if info.Size != len(want) || info.LastSeq != info.DurableSeq || info.LastSeq == 0 {
				t.Fatalf("Info = %+v", info)
			}
			if err := tab.Close(); err != nil {
				t.Fatal(err)
			}
			if err := tab.Put("x", 1); !errors.Is(err, kvs.ErrClosed) {
				t.Fatalf("Put after Close: %v", err)
			}

			tab = open(t, dir, o)
			defer tab.Close()
			if got := contents(tab); !maps.Equal(got, want) {
				t.Fatalf("reopened: %v, want %v", got, want)
			}
			if got := tab.Info().LastSeq; got != info.LastSeq {
				t.Fatalf("reopened LastSeq = %d, want %d", got, info.LastSeq)
			}
		})
	}
}

func TestLocked(t *testing.T) {
	dir := t.TempDir()
	tab := open(t, dir, kvs.Options[string, string]{})
	if _, err := kvs.Open(dir, kvs.Options[string, string]{}); !errors.Is(err, kvs.ErrLocked) {
		t.Fatalf("second Open: %v", err)
	}
	tab.Close()
	open(t, dir, kvs.Options[string, string]{}).Close()
}

// segments returns the paths of the segments of dir.
func segments(t *testing.T, dir string) []string {
	t.Helper()
	segs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		t.Fatal(err)
	}
	return segs
}

func TestTornTail(t *testing.T) {
	for _, cut := range []struct {
		name string
		do   func(path string, size int64) error
	}{
		{"garbage", func(path string, _ int64) error {
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = f.Write([]byte{9, 0, 0, 0, 1, 2, 3})
			return err
		}},
		{"short", func(path string, size int64) error { return os.Truncate(path, size-3) }},
		{"flipped", func(path string, size int64) error {
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = f.WriteAt([]byte{0xff}, size-1)
			return err
		}},
	} {
		t.Run(cut.name, func(t *testing.T) {
			dir := t.TempDir()
			tab := open(t, dir, kvs.Options[string, string]{})
			tab.Put("a", "1")
			tab.Put("b", "2")
			tab.Close()
			segs := segments(t, dir)
			last := segs[len(segs)-1]
			fi, _ := os.Stat(last)
			if err := cut.do(last, fi.Size()); err != nil {
				t.Fatal(err)
			}

			tab = open(t, dir, kvs.Options[string, string]{})
			want := map[string]string{"a": "1", "b": "2"}
			if cut.name != "garbage" {
				delete(want, "b") // the last record is damaged
			}
			if got := contents(tab); !maps.Equal(got, want) {
				t.Fatalf("got %v, want %v", got, want)
			}
			tab.Put("c", "3")
			tab.Close()
			tab = open(t, dir, kvs.Options[string, string]{})
			defer tab.Close()
			want["c"] = "3"
			if got := contents(tab); !maps.Equal(got, want) {
				t.Fatalf("after writing on: got %v, want %v", got, want)
			}
		})
	}
}

// TestZeroTail opens a log whose segments end in zeros, as a crash leaves
// them before they are cut to their records.
func TestZeroTail(t *testing.T) {
	dir := t.TempDir()
	tab := open(t, dir, kvs.Options[string, string]{})
	tab.Put("a", "1")
	tab.Close()
	tab = open(t, dir, kvs.Options[string, string]{})
	tab.Put("b", "2")
	tab.Close()
	for _, seg := range segments(t, dir) {
		f, _ := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0)
		f.Write(make([]byte, 4096))
		f.Close()
	}
	tab = open(t, dir, kvs.Options[string, string]{})
	defer tab.Close()
	if got, want := contents(tab), map[string]string{"a": "1", "b": "2"}; !maps.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, seg := range segments(t, dir) {
		if fi, _ := os.Stat(seg); fi.Size()%4096 == 0 && fi.Size() > 0 {
			t.Fatalf("%s not cut: %d bytes", seg, fi.Size())
		}
	}
}

func TestCorrupt(t *testing.T) {
	dir := t.TempDir()
	tab := open(t, dir, kvs.Options[string, string]{})
	tab.Put("a", "1")
	tab.Close()
	tab = open(t, dir, kvs.Options[string, string]{})
	tab.Put("b", "2")
	tab.Close()
	segs := segments(t, dir)
	if len(segs) != 2 {
		t.Fatalf("segments %v", segs)
	}
	f, _ := os.OpenFile(segs[0], os.O_WRONLY, 0)
	f.WriteAt([]byte{0xff}, 9)
	f.Close()
	if _, err := kvs.Open(dir, kvs.Options[string, string]{}); !errors.Is(err, kvs.ErrCorrupt) {
		t.Fatalf("Open: %v", err)
	}
}

func TestCompact(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			dir := t.TempDir()
			o := kvs.Options[int, string]{
				ReadConcurrency: m.read, WriteConcurrency: m.write, Shards: 4,
				Sync: kvs.SyncNone, SegmentSize: 4 << 10, CompactRatio: -1,
			}
			tab := open(t, dir, o)
			want := map[int]string{}
			for i := range 5000 {
				v := fmt.Sprint("value-", i)
				tab.Put(i%100, v)
				want[i%100] = v
			}
			before := tab.Info()
			if err := tab.Compact(); err != nil {
				t.Fatal(err)
			}
			after := tab.Info()
			if after.Segments != 1 || after.DiskBytes >= before.DiskBytes || after.Compactions != 1 {
				t.Fatalf("before %+v\nafter %+v", before, after)
			}
			if after.DiskBytes != after.LiveBytes {
				t.Fatalf("after compaction, DiskBytes %d, LiveBytes %d", after.DiskBytes, after.LiveBytes)
			}
			tab.Put(1000, "more")
			want[1000] = "more"
			tab.Delete(0)
			delete(want, 0)
			tab.Close()

			tab = open(t, dir, o)
			defer tab.Close()
			if got := contents(tab); !maps.Equal(got, want) {
				t.Fatalf("reopened: %v, want %v", got, want)
			}
		})
	}
}

// TestCompactWhileWriting compacts again and again while writers write,
// each to keys of its own, and checks that the table opened again holds
// the last write to each key.
func TestCompactWhileWriting(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			dir := t.TempDir()
			o := kvs.Options[int, int]{
				ReadConcurrency: m.read, WriteConcurrency: m.write, Shards: 4,
				Sync: kvs.SyncNone, SegmentSize: 8 << 10, CompactRatio: 1.5, CompactMin: 16 << 10,
			}
			tab := open(t, dir, o)
			const writers, keys, rounds = 4, 50, 400
			var wg sync.WaitGroup
			for w := range writers {
				wg.Go(func() {
					for i := range rounds {
						k := w*keys + i%keys
						if i%11 == 0 {
							tab.Delete(k)
						} else {
							tab.Put(k, i)
						}
					}
				})
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				for range 20 {
					if err := tab.Compact(); err != nil {
						t.Error(err)
					}
				}
			}()
			wg.Wait()
			<-done
			want := map[int]int{}
			for w := range writers {
				for i := range rounds {
					k := w*keys + i%keys
					if i%11 == 0 {
						delete(want, k)
					} else {
						want[k] = i
					}
				}
			}
			if got := contents(tab); !maps.Equal(got, want) {
				t.Fatalf("in memory: %v, want %v", got, want)
			}
			if err := tab.Info().CompactionErr; err != nil {
				t.Fatal(err)
			}
			tab.Close()
			tab = open(t, dir, o)
			defer tab.Close()
			if got := contents(tab); !maps.Equal(got, want) {
				t.Fatalf("reopened: %v, want %v", got, want)
			}
		})
	}
}

func TestSelect(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			tab := open(t, t.TempDir(), kvs.Options[int, int]{ReadConcurrency: m.read, WriteConcurrency: m.write, Sync: kvs.SyncNone})
			defer tab.Close()
			for i := range 100 {
				tab.Put(i, i*i)
			}
			n := 0
			for k, v := range tab.Select(func(k, v int) bool { return k%2 == 0 }) {
				if k%2 != 0 || v != k*k {
					t.Fatalf("selected %d=%d", k, v)
				}
				tab.Delete(k) // writing during the range
				n++
			}
			if n != 50 || tab.Len() != 50 {
				t.Fatalf("selected %d, %d left", n, tab.Len())
			}
		})
	}
}

func TestSyncEvery(t *testing.T) {
	tab := open(t, t.TempDir(), kvs.Options[string, string]{Sync: kvs.SyncEvery(5 * time.Millisecond)})
	defer tab.Close()
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			if err := tab.Put(fmt.Sprint(i), "v"); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	info := tab.Info()
	if info.DurableSeq != 20 || info.Syncs == 0 || info.Syncs > 20 {
		t.Fatalf("Info = %+v", info)
	}
}

type lease struct {
	MAC     string
	Expires time.Time
	Tags    []string
}

func TestCodecs(t *testing.T) {
	dir := t.TempDir()
	addr := netip.MustParseAddr("192.0.2.10")
	l := lease{MAC: "aa:bb", Expires: time.Unix(1700000000, 0).UTC(), Tags: []string{"x"}}
	tab := open(t, dir, kvs.Options[netip.Addr, lease]{})
	tab.Put(addr, l)
	tab.Close()
	tab = open(t, dir, kvs.Options[netip.Addr, lease]{})
	defer tab.Close()
	got, ok := tab.Get(addr)
	if !ok || got.MAC != l.MAC || !got.Expires.Equal(l.Expires) || len(got.Tags) != 1 {
		t.Fatalf("got %+v %v", got, ok)
	}

	b := kvs.DefaultCodec[[]byte]()
	buf := []byte("abc")
	enc, _ := b.Append(nil, buf)
	dec, _ := b.Decode(enc)
	enc[0] = 'x'
	if string(dec) != "abc" {
		t.Fatal("decoded bytes share the buffer")
	}
	u8 := kvs.DefaultCodec[uint8]()
	big, _ := kvs.DefaultCodec[uint64]().Append(nil, 300)
	if _, err := u8.Decode(big); err == nil {
		t.Fatal("300 decoded as a uint8")
	}
}

// TestKilled has a child process put with SyncNone and kill itself,
// without closing the table: what Put returned for is in the log.
func TestKilled(t *testing.T) {
	if dir := os.Getenv("KVS_KILLED_DIR"); dir != "" {
		tab, err := kvs.Open(dir, kvs.Options[int, int]{Sync: kvs.SyncNone})
		if err != nil {
			os.Exit(2)
		}
		for i := range 10000 {
			if tab.Put(i, i) != nil {
				os.Exit(3)
			}
		}
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run", "^TestKilled$")
	cmd.Env = append(os.Environ(), "KVS_KILLED_DIR="+dir)
	err := cmd.Run()
	if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("child: %v", err)
	}
	tab := open(t, dir, kvs.Options[int, int]{})
	defer tab.Close()
	if n := tab.Len(); n != 10000 {
		t.Fatalf("%d entries after the kill, want 10000", n)
	}
}
