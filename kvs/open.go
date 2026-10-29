package kvs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

// Open opens the table of the directory dir, made if missing: it reads the
// log into memory, and writes go on in it. The directory is the table's
// alone, until Close.
//
// A write cut short at the end of the log, by a crash, is dropped; damage
// elsewhere fails Open with ErrCorrupt.
func Open[K comparable, V any](dir string, opts Options[K, V]) (*Table[K, V], error) {
	opts, err := opts.resolved()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	t := newTable(dir, opts, lock)
	if err := t.load(); err != nil {
		unlockDir(lock)
		return nil, err
	}
	return t, nil
}

// resolved returns the options with the defaults for what is unset, and
// the order of K for an OrderedSet without Compare.
func (o Options[K, V]) resolved() (Options[K, V], error) {
	return o.withDefaults().withOrder()
}

func (o Options[K, V]) withDefaults() Options[K, V] {
	if o.Shards <= 0 {
		o.Shards = 64
	}
	if o.SegmentSize <= 0 {
		o.SegmentSize = 64 << 20
	}
	if o.CompactRatio == 0 {
		o.CompactRatio = 2
	}
	if o.CompactMin <= 0 {
		o.CompactMin = 16 << 20
	}
	return o
}

// withOrder checks Compare against Type, and finds the order of K for an
// OrderedSet without one.
func (o Options[K, V]) withOrder() (Options[K, V], error) {
	switch o.Type {
	case Set:
		if o.Compare != nil {
			return o, errors.New("kvs: Compare of a Set")
		}
	case OrderedSet:
		if o.Compare != nil {
			break
		}
		var ok bool
		if o.Compare, ok = compareOf[K](); !ok {
			return o, fmt.Errorf("kvs: no order of %v for an OrderedSet: give Compare", reflect.TypeFor[K]())
		}
	default:
		return o, fmt.Errorf("kvs: %v", o.Type)
	}
	return o, nil
}

// newTable makes the table of dir, empty, holding lock.
func newTable[K comparable, V any](dir string, opts Options[K, V], lock *os.File) *Table[K, V] {
	t := &Table[K, V]{dir: dir, opts: opts, lock: lock, kc: opts.KeyCodec, vc: opts.ValueCodec}
	if t.kc == nil {
		t.kc = DefaultCodec[K]()
	}
	if t.vc == nil {
		t.vc = DefaultCodec[V]()
	}
	t.idx = newIndex[K, V](opts.ReadConcurrency, opts.WriteConcurrency, opts.Shards, opts.Compare, &t.live)
	return t
}

// load reads the log into the table, and starts the log, for the writes to
// go on in it.
func (t *Table[K, V]) load() error {
	r, err := t.recover()
	if err != nil {
		return err
	}
	if t.log, err = startWAL(t.dir, r.seq, r.disk, r.segs, t.opts.Sync, t.opts.SegmentSize); err != nil {
		return err
	}
	if t.opts.Expires != nil {
		t.exp = newExpiries(t)
	}
	return nil
}

// recovered is what recover found in the log.
type recovered struct {
	seq  uint64 // of the last write
	disk int64  // the bytes of the files kept
	segs int    // the segments kept

	cuts  []cut    // the segments to cut to their records
	stale []string // the files of the log not needed
}

// A cut is a segment of name to cut to n bytes, and sync.
type cut struct {
	name string
	n    int64
}

// recover reads the latest snapshot and the segments of the log after it
// into the index. It cuts the segments to their records, and removes the
// files of the log it does not need: those left by a compaction cut
// short, the snapshots before the latest, and the segments it holds. It
// does so once it read them all: a read failing changes nothing.
func (t *Table[K, V]) recover() (recovered, error) {
	segs, snaps, err := listLog(t.dir)
	if err != nil {
		return recovered{}, err
	}
	r := recovered{stale: t.tempFiles()}
	if len(snaps) > 0 {
		if err := t.loadSnapshot(snaps, &r); err != nil {
			return r, err
		}
	}
	base := r.seq // the last write in the snapshot, 0 without
	for i, s := range segs {
		isLast := i == len(segs)-1
		// The next segment beginning by the first write after the snapshot,
		// this one has none.
		inSnapshot := !isLast && segs[i+1].seq <= base+1
		if inSnapshot {
			r.stale = append(r.stale, s.name)
			continue
		}
		if err := t.loadSegment(s, base, isLast, &r); err != nil {
			return r, err
		}
	}
	return r, t.tidy(r)
}

// tempFiles returns the files a compaction cut short left, if any.
func (t *Table[K, V]) tempFiles() []string {
	ents, err := os.ReadDir(t.dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".tmp") {
			names = append(names, e.Name())
		}
	}
	return names
}

// tidy cuts the segments of r to their records, and removes its files not
// needed.
func (t *Table[K, V]) tidy(r recovered) error {
	for _, c := range r.cuts {
		if err := truncate(filepath.Join(t.dir, c.name), c.n); err != nil {
			return err
		}
	}
	for _, name := range r.stale {
		remove(filepath.Join(t.dir, name))
	}
	return nil
}

// loadSnapshot loads the latest of snaps; the others are stale. A
// snapshot is written whole before it is in place: one cut short is
// damage.
func (t *Table[K, V]) loadSnapshot(snaps []logFile, r *recovered) error {
	latest := snaps[len(snaps)-1]
	n, _, err := t.readFile(latest.name, 0)
	if err == errTorn {
		return fmt.Errorf("%w: snapshot %s at %d", ErrCorrupt, latest.name, n)
	}
	if err != nil {
		return err
	}
	r.seq, r.disk = latest.seq, n
	for _, old := range snaps[:len(snaps)-1] {
		r.stale = append(r.stale, old.name)
	}
	return nil
}

// loadSegment loads the writes of s after base. A write cut short, by a
// crash, ends the last segment, and is to be cut off it; in another, it
// is damage. A segment left without a write after base is stale.
func (t *Table[K, V]) loadSegment(s logFile, base uint64, isLast bool, r *recovered) error {
	n, last, err := t.readFile(s.name, base)
	torn := err == errTorn
	if torn && !isLast {
		return fmt.Errorf("%w: segment %s at %d", ErrCorrupt, s.name, n)
	}
	if err != nil && !torn {
		return err
	}
	r.seq = max(r.seq, last)
	if last == 0 { // empty, or all in the snapshot: the last segment too
		r.stale = append(r.stale, s.name)
		return nil
	}
	// Cut what follows the records: zeros, or a write cut short; and make
	// the records durable, read maybe from the page cache only.
	r.cuts = append(r.cuts, cut{s.name, n})
	r.disk += n
	r.segs++
	return nil
}

// readFile loads the records of the log file name after the seq base. It
// returns the offset after the last whole one, and the last seq it read,
// 0 if none.
func (t *Table[K, V]) readFile(name string, base uint64) (n int64, last uint64, err error) {
	f, err := openRead(filepath.Join(t.dir, name))
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	n, err = readRecords(f, func(seq uint64, op byte, kb, vb []byte, size int64, _ uint64) error {
		if seq <= base {
			return nil
		}
		last = seq
		return t.loadRecord(name, seq, op, kb, vb, size)
	})
	return n, last, err
}

// openRead opens the file at path for reading. A variable, for a test to
// fail the reads.
var openRead = func(path string) (io.ReadCloser, error) { return os.Open(path) }

// loadRecord sets or deletes the entry of a record of the file name.
func (t *Table[K, V]) loadRecord(name string, seq uint64, op byte, kb, vb []byte, size int64) error {
	k, err := t.kc.Decode(kb)
	if err != nil {
		return fmt.Errorf("kvs: decoding a key of %s: %w", name, err)
	}
	if op == opDel {
		t.idx.unload(k)
		return nil
	}
	v, err := t.vc.Decode(vb)
	if err != nil {
		return fmt.Errorf("kvs: decoding a value of %s: %w", name, err)
	}
	t.idx.load(k, v, size, seq)
	return nil
}

// truncate cuts the file at path to n bytes, if longer, and syncs it, cut
// or not: a process dying before a sync leaves its writes in the page
// cache only, where opening reads them, and the writes after are made
// durable after them.
func truncate(path string, n int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() != n {
		if err := truncateFile(f, n); err != nil {
			return err
		}
	}
	return syncFile(f, false)
}
