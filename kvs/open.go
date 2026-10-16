package kvs

import (
	"errors"
	"fmt"
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
	if opts.Shards <= 0 {
		opts.Shards = 64
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 64 << 20
	}
	if opts.CompactRatio == 0 {
		opts.CompactRatio = 2
	}
	if opts.CompactMin <= 0 {
		opts.CompactMin = 16 << 20
	}
	switch {
	case opts.Type == Set && opts.Compare != nil:
		return nil, errors.New("kvs: Compare of a Set")
	case opts.Type == OrderedSet && opts.Compare == nil:
		var ok bool
		if opts.Compare, ok = compareOf[K](); !ok {
			return nil, fmt.Errorf("kvs: no order of %v for an OrderedSet: give Compare", reflect.TypeFor[K]())
		}
	case opts.Type != Set && opts.Type != OrderedSet:
		return nil, fmt.Errorf("kvs: %v", opts.Type)
	}
	t := &Table[K, V]{dir: dir, opts: opts, kc: opts.KeyCodec, vc: opts.ValueCodec}
	if t.kc == nil {
		t.kc = DefaultCodec[K]()
	}
	if t.vc == nil {
		t.vc = DefaultCodec[V]()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	t.lock = lock
	t.idx = newIndex[K, V](opts.ReadConcurrency, opts.WriteConcurrency, opts.Shards, opts.Compare, &t.live)
	seq, disk, segs, err := t.recover()
	if err == nil {
		t.log, err = startWAL(dir, seq, disk, segs, opts.Sync, opts.SegmentSize)
	}
	if err != nil {
		unlockDir(lock)
		return nil, err
	}
	if opts.Expires != nil {
		t.exp = newExpiries(t)
	}
	return t, nil
}

// recover reads the snapshot and the segments of the log into the index,
// and returns the last seq, the bytes on disk and the number of segments.
func (t *Table[K, V]) recover() (seq uint64, disk int64, nsegs int, err error) {
	if ents, err := os.ReadDir(t.dir); err == nil {
		for _, e := range ents {
			if strings.HasSuffix(e.Name(), ".tmp") {
				os.Remove(filepath.Join(t.dir, e.Name()))
			}
		}
	}
	segs, snaps, err := listLog(t.dir)
	if err != nil {
		return 0, 0, 0, err
	}
	var base uint64
	if len(snaps) > 0 {
		s := snaps[len(snaps)-1]
		base = s.seq
		n, err := t.readFile(s.name, 0)
		if err == errTorn {
			return 0, 0, 0, fmt.Errorf("%w: snapshot %s at %d", ErrCorrupt, s.name, n)
		}
		if err != nil {
			return 0, 0, 0, err
		}
		disk += n
		for _, old := range snaps[:len(snaps)-1] {
			os.Remove(filepath.Join(t.dir, old.name))
		}
	}
	seq = base
	for i, s := range segs {
		path := filepath.Join(t.dir, s.name)
		if i+1 < len(segs) && segs[i+1].seq <= base+1 {
			os.Remove(path) // all in the snapshot
			continue
		}
		var last uint64
		n, err := t.readFile(s.name, base, &last)
		switch {
		case err == nil || err == errTorn && i == len(segs)-1:
			// Cut what follows the records: zeros, or a write cut short.
			if err := truncate(path, n); err != nil {
				return 0, 0, 0, err
			}
		case err == errTorn:
			return 0, 0, 0, fmt.Errorf("%w: segment %s at %d", ErrCorrupt, s.name, n)
		case err != nil:
			return 0, 0, 0, err
		}
		seq = max(seq, last)
		if last == 0 { // empty, or all in the snapshot: the last segment too
			os.Remove(path)
			continue
		}
		disk += n
		nsegs++
	}
	return seq, disk, nsegs, nil
}

// readFile loads the records of the log file name after the seq base,
// and returns the offset after the last whole one, the last seq read in
// *last, if given.
func (t *Table[K, V]) readFile(name string, base uint64, last ...*uint64) (int64, error) {
	f, err := os.Open(filepath.Join(t.dir, name))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return readRecords(f, func(seq uint64, op byte, kb, vb []byte, size int64) error {
		if seq <= base {
			return nil
		}
		if len(last) > 0 {
			*last[0] = seq
		}
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
	})
}

func truncate(path string, n int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if fi, err := f.Stat(); err != nil || fi.Size() == n {
		return err
	}
	if err := f.Truncate(n); err != nil {
		return err
	}
	return f.Sync()
}
