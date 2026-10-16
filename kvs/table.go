package kvs

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Options configure a table.
type Options[K comparable, V any] struct {
	// ReadConcurrency makes reads take no lock: the entries are a
	// persistent trie, whose root each write replaces. A write costs
	// more, allocating the path to what it changes; a range over the
	// table copies nothing.
	ReadConcurrency bool
	// WriteConcurrency splits the table in Shards, each with a lock of
	// its own, so that writes to keys of different shards go at once.
	// They still append to one log, under a lock held briefly.
	WriteConcurrency bool
	// Shards is the number of shards with WriteConcurrency, rounded up
	// to a power of two: 64 if zero.
	Shards int

	// Type is Set, the default, or OrderedSet.
	Type Type
	// Compare orders the keys of an OrderedSet, as cmp.Compare does,
	// reporting 0 for equal keys only. If nil, the order of K: by its
	// method Compare(K) int, if any, as for netip.Addr and time.Time, or
	// that of its strings, numbers, or arrays of bytes.
	Compare func(a, b K) int

	// Expires tells when the entry of a value expires, if ever, with the
	// zero time: if set, Expire deletes the entries expired.
	Expires func(V) time.Time

	// Sync says when a write is durable: SyncEach if zero.
	Sync Sync
	// SegmentSize is the size past which the log goes on in a new
	// segment: 64 MiB if zero.
	SegmentSize int64
	// The log is compacted by itself when it is CompactRatio times as
	// large as the records of the entries, 2 if zero, and at least
	// CompactMin bytes, 16 MiB if zero. A negative CompactRatio leaves
	// compaction to Compact.
	CompactRatio float64
	CompactMin   int64

	// KeyCodec and ValueCodec encode the keys and the values for the
	// log: DefaultCodec if nil.
	KeyCodec   Codec[K]
	ValueCodec Codec[V]
}

// Sync says when the writes to a table are durable: on disk, or, with
// SyncNone, in the hands of the system.
type Sync struct {
	none  bool
	every time.Duration
}

var (
	// SyncEach syncs the log after each batch of writes, the writes made
	// while the previous batch was written and synced: a write is
	// durable once on disk, at the cost of a sync shared with its batch.
	SyncEach = Sync{}
	// SyncNone never syncs but on Close: a write is durable once copied
	// into the page cache, through a mapping of the log, without a
	// system call. It outlives the program, not the machine.
	SyncNone = Sync{none: true}
)

// SyncEvery syncs the log every d: a write is durable at the sync after
// it.
func SyncEvery(d time.Duration) Sync { return Sync{every: d} }

// Table is a table of entries, each a value under a key, in memory and in
// a log on disk. Its methods may be called from any goroutine.
//
// Values are not copied: a value put is shared with those who get it, and
// is not to be changed after.
type Table[K comparable, V any] struct {
	dir  string
	opts Options[K, V]
	kc   Codec[K]
	vc   Codec[V]
	idx  index[K, V]
	log  *wal
	live atomic.Int64
	lock *os.File
	bufs sync.Pool

	closed      atomic.Bool
	compactMu   sync.Mutex
	compacting  atomic.Bool
	compactions atomic.Uint64
	lastCompact atomic.Int64 // unix nanoseconds
	compactErr  atomic.Pointer[error]

	exp *expiries[K, V] // with Expires
}

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

// Get returns the value under k.
func (t *Table[K, V]) Get(k K) (V, bool) {
	v, _, ok := t.idx.get(k)
	return v, ok
}

// Len returns the number of entries.
func (t *Table[K, V]) Len() int { return t.idx.len() }

// Put puts v under k, and returns once the write is durable. Readers see
// it at once, before.
func (t *Table[K, V]) Put(k K, v V) error {
	b, err := t.put(k, v, nil)
	if err != nil {
		return err
	}
	return t.log.wait(b)
}

// Delete deletes the entry under k, if any, and returns once the delete
// is durable.
func (t *Table[K, V]) Delete(k K) error {
	b, err := t.delete(k, nil)
	if err != nil {
		return err
	}
	return t.log.wait(b)
}

func (t *Table[K, V]) put(k K, v V, match matcher[V]) (*batch, error) {
	bp := t.buf()
	defer t.putBuf(bp)
	body, err := t.encode(bp, opPut, k, v)
	if err != nil {
		return nil, err
	}
	b, err := t.idx.put(t.log, k, v, newRec(body), match)
	if err == nil {
		t.noteExpiry(k, v)
		t.maybeCompact()
	}
	return b, err
}

func (t *Table[K, V]) delete(k K, match matcher[V]) (*batch, error) {
	bp := t.buf()
	defer t.putBuf(bp)
	var zero V
	body, err := t.encode(bp, opDel, k, zero)
	if err != nil {
		return nil, err
	}
	b, err := t.idx.del(t.log, k, newRec(body), match)
	if err == nil {
		t.maybeCompact()
	}
	return b, err
}

// keptBuffer is the most a buffer kept for the next write holds: one a
// large record grew is let go.
const keptBuffer = 1 << 20

// putBuf keeps bp for the next encoding, unless a large record grew it.
func (t *Table[K, V]) putBuf(bp *[]byte) {
	if cap(*bp) <= keptBuffer {
		t.bufs.Put(bp)
	}
}

func (t *Table[K, V]) buf() *[]byte {
	if bp, ok := t.bufs.Get().(*[]byte); ok {
		return bp
	}
	b := make([]byte, 0, 256)
	return &b
}

// encode returns the body of the record of op on k and v, made in *bp.
// The key goes after room for its length, written before it once known.
func (t *Table[K, V]) encode(bp *[]byte, op byte, k K, v V) ([]byte, error) {
	const pre = 1 + binary.MaxVarintLen64
	b, err := t.kc.Append(append((*bp)[:0], make([]byte, pre)...), k)
	if err != nil {
		return nil, err
	}
	var l [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(l[:], uint64(len(b)-pre))
	start := pre - n - 1
	b[start] = op
	copy(b[start+1:pre], l[:n])
	if op == opPut {
		if b, err = t.vc.Append(b, v); err != nil {
			return nil, err
		}
	}
	*bp = b
	return b[start:], nil
}

// All returns the entries, in the order of their keys in an OrderedSet.
// Each shard is ranged over as it was at a moment: in a Set without
// ReadConcurrency, as a copy of it. The table may be written during the
// range.
func (t *Table[K, V]) All() iter.Seq2[K, V] {
	return t.Select(nil)
}

// Select returns the entries for which keep, if not nil, reports true,
// as All does.
func (t *Table[K, V]) Select(keep func(K, V) bool) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		// kept yields l, unless left out, and reports whether to go on.
		kept := func(l *leaf[K, V]) bool {
			if keep != nil && !keep(l.key, l.val) {
				return true
			}
			return yield(l.key, l.val)
		}
		if x, ok := t.idx.(*treeIndex[K, V]); ok {
			x.ascend(nil, nil, kept) // in order, merging the shards
			return
		}
		for shard := range t.idx.byShard(false) {
			for l := range shard {
				if !kept(l) {
					return
				}
			}
		}
	}
}

// Range returns the entries of the keys from lo, up to hi but not hi, in
// order, as All does. It panics if the table is not an OrderedSet.
//
// With WriteConcurrency, it merges the entries of the shards: a range of
// few entries costs a search in each shard.
func (t *Table[K, V]) Range(lo, hi K) iter.Seq2[K, V] {
	x, ok := t.idx.(*treeIndex[K, V])
	if !ok {
		panic("kvs: Range of a Set")
	}
	return func(yield func(K, V) bool) {
		x.ascend(&lo, &hi, func(l *leaf[K, V]) bool { return yield(l.key, l.val) })
	}
}

// Info is what Table.Info tells of a table.
type Info struct {
	Dir              string
	ReadConcurrency  bool
	WriteConcurrency bool
	Shards           int
	Type             Type

	// Size is the number of entries; LiveBytes, the size of the records
	// that wrote them, in the log.
	Size      int
	LiveBytes int64
	// DiskBytes is the size of the log, Segments its number of segments.
	DiskBytes int64
	Segments  int

	// LastSeq is the seq of the last write, DurableSeq of the last
	// durable: the writes between are not yet.
	LastSeq    uint64
	DurableSeq uint64
	// Syncs is the number of syncs of the log since Open.
	Syncs uint64

	// Compactions is the number of compactions since Open, the last at
	// LastCompaction; CompactionErr is the error of the last that ran by
	// itself and failed, if any.
	Compactions    uint64
	LastCompaction time.Time
	CompactionErr  error
}

// Info returns the size of the table and the state of its log.
func (t *Table[K, V]) Info() Info {
	shards := 1
	if t.opts.WriteConcurrency {
		shards = 1
		for shards < t.opts.Shards {
			shards <<= 1
		}
	}
	i := Info{
		Dir:              t.dir,
		ReadConcurrency:  t.opts.ReadConcurrency,
		WriteConcurrency: t.opts.WriteConcurrency,
		Shards:           shards,
		Type:             t.opts.Type,
		Size:             t.idx.len(),
		LiveBytes:        t.live.Load(),
		DiskBytes:        t.log.disk.Load(),
		Segments:         int(t.log.segments.Load()),
		LastSeq:          t.log.lastSeq(),
		DurableSeq:       t.log.durable.Load(),
		Syncs:            t.log.syncs.Load(),
		Compactions:      t.compactions.Load(),
	}
	if ns := t.lastCompact.Load(); ns != 0 {
		i.LastCompaction = time.Unix(0, ns)
	}
	if err := t.compactErr.Load(); err != nil {
		i.CompactionErr = *err
	}
	return i
}

// Compact rewrites the log as a snapshot of the entries, and the writes
// made while it is written: the segments before go. Writes go on during
// it.
func (t *Table[K, V]) Compact() error {
	t.compactMu.Lock()
	defer t.compactMu.Unlock()
	if t.closed.Load() {
		return ErrClosed
	}
	return t.compact()
}

// maybeCompact starts a compaction if the log has grown large enough.
func (t *Table[K, V]) maybeCompact() {
	if t.opts.CompactRatio < 0 {
		return
	}
	disk := t.log.disk.Load()
	if disk < t.opts.CompactMin || float64(disk) < t.opts.CompactRatio*float64(t.live.Load()) {
		return
	}
	if !t.compacting.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer t.compacting.Store(false)
		t.compactMu.Lock()
		defer t.compactMu.Unlock()
		if t.closed.Load() {
			return
		}
		if err := t.compact(); err != nil {
			t.compactErr.Store(&err)
		}
	}()
}

func (t *Table[K, V]) compact() error {
	cut := t.log.cut()
	if cut.err != nil {
		return cut.err
	}
	name := snapName(cut.seq)
	size, err := t.writeSnapshot(name, cut.seq)
	if err != nil {
		return err
	}
	t.log.disk.Add(size)
	remove := func(name string) {
		path := filepath.Join(t.dir, name)
		if fi, err := os.Stat(path); err == nil && os.Remove(path) == nil {
			t.log.disk.Add(-fi.Size())
		}
	}
	for _, s := range cut.old {
		remove(s)
		t.log.segments.Add(-1)
	}
	if _, snaps, err := listLog(t.dir); err == nil {
		for _, s := range snaps {
			if s.name != name {
				remove(s.name)
			}
		}
	}
	t.compactions.Add(1)
	t.lastCompact.Store(time.Now().UnixNano())
	return nil
}

// writeSnapshot writes the entries to the snapshot name, and returns its
// size. Each record has the seq of the write that set its entry, so that
// revisions outlive compactions.
// writeSnapshot writes the entries to the snapshot name, of the log up to
// seq: and of writes after, to the shards walked after them, which the
// snapshot holds too. It is put in place once those are durable in the
// log: else a crash losing them from the log would find them in it, never
// durable, and maybe without others before them, in shards walked before.
func (t *Table[K, V]) writeSnapshot(name string, seq uint64) (int64, error) {
	path := filepath.Join(t.dir, name)
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return 0, err
	}
	defer os.Remove(path + ".tmp") // if left
	w := bufio.NewWriterSize(f, 1<<20)
	size, last, werr := t.writeEntries(w, seq)
	if werr == nil {
		werr = w.Flush()
	}
	if werr == nil {
		werr = f.Sync()
	}
	if err := f.Close(); werr == nil {
		werr = err
	}
	if werr == nil && last > seq {
		werr = t.log.syncWritten()
	}
	if werr == nil {
		werr = os.Rename(path+".tmp", path)
	}
	if werr == nil {
		werr = syncDir(t.dir)
	}
	return size, werr
}

// writeEntries writes a record of each entry to w, as the shards hold them
// once locked, and returns their size and the latest write among them, or
// since, if later.
func (t *Table[K, V]) writeEntries(w io.Writer, since uint64) (size int64, last uint64, err error) {
	last = since
	var buf, out []byte
	for shard := range t.idx.byShard(true) {
		for l := range shard {
			body, err := t.encode(&buf, opPut, l.key, l.val)
			if err != nil {
				return size, last, err
			}
			out = appendRecord(out[:0], newRec(body), l.rev)
			if _, err := w.Write(out); err != nil {
				return size, last, err
			}
			last = max(last, l.rev)
			size += int64(len(out))
		}
	}
	return size, last, nil
}

// Close makes the writes durable, closes the log, and lets go of the
// directory. Writes after fail with ErrClosed; reads go on, of the
// entries as they are. Closing again does nothing.
func (t *Table[K, V]) Close() error {
	if t.closed.Swap(true) {
		return nil
	}
	t.compactMu.Lock()
	defer t.compactMu.Unlock()
	t.log.close()
	return errors.Join(t.log.failure(), unlockDir(t.lock))
}
