package compare

import (
	"encoding/binary"
	"errors"
	"path/filepath"

	"github.com/rosedblabs/rosedb/v2"
	"github.com/shun159/molecule/kvs"
	bolt "go.etcd.io/bbolt"
	"go.mills.io/bitcask/v2"
)

// A store is a table of the benchmarks: the key of i is keys[i].
type store interface {
	put(i uint64, v []byte) error
	get(i uint64) bool
	close() error
}

// An engine opens a store in dir; durable says a put returns once on disk.
type engine struct {
	name string
	open func(dir string, durable bool) (store, error)
}

var engines = []engine{
	{"kvs", openKVS(kvs.Options[string, []byte]{WriteConcurrency: true})},
	{"kvs-rc", openKVS(kvs.Options[string, []byte]{ReadConcurrency: true})},
	{"rosedb", openRosedb},
	{"bitcask-go", openBitcask},
	{"bbolt", openBolt},
}

const (
	numKeys = 100_000
	valSize = 64
)

var (
	val   = make([]byte, valSize)
	keys  = make([][]byte, numKeys)
	skeys = make([]string, numKeys)
)

func init() {
	for i := range keys {
		keys[i] = binary.BigEndian.AppendUint64(nil, uint64(i))
		skeys[i] = string(keys[i])
	}
}

// kvs, with the options recommended for a server, WriteConcurrency:
// shards of maps, each under a RWMutex; or with ReadConcurrency: a trie
// read without lock, written under a Mutex.
type kvsStore struct{ t *kvs.Table[string, []byte] }

func openKVS(opts kvs.Options[string, []byte]) func(string, bool) (store, error) {
	return func(dir string, durable bool) (store, error) {
		o := opts
		if durable {
			o.Sync = kvs.SyncEach
		} else {
			o.Sync = kvs.SyncNone
		}
		t, err := kvs.Open(dir, o)
		return kvsStore{t}, err
	}
}

func (s kvsStore) put(i uint64, v []byte) error { return s.t.Put(skeys[i], v) }
func (s kvsStore) get(i uint64) bool            { _, ok := s.t.Get(skeys[i]); return ok }
func (s kvsStore) close() error                 { return s.t.Close() }

// rosedb v2: Bitcask, a B-tree index over a WAL; Sync fsyncs each write.
type rosedbStore struct{ db *rosedb.DB }

func openRosedb(dir string, durable bool) (store, error) {
	opts := rosedb.DefaultOptions
	opts.DirPath = dir
	opts.Sync = durable
	db, err := rosedb.Open(opts)
	return rosedbStore{db}, err
}

func (s rosedbStore) put(i uint64, v []byte) error { return s.db.Put(keys[i], v) }
func (s rosedbStore) get(i uint64) bool {
	_, err := s.db.Get(keys[i])
	return err == nil
}
func (s rosedbStore) close() error { return s.db.Close() }

// go.mills.io/bitcask: Bitcask, a radix tree over data files; with
// SyncWrites each write syncs.
type bitcaskStore struct{ db *bitcask.Bitcask }

func openBitcask(dir string, durable bool) (store, error) {
	db, err := bitcask.Open(dir, bitcask.WithSyncWrites(durable))
	return bitcaskStore{db}, err
}

func (s bitcaskStore) put(i uint64, v []byte) error { return s.db.Put(keys[i], v) }
func (s bitcaskStore) get(i uint64) bool {
	_, err := s.db.Get(keys[i])
	return err == nil
}
func (s bitcaskStore) close() error { return s.db.Close() }

// bbolt, for reference: a B+tree in a file, mapped; each Update commits
// a transaction, synced unless NoSync.
type boltStore struct{ db *bolt.DB }

var bucket = []byte("b")

func openBolt(dir string, durable bool) (store, error) {
	db, err := bolt.Open(filepath.Join(dir, "bolt.db"), 0o600, &bolt.Options{NoSync: !durable})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	})
	return boltStore{db}, err
}

func (s boltStore) put(i uint64, v []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).Put(keys[i], v)
	})
}

func (s boltStore) get(i uint64) bool {
	var ok bool
	s.db.View(func(tx *bolt.Tx) error {
		ok = tx.Bucket(bucket).Get(keys[i]) != nil
		return nil
	})
	return ok
}

func (s boltStore) close() error { return s.db.Close() }

var errMissing = errors.New("missing key")
