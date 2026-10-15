package pmap

import (
	"bytes"
	"encoding/gob"
	"errors"
)

// entries are the keys and the values of a map, as gob encodes it.
type entries[K comparable, V any] struct {
	Keys []K
	Vals []V
}

// GobEncode encodes the entries of m, for a Map to go between nodes, or to
// disk, in what gob encodes, as the state of a behaviour.
func (m Map[K, V]) GobEncode() ([]byte, error) {
	es := entries[K, V]{Keys: make([]K, 0, m.n), Vals: make([]V, 0, m.n)}
	for k, v := range m.All() {
		es.Keys, es.Vals = append(es.Keys, k), append(es.Vals, v)
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(es)
	return buf.Bytes(), err
}

// GobDecode decodes what GobEncode encoded into m.
func (m *Map[K, V]) GobDecode(b []byte) error {
	var es entries[K, V]
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&es); err != nil {
		return err
	}
	if len(es.Keys) != len(es.Vals) {
		return errors.New("pmap: as many keys as values decoded")
	}
	if len(es.Keys) == 0 {
		*m = Map[K, V]{}
		return nil
	}
	*m = Collect(func(yield func(K, V) bool) {
		for i, k := range es.Keys {
			if !yield(k, es.Vals[i]) {
				return
			}
		}
	})
	return nil
}
