package proc

import "sync"

// procTable maps the ids of the live local processes to them. It is
// sharded, as every spawn and death writes to it and every send reads it.
type procTable struct {
	shards [64]procShard
}

type procShard struct {
	mu sync.RWMutex
	m  map[uint64]*process
	_  [32]byte // keep shards on separate cache lines
}

func (t *procTable) shard(id uint64) *procShard { return &t.shards[id%uint64(len(t.shards))] }

func (t *procTable) get(id uint64) *process {
	s := t.shard(id)
	s.mu.RLock()
	p := s.m[id]
	s.mu.RUnlock()
	return p
}

func (t *procTable) put(id uint64, p *process) {
	s := t.shard(id)
	s.mu.Lock()
	if s.m == nil {
		s.m = make(map[uint64]*process)
	}
	s.m[id] = p
	s.mu.Unlock()
}

func (t *procTable) del(id uint64) {
	s := t.shard(id)
	s.mu.Lock()
	delete(s.m, id)
	s.mu.Unlock()
}

// all returns the live processes, in no particular order.
func (t *procTable) all() []*process {
	var ps []*process
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		for _, p := range s.m {
			ps = append(ps, p)
		}
		s.mu.RUnlock()
	}
	return ps
}
