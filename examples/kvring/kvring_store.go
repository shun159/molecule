package main

import (
	"context"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/kvs"
)

func init() { dist.Register(getReq{}, putReq{}, Reply{}) }

// storeName is the name of the store on every node.
const storeName = actor.Local("kvring_store")

// Table is the table of a node: its keys, on disk.
type Table = kvs.Table[string, string]

// Store is the gen_server of the part of the table on one node, the keys
// the ring gives the node, the table in Dir.
type Store struct {
	genserver.Default[struct{}]
	Node string
	Dir  string
}

// request is a getReq or a putReq.
type request interface{ key() string }

type (
	getReq struct{ Key string }
	putReq struct{ Key, Value string }
)

func (r getReq) key() string { return r.Key }
func (r putReq) key() string { return r.Key }

// Reply answers a request: the value of a Get, if Found, and the node
// that served it.
type Reply struct {
	Value string
	Found bool
	Node  string
}

// Get returns the value under key, from its node on ring.
func Get(ctx context.Context, caller actor.Caller, ring Ring, key string) (Reply, error) {
	return call(ctx, caller, ring, getReq{Key: key})
}

// Put puts value under key, at its node on ring, and returns once it
// is on the disk of that node.
func Put(ctx context.Context, caller actor.Caller, ring Ring, key, value string) (Reply, error) {
	return call(ctx, caller, ring, putReq{Key: key, Value: value})
}

// call sends req to the store of the node of its key.
func call(ctx context.Context, caller actor.Caller, ring Ring, req request) (Reply, error) {
	to := actor.Remote{Node: ring.Owner(req.key()), Name: string(storeName)}
	return genserver.NewRef[request, Reply, genserver.None](to).Call(ctx, caller, req)
}

func (s Store) HandleCall(st struct{}, req request, from genserver.From[Reply]) (struct{}, []actor.Effect) {
	table, err := kvs.Opened[string, string](s.Dir)
	if err != nil {
		return st, actor.Do(actor.Stop{Reason: err})
	}
	switch r := req.(type) {
	case getReq:
		return st, s.get(table, r, from)
	case putReq:
		return st, s.put(table, r, from)
	}
	return st, nil
}

func (s Store) HandleInfo(st struct{}, msg any) (struct{}, []actor.Effect) {
	if d, ok := msg.(kvs.Durable); ok {
		return st, s.durable(d)
	}
	return st, nil
}

func (s Store) get(table *Table, r getReq, from genserver.From[Reply]) []actor.Effect {
	v, ok := table.Get(r.Key)
	return actor.Do(from.Reply(Reply{Value: v, Found: ok, Node: s.Node}))
}

// put writes, and leaves the reply for when the write is durable.
func (s Store) put(table *Table, r putReq, from genserver.From[Reply]) []actor.Effect {
	return actor.Do(table.PutEffect(r.Key, r.Value, from))
}

// durable replies to the put of d, now on disk. A table failing a write
// fails every one after: the store stops, and the table is opened again
// with it.
func (s Store) durable(d kvs.Durable) []actor.Effect {
	if d.Err != nil {
		return actor.Do(actor.Stop{Reason: d.Err})
	}
	return actor.Do(d.Tag.(genserver.From[Reply]).Reply(Reply{Node: s.Node}))
}
