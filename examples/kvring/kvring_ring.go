package main

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"strconv"
)

// Ring places each node at Vnodes points on a circle of hashes, as
// consistent hashing does: a key belongs to the node of the first point at
// or after its hash. A node added takes from the others only the keys it
// now owns, about 1/n of them; a node removed gives away only its own.
type Ring struct {
	points []point // by hash
}

type point struct {
	hash uint64
	node string
}

// NewRing makes the ring of nodes, each at vnodes points: the more, the
// more even the share of each.
func NewRing(nodes []string, vnodes int) Ring {
	var r Ring
	for _, n := range nodes {
		for i := range vnodes {
			r.points = append(r.points, point{hash(n + "#" + strconv.Itoa(i)), n})
		}
	}
	slices.SortFunc(r.points, func(a, b point) int { return cmp.Compare(a.hash, b.hash) })
	return r
}

// Owner returns the node key belongs to.
func (r Ring) Owner(key string) string {
	h := hash(key)
	i, _ := slices.BinarySearchFunc(r.points, h, func(p point, h uint64) int { return cmp.Compare(p.hash, h) })
	if i == len(r.points) {
		i = 0 // past the last point, round to the first
	}
	return r.points[i].node
}

// hash is any hash the nodes agree on, spread evenly.
func hash(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}
