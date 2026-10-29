package main

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// record is a line parsed: a key, and a value of it, 1 if the line has
// none.
type record struct {
	Key   string
	Value int64
}

// parseLine parses a line "key [value]", and reports whether it is one.
func parseLine(line string) (record, bool) {
	key, rest, _ := strings.Cut(strings.TrimSpace(line), " ")
	if key == "" {
		return record{}, false
	}
	if rest == "" {
		return record{Key: key, Value: 1}, true
	}
	v, err := strconv.ParseInt(strings.TrimSpace(rest), 10, 64)
	return record{Key: key, Value: v}, err == nil
}

// Records encode themselves, to go between the nodes as bytes, not gob.
func (r record) AppendBinary(b []byte) ([]byte, error) {
	b = binary.AppendUvarint(b, uint64(len(r.Key)))
	return binary.AppendVarint(append(b, r.Key...), r.Value), nil
}

func (r *record) DecodeBinary(b []byte) error {
	l, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b)-n) < l {
		return errRecord
	}
	key := string(b[n : n+int(l)])
	v, m := binary.Varint(b[n+int(l):])
	if m <= 0 {
		return errRecord
	}
	*r = record{Key: key, Value: v}
	return nil
}

var errRecord = errors.New("logstats: record cut short")

func init() { stage.Register[record]() }

// Parse parses the lines of its tail into records, the lines not of
// records dropped, and partitions them by key among the reducers, one a
// node: a node gone, the records of its partition are dropped, the
// others going on. Its state is the lines not records.
type Parse struct {
	From       actor.Dest
	Partitions int
}

func (p Parse) Dispatcher() stage.Dispatcher {
	return stage.Partition[record]{Partitions: p.Partitions, Hash: p.partition, Drop: true}
}

// partition is the partition of the key of r.
func (p Parse) partition(r record) int {
	h := fnv.New32a()
	h.Write([]byte(r.Key))
	return int(h.Sum32() % uint32(p.Partitions))
}

func (p Parse) Init(proc.PID) (int, []actor.Effect, error) {
	return 0, actor.Do(stage.Subscribe{To: p.From, Max: 1000, Ending: stage.Resubscribe}), nil
}

func (Parse) HandleEvents(bad int, lines []string, _ any) (int, []actor.Effect) {
	records := make([]record, 0, len(lines))
	for _, line := range lines {
		if r, ok := parseLine(line); ok {
			records = append(records, r)
		} else {
			bad++
		}
	}
	return bad, actor.Do(stage.Emit[record]{Events: records})
}
