package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/pmap"
	"github.com/shun159/molecule/proc"
)

// Reduce counts and sums the records of its partition by key, from the
// parsers of all the nodes, and writes to Out what each window of Every
// had: its keys, the most counted first, Top of them at most.
type Reduce struct {
	Partition int
	From      []actor.Dest
	Every     time.Duration
	Top       int
	Out       io.Writer
}

// tally is the count and sum of a key.
type tally struct{ Count, Sum int64 }

func (r Reduce) Init(proc.PID) (pmap.Map[string, tally], []actor.Effect, error) {
	var out []actor.Effect
	for i, from := range r.From {
		out = append(out, stage.Subscribe{To: from, Tag: i, Max: 1000, Partition: r.Partition, Ending: stage.Resubscribe})
	}
	return pmap.Map[string, tally]{}, out, nil
}

func (Reduce) HandleEvents(keys pmap.Map[string, tally], records []record, _ any) (pmap.Map[string, tally], []actor.Effect) {
	for _, rec := range records {
		t, _ := keys.Get(rec.Key)
		keys = keys.Set(rec.Key, tally{Count: t.Count + 1, Sum: t.Sum + rec.Value})
	}
	return keys, nil
}

func (r Reduce) Window() time.Duration { return r.Every }

// HandleWindow writes the window in an Async, and starts the next afresh.
func (r Reduce) HandleWindow(keys pmap.Map[string, tally], start time.Time) (pmap.Map[string, tally], []actor.Effect) {
	return pmap.Map[string, tally]{}, actor.Do(actor.Async{Key: start, Run: func(context.Context) (any, error) {
		_, err := io.WriteString(r.Out, r.report(keys, start))
		return nil, err
	}})
}

// report tells of the keys of the window of start.
func (r Reduce) report(keys pmap.Map[string, tally], start time.Time) string {
	type entry struct {
		key string
		tally
	}
	var all []entry
	var records int64
	for k, t := range keys.All() {
		all = append(all, entry{k, t})
		records += t.Count
	}
	slices.SortFunc(all, func(a, b entry) int { return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.key, b.key)) })
	var b strings.Builder
	fmt.Fprintf(&b, "%s partition %d: %d records, %d keys\n", start.Format(time.TimeOnly), r.Partition, records, len(all))
	for _, e := range all[:min(r.Top, len(all))] {
		fmt.Fprintf(&b, "  %-24s count %-8d sum %d\n", e.key, e.Count, e.Sum)
	}
	return b.String()
}
