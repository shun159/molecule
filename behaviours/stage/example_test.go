package stage_test

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// Lines emits the lines of a scanner, read in an Async no faster than
// they are asked for, and stops at the end.
type Lines struct {
	Scanner *bufio.Scanner
}

type reading struct {
	want int // lines asked for and not read
	busy int // lines the read running reads, if any
}

func (Lines) Init(proc.PID) (reading, []actor.Effect, error) { return reading{}, nil, nil }

func (l Lines) HandleDemand(r reading, n int) (reading, []actor.Effect) {
	r.want += n
	return l.read(r)
}

func (l Lines) HandleInfo(r reading, msg any) (reading, []actor.Effect) {
	res, ok := msg.(actor.AsyncResult)
	if !ok {
		return r, nil
	}
	batch, _ := res.Value.([]string)
	emit := stage.Emit[string]{Events: batch}
	if res.Err != nil || len(batch) < r.busy {
		return r, actor.Do(emit, actor.Stop{Reason: res.Err}) // the end
	}
	r.want, r.busy = r.want-len(batch), 0
	r, more := l.read(r)
	return r, append(actor.Do(emit), more...)
}

// read reads the lines wanted, unless a read is running.
func (l Lines) read(r reading) (reading, []actor.Effect) {
	if r.busy > 0 || r.want == 0 {
		return r, nil
	}
	r.busy = r.want
	n := r.busy
	return r, actor.Do(actor.Async{Key: "read", Run: func(context.Context) (any, error) {
		var batch []string
		for len(batch) < n && l.Scanner.Scan() {
			batch = append(batch, l.Scanner.Text())
		}
		return batch, l.Scanner.Err()
	}})
}

// Words counts the words of the lines of its producer, and tells Done
// the count once its producer is done, and so is it.
type Words struct {
	From actor.Dest
	Done actor.From
}

func (w Words) Init(proc.PID) (int, []actor.Effect, error) {
	return 0, actor.Do(stage.Subscribe{To: w.From, Max: 2}), nil
}

func (Words) HandleEvents(n int, lines []string, _ any) (int, []actor.Effect) {
	for _, l := range lines {
		n += len(strings.Fields(l))
	}
	return n, nil
}

func (w Words) Terminate(n int, _ error) []actor.Effect {
	return actor.Do(actor.Reply{To: w.Done, Value: n})
}

func Example() {
	n := proc.NewNode("")
	ctx := context.Background()
	text := "the quick brown fox\njumps over\nthe lazy dog\n"

	src, err := stage.Start(ctx, n, stage.Produce(Lines{Scanner: bufio.NewScanner(strings.NewReader(text))}))
	if err != nil {
		panic(err)
	}
	done := n.Alias()
	_, err = stage.Start(ctx, n, stage.Consume(Words{From: src, Done: actor.From{Tag: done.Ref}}))
	if err != nil {
		panic(err)
	}
	fmt.Println("words:", (<-done.C).Msg)
	// Output: words: 9
}
