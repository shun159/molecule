package gen

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Pending is a call made without waiting for its reply, like the request
// id of gen_server:send_request: the caller goes on, and takes the reply
// when it is there. Done tells it is, in a select as well; Result returns
// it, Wait waits for it.
//
//	p := ref.SendRequest(self, req)
//	select {
//	case <-p.Done():
//		rep, err := p.Result()
//	case <-ctx.Done():
//		p.Cancel()
//	}
//
// The outcome is that of Call: the reply, an *ExitError if the server
// dies first, or the exit reason of the caller dying first. A Pending
// not waited for to its end is cancelled, or it holds a monitor of the
// server until the server replies or dies.
type Pending[Rep any] struct {
	done   chan struct{}
	cancel chan struct{}
	once   sync.Once
	rep    Rep
	err    error
}

// ErrNotDone is the error of Result before Done.
var ErrNotDone = errors.New("gen: no reply yet")

// ErrCancelled is the outcome of a Pending cancelled.
var ErrCancelled = errors.New("gen: request cancelled")

// Request sends req to the server at to, as Call does, without waiting
// for the reply. Behaviours return a SendRequest effect instead; genserver
// and genstatem call it with the type of the reply.
func Request[Rep any](caller Caller, to Dest, req any) *Pending[Rep] {
	p := &Pending[Rep]{done: make(chan struct{}), cancel: make(chan struct{})}
	a, err := sendCall(caller, to, func(f From) any { return CallMsg{From: f, Req: req} })
	if err != nil {
		p.err = err
		close(p.done)
		return p
	}
	d, _ := caller.(dying)
	var callerDone <-chan struct{}
	if d != nil {
		callerDone = d.Done()
	}
	go func() {
		defer close(p.done)
		defer a.Release()
		select {
		case m := <-a.C:
			switch {
			case m.Down:
				p.err = &ExitError{To: to, Reason: m.Reason}
			case m.Msg != nil:
				rep, ok := m.Msg.(Rep)
				if !ok {
					p.err = fmt.Errorf("gen: reply of type %T, want %T", m.Msg, p.rep)
				}
				p.rep = rep
			}
		case <-p.cancel:
			p.err = ErrCancelled
		case <-callerDone:
			p.err = d.ExitReason()
		}
	}()
	return p
}

// Done is closed once the outcome is there.
func (p *Pending[Rep]) Done() <-chan struct{} { return p.done }

// Result returns the outcome, or ErrNotDone before Done, as
// gen_server:check_response does.
func (p *Pending[Rep]) Result() (Rep, error) {
	select {
	case <-p.done:
		return p.rep, p.err
	default:
		var zero Rep
		return zero, ErrNotDone
	}
}

// Wait waits for the outcome, or returns ctx.Err() if ctx is done first,
// the request still pending, as gen_server:wait_response does.
func (p *Pending[Rep]) Wait(ctx context.Context) (Rep, error) {
	select {
	case <-p.done:
		return p.rep, p.err
	case <-ctx.Done():
		var zero Rep
		return zero, ctx.Err()
	}
}

// Cancel abandons the request: the reply, should it come, is dropped, and
// the outcome is ErrCancelled, unless it was there already.
func (p *Pending[Rep]) Cancel() {
	p.once.Do(func() { close(p.cancel) })
	<-p.done
}
