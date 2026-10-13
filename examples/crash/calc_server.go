package main

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
)

// CalcServer is a gen_server holding a number. It has a bug, of the kind
// supervision is for: dividing by zero panics, taking the server down.
type CalcServer struct{ genserver.Default[int] }

// CalcReq is what CalcServer answers.
type CalcReq interface{ calcReq() }

type (
	// Get returns the number.
	Get struct{}
	// Div divides the number and returns the result. By zero, it panics.
	Div struct{ By int }
)

func (Get) calcReq() {}
func (Div) calcReq() {}

// Add is the only cast.
type Add struct{ N int }

var (
	calcName = actor.Local("calc")
	calcRef  = genserver.RefFor(CalcServer{}, calcName)
)

func (CalcServer) HandleCall(n int, req CalcReq, from genserver.From[int]) (int, []actor.Effect) {
	switch r := req.(type) {
	case Get:
		return n, actor.Do(from.Reply(n))
	case Div:
		n /= r.By // no check for zero: the bug
		return n, actor.Do(from.Reply(n))
	}
	return n, nil
}

func (CalcServer) HandleCast(n int, msg Add) (int, []actor.Effect) {
	return n + msg.N, nil
}
