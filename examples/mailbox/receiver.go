package main

import (
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
)

// Receiver is the gen_server every mailer sends to: it counts the Pong
// casts in its mailbox, and answers Wait once Expect of them arrived.
type Receiver struct {
	Expect int
}

// Pong is what mailers cast, the be pong() of the Pony example.
type Pong struct{}

// Wait returns the number of pongs once all are in.
type Wait struct{}

type receiverState struct {
	pongs   int
	waiting []genserver.From[int] // Wait calls, answered when all are in
}

func (Receiver) Init() (receiverState, []gen.Effect, error) {
	return receiverState{}, nil, nil
}

func (r Receiver) HandleCast(s receiverState, _ Pong) (receiverState, []gen.Effect) {
	s.pongs++
	if s.pongs != r.Expect {
		return s, nil
	}
	return receiverState{pongs: s.pongs}, r.answer(s)
}

func (r Receiver) HandleCall(s receiverState, _ Wait, from genserver.From[int]) (receiverState, []gen.Effect) {
	if s.pongs >= r.Expect {
		return s, gen.Do(from.Reply(s.pongs))
	}
	// Not all in yet: keep the caller waiting, and reply later.
	s.waiting = append(s.waiting[:len(s.waiting):len(s.waiting)], from)
	return s, nil
}

// answer replies to every waiting caller.
func (Receiver) answer(s receiverState) []gen.Effect {
	var effs []gen.Effect
	for _, from := range s.waiting {
		effs = append(effs, from.Reply(s.pongs))
	}
	return effs
}
