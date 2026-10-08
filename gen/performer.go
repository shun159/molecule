package gen

import "github.com/shun159/molecule"

// Performer is an effect that performs itself, for what lies outside the
// processes, such as a socket: the runtime calls Perform in the process
// of the behaviour, in the order of the effects, with the Env it acts on.
// It embeds Extension. The effect is still a value, compared and tested as
// any other; only the runtime performs it, so the behaviour stays pure.
// In gensim, Perform acts on the real world all the same.
type Performer interface {
	molecule.Effect
	Perform(env Env)
}
