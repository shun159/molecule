package supervisor

// Strategy is how a supervisor reacts to a child that is to be restarted.
type Strategy int

const (
	// OneForOne restarts only the child that terminated.
	OneForOne Strategy = iota
	// OneForAll terminates the other children, in reverse start order,
	// then restarts them all in start order.
	OneForAll
	// RestForOne terminates the children started after the one that
	// terminated, in reverse order, then restarts it and them in order.
	RestForOne
)

func (s Strategy) String() string {
	switch s {
	case OneForOne:
		return "one_for_one"
	case OneForAll:
		return "one_for_all"
	case RestForOne:
		return "rest_for_one"
	}
	return "unknown"
}

// Restart is when a terminated child is restarted.
type Restart int

const (
	// Permanent children are always restarted.
	Permanent Restart = iota
	// Transient children are restarted only after an abnormal exit, that
	// is, other than proc.Normal or proc.Shutdown.
	Transient
	// Temporary children are never restarted, and are forgotten once
	// they terminate.
	Temporary
)

func (r Restart) String() string {
	switch r {
	case Permanent:
		return "permanent"
	case Transient:
		return "transient"
	case Temporary:
		return "temporary"
	}
	return "unknown"
}
