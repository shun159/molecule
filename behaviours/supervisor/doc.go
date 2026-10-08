// Package supervisor is the generic supervisor, like Erlang's supervisor: a
// process that starts, watches and restarts other processes, its children.
//
// A child is a worker, usually a genserver, genstatem or gen behaviour, or
// another supervisor. Supervisors started under one another form a
// supervision tree, the structure of a program that keeps running when
// parts of it fail.
//
// Unlike in OTP, a supervisor has no callback module: what it supervises
// is given as data, a [Spec]. Its decisions (whether a child is restarted,
// whether the restart intensity is exceeded, which children to stop and to
// start again) are pure functions of their inputs.
//
// # Supervision principles
//
// The supervisor starts its children, stops them, and keeps them alive by
// restarting them when they exit.
//
// The children are a list of child specifications, [ChildSpec]. When the
// supervisor starts, it starts the children in the order of the list. When
// it stops, it stops them in the reverse order.
//
// # Supervisor flags
//
// The fields of [Spec], besides the children:
//
//	Name       registered when the supervisor starts
//	Strategy   OneForOne, OneForAll or RestForOne
//	Intensity  restarts allowed within Period; 1 if zero
//	Period     5 seconds if zero
//
// # Restart strategies
//
//   - [OneForOne]: when a child is to be restarted, only that child is
//     affected. This is the default.
//   - [OneForAll]: when a child is to be restarted, the other children are
//     stopped, in reverse start order, then all are started again, in order.
//   - [RestForOne]: when a child is to be restarted, the children started
//     after it are stopped, in reverse order, then it and they are started
//     again, in order.
//
// A temporary child stopped because of another child is not started again,
// and is forgotten.
//
// OTP's simple_one_for_one is a separate kind of supervisor here, see
// [DynamicSpec]: it starts with no children, [StartChild] adds them, and
// they are known by PID. Each is restarted on its own, and on shutdown all
// are stopped at once, in no defined order.
//
// # Restart intensity and period
//
// To keep a supervisor from restarting a failing child forever, if more
// than Intensity restarts happen within Period, the supervisor stops its
// children and then exits with [ErrMaxIntensity]. That reason wraps
// proc.Shutdown, so the supervisor above restarts it only if it is
// permanent.
//
// # Child specification
//
//   - ID identifies the child within the supervisor. It is required, except
//     for a dynamic supervisor.
//   - Start starts the child and returns its PID. It must start the child
//     linked to the supervisor, which it gets as parent, and wait until the
//     child has started: the StartLinkFunc functions of genserver,
//     genstatem, gen and this package do so. Returning molecule.ErrIgnore means
//     the child is not to run: the supervisor keeps its specification
//     without a process. Any other error is a failure to start.
//   - Restart says when a child that exited is restarted. [Permanent]
//     children always are. [Temporary] children never are, and are
//     forgotten. [Transient] children are restarted only after an abnormal
//     exit, with a reason other than proc.Normal or proc.Shutdown, wrapped
//     or not. Permanent is the default.
//   - Shutdown says how a child is stopped. The supervisor sends it an exit
//     signal with proc.Shutdown, waits up to Shutdown for it to exit, then
//     kills it. [Brutal] kills it at once; [Infinity] waits as long as it
//     takes. The default is 5 seconds for a worker and Infinity for a
//     supervisor, which must be given the time to stop its own children.
//   - Type is [Worker] or [Supervisor], Worker by default. It only changes
//     the default Shutdown.
//
// A child stops on a shutdown signal only if it does: a behaviour trapping
// exits runs Terminate and exits, one not trapping exits is terminated by
// the signal. A process blocked in a call of its own that ignores its
// context is not stopped, even when killed; see proc.
//
// # Starting
//
// [StartLink] and [Start] return once all the children have started. If a
// child fails to start, the children already started are stopped in
// reverse order, and the start fails with a [*StartError] wrapping both
// proc.Shutdown and the reason of the child. If the name is taken, it fails
// with a molecule.AlreadyStartedError.
//
// When a restart fails to start a child, the supervisor tries again,
// counting it as another restart, as OTP does.
//
// # Stopping
//
// A supervisor stops when its parent exits, and on [Stop]. It stops its
// children first. Like OTP's, it ignores exit signals from processes other
// than its parent and its children.
//
// # Reports
//
// A supervisor reports to the logger of its node when a child exits with an
// abnormal reason, when a child fails to start, and when it gives up after
// too many restarts. It reports each child it starts at debug level.
package supervisor
