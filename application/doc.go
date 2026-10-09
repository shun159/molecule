// Package application starts and stops the trees of processes of a
// program, like OTP's applications: each [App] is a tree, started from its
// top, usually a supervisor, under a master process watching it.
//
//	func main() {
//		n := proc.NewNode("echo@host")
//		err := application.Run(context.Background(), n,
//			application.App{Name: "echo", Start: supervisor.Child(echoSup)},
//		)
//		...
//	}
//
// [Run] starts the applications in order, runs them until the program gets
// SIGINT or SIGTERM, its context is done, or a permanent application ends,
// then stops them in reverse order: each top is asked to stop, and a
// supervisor stops its children, in reverse order, each given its
// Shutdown. A program stops as a whole, in order, rather than at once.
//
// An application is Permanent or Temporary, as in OTP: when the top of a
// permanent one ends, as a supervisor giving up does, all stop, and Run
// returns why; a temporary one is reported, and the others run on.
//
// [Start] and [Running.Stop] do the same for a program doing more than
// waiting. [Running.Start] starts more applications once others run, for a
// program with work of its own to do in between: they stop before those
// started earlier, as if they had been started with them.
package application
