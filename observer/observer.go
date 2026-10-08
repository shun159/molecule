package observer

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// Processes returns what the processes of n are and do, those with the
// most messages waiting first: where a program is behind shows at the top.
func Processes(n *proc.Node) []proc.ProcessInfo {
	var infos []proc.ProcessInfo
	for _, pid := range n.Processes() {
		if info, ok := n.Info(pid); ok {
			infos = append(infos, info)
		}
	}
	slices.SortStableFunc(infos, func(a, b proc.ProcessInfo) int {
		return cmp.Compare(b.MessageQueueLen, a.MessageQueueLen)
	})
	return infos
}

// WriteProcesses writes infos as a table.
func WriteProcesses(w io.Writer, infos []proc.ProcessInfo) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PID\tMSGS\tLINKS\tMONITORED\tNAME\tLABEL")
	for _, p := range infos {
		fmt.Fprintf(tw, "%v\t%d\t%d\t%d\t%s\t%s\n", p.PID, p.MessageQueueLen, len(p.Links), p.MonitoredBy, p.Name, p.Label)
	}
	return tw.Flush()
}

// Tree is a supervision tree, as its supervisors tell it.
type Tree struct {
	ID       string   `json:"id,omitempty"`
	PID      proc.PID `json:"pid"`
	Label    string   `json:"label"`
	Restarts int      `json:"restarts"`
	Children []Tree   `json:"children,omitempty"`
}

// TreeOf returns the tree under top, asking each supervisor its children.
// A child not running has a zero PID.
func TreeOf(ctx context.Context, n *proc.Node, top proc.PID) (Tree, error) {
	t := Tree{PID: top}
	if info, ok := n.Info(top); ok {
		t.Label = info.Label
	}
	children, err := supervisor.WhichChildren(ctx, n, top)
	if err != nil {
		return t, err
	}
	for _, c := range children {
		sub := Tree{ID: c.ID, PID: c.PID, Restarts: c.Restarts}
		switch {
		case c.PID.IsZero():
		case c.Type == supervisor.Supervisor:
			var err error
			if sub, err = TreeOf(ctx, n, c.PID); err != nil {
				return t, err
			}
			sub.ID, sub.Restarts = c.ID, c.Restarts
		default:
			if info, ok := n.Info(c.PID); ok {
				sub.Label = info.Label
			}
		}
		t.Children = append(t.Children, sub)
	}
	return t, nil
}

// WriteTree draws t. A run of three or more children alike, the
// connections of a pool say, is drawn as one line: their count and label.
// Children are alike when they have no ID, no children, no restarts, and
// the same label.
func WriteTree(w io.Writer, t Tree) error { return writeTree(w, t, false) }

// WriteTreeFull draws t, every child on a line of its own.
func WriteTreeFull(w io.Writer, t Tree) error { return writeTree(w, t, true) }

func writeTree(w io.Writer, t Tree, full bool) error {
	var b strings.Builder
	line(&b, t)
	draw(&b, t.Children, "", full)
	_, err := io.WriteString(w, b.String())
	return err
}

func draw(b *strings.Builder, children []Tree, prefix string, full bool) {
	for i := 0; i < len(children); {
		c := children[i]
		run := 1
		if !full {
			for i+run < len(children) && alike(c, children[i+run]) {
				run++
			}
			if run < 3 {
				run = 1
			}
		}
		branch, next := "├─ ", "│  "
		if i+run == len(children) {
			branch, next = "└─ ", "   "
		}
		b.WriteString(prefix + branch)
		if run > 1 {
			fmt.Fprintf(b, "%d × %s\n", run, cmp.Or(c.Label, "(no label)"))
		} else {
			line(b, c)
			draw(b, c.Children, prefix+next, full)
		}
		i += run
	}
}

func alike(a, b Tree) bool {
	plain := func(t Tree) bool {
		return t.ID == "" && len(t.Children) == 0 && t.Restarts == 0 && !t.PID.IsZero()
	}
	return plain(a) && plain(b) && a.Label == b.Label
}

func line(b *strings.Builder, t Tree) {
	var parts []string
	if t.ID != "" {
		parts = append(parts, t.ID)
	}
	if t.PID.IsZero() {
		parts = append(parts, "(not running)")
	} else {
		parts = append(parts, t.PID.String())
	}
	if t.Label != "" {
		parts = append(parts, t.Label)
	}
	if t.Restarts > 0 {
		parts = append(parts, fmt.Sprintf("restarted %d", t.Restarts))
	}
	b.WriteString(strings.Join(parts, "  ") + "\n")
}

// Handler serves what n runs, for a look from outside, with curl say:
//
//	/           the counts of the processes, and the trees of apps
//	/processes  the processes, the most behind first
//	/tree       the trees of apps
//
// ?format=json answers in JSON. The trees as text draw children alike as
// one line, see WriteTree; ?full draws each. apps may be nil.
func Handler(n *proc.Node, apps *application.Running) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		st := n.Stats()
		trees := treesOf(r.Context(), n, apps)
		if asJSON(r) {
			reply(w, map[string]any{"stats": st, "trees": trees})
			return
		}
		fmt.Fprintf(w, "node %s: %d processes; %d spawned, %d exited, %d crashed\n\n",
			n.Name(), st.Processes, st.Spawned, st.Exited, st.Crashed)
		writeTrees(w, trees, r.URL.Query().Has("full"))
	})
	mux.HandleFunc("GET /processes", func(w http.ResponseWriter, r *http.Request) {
		infos := Processes(n)
		if asJSON(r) {
			reply(w, infos)
			return
		}
		WriteProcesses(w, infos)
	})
	mux.HandleFunc("GET /tree", func(w http.ResponseWriter, r *http.Request) {
		trees := treesOf(r.Context(), n, apps)
		if asJSON(r) {
			reply(w, trees)
			return
		}
		writeTrees(w, trees, r.URL.Query().Has("full"))
	})
	return mux
}

// appTree is the tree of an application.
type appTree struct {
	App   string `json:"app"`
	Tree  Tree   `json:"tree"`
	Error string `json:"error,omitempty"`
}

func treesOf(ctx context.Context, n *proc.Node, apps *application.Running) []appTree {
	if apps == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var out []appTree
	for _, name := range apps.Apps() {
		top, _ := apps.Top(name)
		t, err := TreeOf(ctx, n, top)
		at := appTree{App: name, Tree: t}
		if err != nil {
			at.Error = err.Error()
		}
		out = append(out, at)
	}
	return out
}

func writeTrees(w io.Writer, trees []appTree, full bool) {
	for _, at := range trees {
		fmt.Fprintf(w, "application %s\n", at.App)
		if at.Error != "" {
			fmt.Fprintf(w, "  %s\n", at.Error)
		}
		writeTree(w, at.Tree, full)
		fmt.Fprintln(w)
	}
}

func asJSON(r *http.Request) bool { return r.URL.Query().Get("format") == "json" }

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
