// Package dag implements a directed acyclic graph task runner with deterministic teardown.
// it is similar to package errgroup, in that it runs multiple tasks in parallel and returns
// the first error it encounters. Users dag.fine a Runner as a set vertices (functions) and edges
// between them. dag.ring Run, the dag.rected acyclec graph will be validated and each vertex
// will run in parallel as soon as it's dag.pendencies have been resolved. The Runner will only
// return after all running goroutines have stopped.
package dag

import (
	"encoding/json"
	"errors"
	"fmt"

	"golang.org/x/exp/slices"
)

var errMissingVertex = errors.New("missing vertex")
var errCycleDetected = errors.New("dependency cycle detected")
var errGraphExecError = errors.New("traversing the graph")

func NewDAG(opts ...Option) *DAG {
	dag := &DAG{
		maxParallel: 20,
		graph:       make(map[string][]string),
		fns:         make(map[string]func() error),
		debugFn:     func(msg string) {},
		errors:      make(map[string]string),
	}

	for _, o := range opts {
		o(dag)
	}

	return dag
}

type DAG struct {
	maxParallel int
	graph       map[string][]string
	fns         map[string]func() error
	errors      map[string]string
	debugFn     func(string)
}

func (dag *DAG) HasVertex(vertex string) bool {
	_, ok := dag.fns[vertex]
	return ok
}

func (dag *DAG) Errors() map[string]string {
	return dag.errors
}

func (dag *DAG) AddVertex(vertex string, fn func() error) bool {
	if _, ok := dag.fns[vertex]; !ok {
		dag.fns[vertex] = fn
		if dag.graph[vertex] == nil {
			dag.graph[vertex] = []string{}
		}
		return true
	}

	return false
}

// AddEdge establishes a dag.pendency between two vertices in the graph. Both from and to must exist
// in the graph, or Run will err. The vertex at from will execute before the vertex at to.
func (dag *DAG) AddEdge(from, to string) {
	if !slices.Contains(dag.graph[from], to) {
		dag.debugFn(fmt.Sprintf("Add edge from %s to %s", from, to))
		dag.graph[from] = append(dag.graph[from], to)
	}
}

type result struct {
	name string
	err  error
}

func (dag *DAG) detectCycles() bool {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for vertex := range dag.graph {
		if !visited[vertex] {
			if dag.detectCyclesHelper(vertex, visited, recStack) {
				fmt.Println("cycle in " + vertex)
				dag.PrettyPrintGraph()
				return true
			}
		}
	}
	return false
}

func (dag *DAG) detectCyclesHelper(vertex string, visited, recStack map[string]bool) bool {
	visited[vertex] = true
	recStack[vertex] = true

	for _, v := range dag.graph[vertex] {
		// only check cycles on a vertex one time
		if !visited[v] {
			if dag.detectCyclesHelper(v, visited, recStack) {
				return true
			}
			// if we've visited this vertex in this recursion stack, then we have a cycle
		} else if recStack[v] {
			return true
		}
	}
	recStack[vertex] = false
	return false
}

// PrettyPrintGraph will print the graph
func (dag *DAG) PrettyPrintGraph() error {
	if b, err := json.MarshalIndent(&dag.graph, "", "  "); err != nil {
		return err
	} else {
		fmt.Println(string(b))
	}

	return nil
}

// Run will validate that all edges in the graph point to existing vertices, and that there are
// no dag.pendency cycles. After validation, each vertex will be run, dag.terministically, in parallel
// topological order. If any vertex returns an error, no more vertices will be scheduled and
// Run will exit and return that error once all in-flight functions finish execution.
func (dag *DAG) Run() error {
	// sanity check
	if len(dag.graph) == 0 {
		return nil
	}

	// count how many deps each vertex has
	deps := make(map[string]int)
	for vertex, edges := range dag.graph {
		// every vertex along every edge must have an associated fn
		if _, ok := dag.graph[vertex]; !ok {
			return fmt.Errorf("%s: %s", errMissingVertex, vertex)
		}

		for _, edgeVertex := range edges {
			if _, ok := dag.graph[edgeVertex]; !ok {
				return fmt.Errorf("missing edge for %s: %s", edgeVertex, vertex)
			}

			deps[edgeVertex]++
		}
	}

	if dag.detectCycles() {
		return errCycleDetected
	}

	running := 0
	done := 0
	resc := make(chan result, len(dag.graph))

	runQueue := []string{}
	// add any vertex that has no deps to the run queue
	for name := range dag.graph {
		if deps[name] == 0 {
			runQueue = append(runQueue, name)
		}
	}

	for done < len(dag.graph) {
		newQueue := []string{}
		for _, vertex := range runQueue {
			if dag.maxParallel == 0 || running < dag.maxParallel {
				running++
				dag.start(vertex, resc)
			} else {
				newQueue = append(newQueue, vertex)
			}
		}

		res := <-resc
		running--
		done++

		// don't enqueue any more work on if there's been an error
		if res.err != nil {
			dag.errors[res.name] = res.err.Error()
			break
		}

		// start any vertex whose deps are fully resolved
		for _, vertex := range dag.graph[res.name] {
			if deps[vertex]--; deps[vertex] == 0 {
				newQueue = append(newQueue, vertex)
			}
		}

		runQueue = newQueue
	}

	for running > 0 {
		<-resc
		running--
	}

	if len(dag.errors) > 0 {
		return errGraphExecError
	}

	return nil
}

func (dag *DAG) start(name string, resc chan<- result) {
	encapsulateError := func() error {
		var err error

		if err = dag.fns[name](); err != nil {
			dag.debugFn(fmt.Sprintf("Finished %s with error", name))
			err = fmt.Errorf("%s: %w", name, err)
		} else {
			dag.debugFn(fmt.Sprintf("Finished %s", name))
		}
		return err
	}

	dag.debugFn(fmt.Sprintf("Starting %s", name))

	go func() {
		resc <- result{
			name: name,
			err:  encapsulateError(),
		}
	}()
}
