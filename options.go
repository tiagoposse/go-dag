package dag

type Option func(dag *DAG)

func WithMaxParallel(max int) Option {
	return func(dag *DAG) {
		dag.maxParallel = max
	}
}

func WithDebugFunc(fn func(msg string)) Option {
	return func(dag *DAG) {
		dag.debugFn = fn
	}
}
