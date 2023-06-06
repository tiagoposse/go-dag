# go-dag

Simple implementation of a generic directed acyclic graph in golang. It's an iteration of https://github.com/natessilva/dag.

## Max number of parallel tasks

You can set an optional cap for the max number of parallel tasks at once, defaults to 20. Set to 0 to have no limit.