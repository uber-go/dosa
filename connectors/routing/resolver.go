package routing

// Resolver is an object that knows about the map from (scope,prefix) to cluster.
type Resolver interface {
	// Resolve maps a (scope, prefix) to a cluster name.
	Resolve(scope, namePrefix string) string
}
