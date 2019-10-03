package routing

type Resolver interface {
  // Resolve maps a (scope, prefix) to a cluster name.
  Resolve(scope, namePrefix string) string
}
