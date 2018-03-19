package dosa

import "time"

// NoTTL returns predefined identifier for not setting TTL
func NoTTL() time.Duration {
	return time.Duration(-1)
}
