package main

import (
	"fmt"
	"os"
	"strings"
)

// Returns the set of headers required for auth: currently X-Uber-Source and X-Auth-Params-Email. The
// callerName argument is used as X-Uber-Source.
func getAuthHeaders(callerName string) map[string]string {
	return map[string]string{
		"X-Uber-Source":       callerName,
		"X-Auth-Params-Email": fmt.Sprintf("%s@uber.com", strings.ToLower(os.Getenv("USER"))),
	}
}
