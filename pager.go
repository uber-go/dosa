package dosa

import (
	"fmt"
	"io"
)

type pager struct {
	limit        int
	token        string
	fieldsToRead []string
}

func addLimitTokenString(w io.Writer, limit int, token string) {
	if limit > 0 {
		fmt.Fprintf(w, " limit %d", limit)
	}
	if token != "" {
		fmt.Fprintf(w, " token %q", token)
	}
}

func (p pager) equals(p2 pager) bool {
	if len(p.fieldsToRead) != len(p2.fieldsToRead) {
		return false
	}

	for i, field := range p.fieldsToRead {
		if p2.fieldsToRead[i] != field {
			return false
		}
	}

	return p.limit == p2.limit && p.token == p2.token
}
