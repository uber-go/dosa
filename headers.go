package dosa

import (
	"context"
)

type contextKey string

const _key = contextKey("extra-headers")

type rpcHeaders map[string]string

// AddHeader adds a header value to the context; this header will be added to RPC requests.
func AddHeader(ctx context.Context, name, value string) context.Context {
	var hdrs rpcHeaders
	if v := ctx.Value(_key); v == nil {
		hdrs = make(map[string]string)
		ctx = context.WithValue(ctx, _key, hdrs)
	} else {
		hdrs = v.(rpcHeaders)
	}
	hdrs[name] = value
	return ctx
}

// GetHeaders returns all the headers that have been set on this context.
func GetHeaders(ctx context.Context) map[string]string {
	var v interface{}
	if v = ctx.Value(_key); v == nil {
		return make(map[string]string)
	}
	return v.(rpcHeaders)
}
