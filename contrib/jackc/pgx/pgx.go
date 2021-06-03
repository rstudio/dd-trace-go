package pgxtrace

import "context"

const (
	spanTagsKey contextKey = 0
)

type contextKey int

// ContextWithSpanTags sets arbitrary tags in the given context
// which will be extracted and included in each tracing span
// created by the wrapped logger.
func ContextWithSpanTags(ctx context.Context, tags map[string]string) context.Context {
	return context.WithValue(ctx, spanTagsKey, tags)
}
