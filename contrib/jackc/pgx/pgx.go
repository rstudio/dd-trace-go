package pgxtrace

import (
	"gopkg.in/DataDog/dd-trace-go.v1/contrib/jackc/pgx/tracing"
)

var ContextWithSpanTags = tracing.ContextWithSpanTags
