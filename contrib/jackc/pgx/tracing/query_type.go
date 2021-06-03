package tracing

type QueryType string

const (
	QueryTypeQuery      QueryType = "Query"
	QueryTypeBegin                = "Begin"
	QueryTypeClose                = "Close"
	QueryTypeCommit               = "Commit"
	QueryTypeCopyFrom             = "CopyFrom"
	QueryTypeDeallocate           = "Deallocate"
	QueryTypeExec                 = "Exec"
	QueryTypePing                 = "Ping"
	QueryTypePrepare              = "Prepare"
	QueryTypeSendBatch            = "SendBatch"
)
