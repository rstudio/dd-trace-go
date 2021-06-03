package tracing

type QueryType string

const (
	QueryTypeBegin      QueryType = "Begin"
	QueryTypeClose                = "Close"
	QueryTypeCommit               = "Commit"
	QueryTypeCopyFrom             = "CopyFrom"
	QueryTypeDeallocate           = "Deallocate"
	QueryTypeExec                 = "Exec"
	QueryTypePing                 = "Ping"
	QueryTypePrepare              = "Prepare"
	QueryTypeQuery                = "Query"
	QueryTypeRollback             = "Rollback"
	QueryTypeSendBatch            = "SendBatch"
)
