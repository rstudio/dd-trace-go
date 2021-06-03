package pgxtrace

import (
	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
)

type closedErrRows struct {
	err error
}

func (cer *closedErrRows) Close()                                         {}
func (cer *closedErrRows) CommandTag() pgconn.CommandTag                  { return nil }
func (cer *closedErrRows) Err() error                                     { return cer.err }
func (cer *closedErrRows) FieldDescriptions() []pgproto3.FieldDescription { return nil }
func (cer *closedErrRows) Next() bool                                     { return false }
func (cer *closedErrRows) RawValues() [][]byte                            { return nil }
func (cer *closedErrRows) Scan(...interface{}) error                      { return cer.err }
func (cer *closedErrRows) Values() ([]interface{}, error)                 { return nil, cer.err }
