package pgx

import "gopkg.in/jackc/pgx.v2"

// Identifier a PostgreSQL identifier or name. Identifiers can be composed of
// multiple parts such as ["schema", "table"] or ["table", "column"].
type Identifier pgx.Identifier

// CopyFromSource is the interface used by *Conn.CopyFrom as the source for copy data.
type CopyFromSource pgx.CopyFromSource

// CopyFromRows returns a CopyFromSource interface over the provided rows slice
// making it usable by *Conn.CopyFrom.
func CopyFromRows(rows [][]interface{}) CopyFromSource {
	return &copyFromRows{rows: rows, idx: -1}
}

// CommandTag is the result of an Exec function
type CommandTag pgx.CommandTag

type copyFromRows struct {
	rows [][]interface{}
	idx  int
}

func (ctr *copyFromRows) Next() bool {
	ctr.idx++
	return ctr.idx < len(ctr.rows)
}

func (ctr *copyFromRows) Values() ([]interface{}, error) {
	return ctr.rows[ctr.idx], nil
}

func (ctr *copyFromRows) Err() error {
	return nil
}

type FieldDescription struct {
	Name            string
	Table           Oid
	AttributeNumber int16
	DataType        Oid
	DataTypeSize    int16
	DataTypeName    string
	Modifier        int32
	FormatCode      int16
}

// Oid (Object Identifier Type) is, according to https://www.postgresql.org/docs/current/static/datatype-oid.html,
// used internally by PostgreSQL as a primary key for various system tables. It is currently implemented
// as an unsigned four-byte integer. Its definition can be found in src/include/postgres_ext.h
// in the PostgreSQL sources.
type Oid uint32
