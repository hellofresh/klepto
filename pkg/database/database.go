package database

import (
	"database/sql"
	"io"

	// Required plugin for database/sql
	_ "github.com/go-sql-driver/mysql"
)

// A Dumper writes a database's stucture to the provided stream.
type Dumper interface {
	DumpStructure(io.Writer) error
}

// An Anonymiser grabs a table's data and anonymises it, before sending it to a channel
type Anonymiser interface {
	AnonymiseRows(string, chan<- []*Cell, chan<- bool) error
}

// A Cell represents the value in a particular row and column
type Cell struct {
	Column string
	Value  interface{}
	Type   string
}

// Connect to a (for now) MySQL database with the provided DSN
func Connect(dsn string) (*sql.DB, error) {
	conn, err := sql.Open("mysql", dsn+"?parseTime=true")
	if err != nil {
		return nil, err
	}
	return conn, err
}
