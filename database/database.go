package database

import (
	"database/sql"
	"io"

	_ "github.com/go-sql-driver/mysql"
)

// A Dumper writes a database's stucture to the provided stream.
type Dumper interface {
	DumpStructure(io.Writer) error
}

// An Anonymiser grabs a table's data and anonymises it, before sending it to a channel
type Anonymiser interface {
	DumpTable(string) error
}

// A Cell represents the value in a particular row and column
type Cell struct {
	column string
	value  interface{}
}

// Connect to a (for now) MySQL database with the provided DSN
func Connect(dsn string) (*sql.DB, error) {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return conn, err
}
