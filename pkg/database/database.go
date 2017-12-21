package database

import (
	"database/sql"
	"io"

	// Required plugin for database/sql
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
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
	var connType string
	if strings.HasPrefix(strings.ToLower(dsn), "postgres://") {
		connType = "postgres"
	} else {
		connType = "mysql"
	}

	return sql.Open(connType, dsn)
}
