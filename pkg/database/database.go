package database

import (
	"database/sql"
	"io"

	// Required plugin for database/sql
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

const (
	ENGINE_MYSQL    = "mysql"
	ENGINE_POSTGRES = "postgres"
)

type (
	// A Dumper writes a database's stucture to the provided stream.
	Dumper interface {
		DumpStructure(io.Writer) error
	}

	// An Anonymiser grabs a table's data and anonymises it, before sending it to a channel
	Anonymiser interface {
		AnonymiseRows(string, chan<- []*Cell, chan<- bool) error
	}

	// A Cell represents the value in a particular row and column
	Cell struct {
		Column string
		Value  interface{}
		Type   string
	}

	// Store provides an interface to access database stores.
	Store interface {
		GetTables() ([]string, error)
		GetTableStructure(string) (string, error)
		GetColumns(string) ([]string, error)
		GetPreamble() (string, error)
		Rows(string) (*sql.Rows, error)
	}
)

// Connect to a (for now) MySQL database with the provided DSN
func Connect(dsn string) (*sql.DB, error) {
	var connType string
	if strings.HasPrefix(strings.ToLower(dsn), "postgres://") {
		connType = ENGINE_POSTGRES
	} else {
		connType = ENGINE_MYSQL
	}

	return sql.Open(connType, dsn)
}
