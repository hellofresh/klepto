package postgres

import (
	"database/sql"
	"strconv"
	"sync"

	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/hellofresh/klepto/pkg/reader/generic"
	log "github.com/sirupsen/logrus"
)

// Storage ...
type storage struct {
	PgDump

	connection *sql.DB
	cache      sync.Map
}

// NewStorage ...
func NewStorage(conn *sql.DB, dumper PgDump) reader.Reader {
	return generic.NewSqlReader(
		&storage{
			PgDump:     dumper,
			connection: conn,
		},
	)
}

func (s *storage) GetConnection() *sql.DB {
	return s.connection
}

// GetTables gets a list of all tables in the database
func (s *storage) GetTables() ([]string, error) {
	log.Debug("Fetching table list")
	rows, err := s.connection.Query(
		`SELECT table_name FROM information_schema.tables
		 WHERE table_catalog=current_database() AND table_schema NOT IN ('pg_catalog', 'information_schema')`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}

		tables = append(tables, tableName)
	}

	log.WithField("tables", tables).Debug("Fetched table list")

	return tables, nil
}

// GetColumns returns the columns in the specified database table
func (s *storage) GetColumns(table string) ([]string, error) {
	log.WithField("table", table).Debug("Fetching table columns")

	if c, ok := s.cache.Load(table); ok {
		if columns, ok := c.([]string); ok {
			return columns, nil
		}
	}

	rows, err := s.connection.Query(
		"SELECT column_name FROM information_schema.columns WHERE table_catalog=current_database() AND table_name=$1",
		table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}

		columns = append(columns, column)
	}

	s.cache.Store(table, columns)

	return columns, nil
}

func (s *storage) QuoteIdentifier(name string) string {
	return strconv.Quote(name)
}

func (s *storage) Close() error {
	return s.connection.Close()
}
