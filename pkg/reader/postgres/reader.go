package postgres

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/hellofresh/klepto/pkg/reader/engine"
)

type (
	storage struct {
		PgDumper
		conn *sql.DB
	}

	// PgDumper executes the pg dump command.
	PgDumper interface {
		GetStructure() (stmt string, err error)
	}
)

// NewStorage creates a new postgres storage reader.
func NewStorage(conn *sql.DB, dumper PgDumper, timeout time.Duration) reader.Reader {
	return engine.New(&storage{
		PgDumper: dumper,
		conn:     conn,
	}, timeout)
}

// removeQuotesFromTableName this functions address a issue cause by have tables in camel case format
func removeQuotesFromTableName(table string) string {
	return strings.ReplaceAll(table, "\"", "")
}

// GetTables gets a list of all tables in the database
func (s *storage) GetTables() ([]string, error) {
	log.Debug("fetching table list")
	rows, err := s.conn.Query(
		`SELECT table_name FROM information_schema.tables
		 WHERE table_catalog=current_database()
		 AND table_type = 'BASE TABLE'
		 AND table_schema NOT IN ('pg_catalog', 'information_schema')`,
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

		tableName = removeQuotesFromTableName(tableName)
		tables = append(tables, tableName)
	}

	log.WithField("tables", tables).Debug("fetched table list")

	return tables, nil
}

func (s *storage) GetColumns(table string) ([]string, error) {
	log.WithField("table", table).Debug("fetching table columns")
	rows, err := s.conn.Query(
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

	return columns, nil
}

// QuoteIdentifier returns a double-quoted name.
func (s *storage) QuoteIdentifier(name string) string {
	return strconv.Quote(name)
}

// Close closes the postgres connection reader.
func (s *storage) Close() error {
	if err := s.conn.Close(); err != nil {
		return fmt.Errorf("failed to close postgres connection reader: %w", err)
	}
	return nil
}

// Conn retrieves the postgres reader connection.
func (s *storage) Conn() *sql.DB { return s.conn }
