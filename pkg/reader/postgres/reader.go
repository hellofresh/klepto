package postgres

import (
	"database/sql"
	"strconv"

	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/hellofresh/klepto/pkg/reader/generic"
	log "github.com/sirupsen/logrus"
)

// Storage ...
type storage struct {
	generic.SqlReader

	PgDump

	// tables is a cache variable for all tables in the db
	tables []string
	// columns is a cache variable for tables and there columns in the db
	columns map[string][]string
}

// NewStorage ...
func NewStorage(conn *sql.DB, dumper PgDump) reader.Reader {
	return &storage{
		PgDump: dumper,
		SqlReader: generic.SqlReader{
			Connection:      conn,
			QuoteIdentifier: strconv.Quote,
		},
		columns: map[string][]string{},
	}
}

// GetPreamble puts a big old comment at the top of the database dump.
// Also acts as first query to check for errors.
func (s *storage) GetPreamble() (string, error) {
	return `# *******************************
# This database was nicked by Klepto™.
#
# https://github.com/hellofresh/klepto
# Host: %s
# Database: %s
# Dumped at: %s
# *******************************

SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;
`, nil
}

// GetTables gets a list of all tables in the database
func (s *storage) GetTables() ([]string, error) {
	if s.tables == nil {
		log.Info("Fetching table list")
		rows, err := s.Connection.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
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

		s.tables = tables
		log.WithField("tables", tables).Debug("Fetched table list")
	}

	return s.tables, nil
}

// GetColumns returns the columns in the specified database table
func (s *storage) GetColumns(table string) ([]string, error) {
	columns, ok := s.columns[table]
	if ok {
		return columns, nil
	}

	log.WithField("table", table).Info("Fetching table columns")
	rows, err := s.Connection.Query(
		"select column_name from information_schema.columns where table_name=$1",
		table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns = []string{}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}

		columns = append(columns, column)
	}

	s.columns[table] = columns
	return columns, nil
}
