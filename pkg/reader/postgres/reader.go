package postgres

import (
	"database/sql"

	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/hellofresh/klepto/pkg/reader/generic"
	log "github.com/sirupsen/logrus"
)

// Storage ...
type storage struct {
	generic.SqlReader

	tables []string
}

// NewStorage ...
func NewStorage(conn *sql.DB) reader.Reader {
	return &storage{
		SqlReader: generic.SqlReader{Connection: conn},
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

// GetTableStructure gets the CREATE TABLE statement of the specified database table
func (s *storage) GetTableStructure(table string) (stmt string, err error) {
	return
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
