package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/hellofresh/klepto/pkg/database"
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
	preamble := `# *******************************
# This database was nicked by Klepto™.
#
# https://github.com/hellofresh/klepto
# Host: %s
# Database: %s
# Dumped at: %s
# *******************************

SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

`
	var hostname string
	row := s.Connection.QueryRow("SELECT @@hostname")
	err := row.Scan(&hostname)
	if err != nil {
		return "", err
	}

	var db string
	row = s.Connection.QueryRow("SELECT DATABASE()")
	err = row.Scan(&db)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(preamble, hostname, db, time.Now().Format(time.RFC1123Z)), nil
}

// GetStructure returns the SQL used to create the database tables structure
func (s *storage) GetStructure() (string, error) {
	tables, err := s.GetTables()
	if err != nil {
		return "", err
	}

	buf := bytes.NewBufferString("")
	for _, tableName := range tables {
		var stmtTableName, tableStmt string
		err := s.Connection.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)).Scan(&stmtTableName, &tableStmt)
		if err != nil {
			return "", err
		}

		buf.WriteString(tableStmt)
	}

	return buf.String(), nil
}

// GetTables gets a list of all tables in the database
func (s *storage) GetTables() ([]string, error) {
	if s.tables == nil {
		log.Info("Fetching table list")

		rows, err := s.Connection.Query("SHOW FULL TABLES")
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		tables := make([]string, 0)
		for rows.Next() {
			var tableName, tableType string
			if err := rows.Scan(&tableName, &tableType); err != nil {
				return nil, err
			}
			if tableType == "BASE TABLE" {
				tables = append(tables, tableName)
			}
		}

		s.tables = tables
		log.WithField("tables", tables).Debug("Fetched table list")
	}

	return s.tables, nil
}

// GetColumns returns the columns in the specified database table
func (s *storage) GetColumns(table string) ([]string, error) {
	// TODO fix since it fails for empty tables
	rows, err := s.Connection.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", s.quoteIdentifier(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for k, column := range columns {
		columns[k] = fmt.Sprintf("%s", column)
	}
	return columns, nil
}

// ReadTable returns a list of all rows in a table
func (s *storage) ReadTable(table string, rowChan chan<- database.Row) error {
	query := fmt.Sprintf("SELECT * FROM %s", s.quoteIdentifier(table))

	log.WithFields(log.Fields{
		"table": table,
		"query": query,
	}).Info("Fetching rows")
	rows, err := s.Connection.Query(query)
	if err != nil {
		return err
	}

	return s.PublishRows(rows, rowChan)
}

func (s *storage) quoteIdentifier(name string) string {
	return "`" + strings.Replace(name, "`", "``", -1) + "`"
}
