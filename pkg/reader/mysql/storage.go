package mysql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/hellofresh/klepto/pkg/reader/generic"
)

// Storage ...
type storage struct {
	conn *sql.DB
}

// NewStorage ...
func NewStorage(conn *sql.DB) reader.Reader {
	return &storage{conn: conn}
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
	row := s.conn.QueryRow("SELECT @@hostname")
	err := row.Scan(&hostname)
	if err != nil {
		return "", err
	}

	var db string
	row = s.conn.QueryRow("SELECT DATABASE()")
	err = row.Scan(&db)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(preamble, hostname, db, time.Now().Format(time.RFC1123Z)), nil
}

// GetTables gets a list of all tables in the database
func (s *storage) GetTables() (tables []string, err error) {
	tables = make([]string, 0)
	var rows *sql.Rows
	if rows, err = s.conn.Query("SHOW FULL TABLES"); err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, tableType string
		if err = rows.Scan(&tableName, &tableType); err != nil {
			return
		}
		if tableType == "BASE TABLE" {
			tables = append(tables, tableName)
		}
	}
	return
}

// GetColumns returns the columns in the specified database table
func (s *storage) GetColumns(table string) (columns []string, err error) {
	var rows *sql.Rows
	if rows, err = s.conn.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table)); err != nil {
		return
	}
	defer rows.Close()

	if columns, err = rows.Columns(); err != nil {
		return
	}

	for k, column := range columns {
		columns[k] = fmt.Sprintf("`%s`", column)
	}
	return
}

// GetTableStructure gets the CREATE TABLE statement of the specified database table
func (s *storage) GetTableStructure(table string) (stmt string, err error) {
	// We don't really care about this value but nevermind
	var tableName string
	err = s.conn.
		QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table)).
		Scan(&tableName, &stmt)

	return
}

// Rows returns a list of all rows in a table
func (s *storage) ReadTable(table string, rowChan chan<- *database.Row) error {
	rows, err := s.conn.Query(fmt.Sprintf("SELECT * FROM `%s`", table))
	if err != nil {
		return err
	}

	return generic.PublishRows(rows, rowChan)
}
