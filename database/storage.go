package database

import (
	"database/sql"
	"fmt"
	"time"
)

// Store provides an interface to access database stores.
type Store interface {
	GetTables() ([]string, error)
	GetTableStructure(string) (string, error)
	GetColumns(string) ([]string, error)
	GetPreamble() (string, error)
	Rows(string) (*sql.Rows, error)
}

// Storage ...
type Storage struct {
	conn *sql.DB
}

// NewStorage ...
func NewStorage(conn *sql.DB) *Storage {
	return &Storage{conn: conn}
}

// GetPreamble puts a big old comment at the top of the database dump.
// Also acts as first query to check for errors.
func (s *Storage) GetPreamble() (string, error) {
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
func (s *Storage) GetTables() (tables []string, err error) {
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
func (s *Storage) GetColumns(table string) (columns []string, err error) {
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
func (s *Storage) GetTableStructure(table string) (stmt string, err error) {
	// We don't really care about this value but nevermind
	var tableName string
	err = s.conn.
		QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table)).
		Scan(&tableName, &stmt)

	return
}

// Rows returns a list of all rows in a table
func (s *Storage) Rows(table string) (*sql.Rows, error) {
	rows, err := s.conn.Query(fmt.Sprintf("SELECT * FROM `%s`", table))
	if err != nil {
		return rows, err
	}
	return rows, nil
}
