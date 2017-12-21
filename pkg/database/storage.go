package database

import (
	"database/sql"
	"fmt"
	"time"
)

// Storage ...
type storage struct {
	conn *sql.DB
}

// NewStorage ...
func NewStorage(conn *sql.DB) Store {
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
	hostname, err := s.hostname()
	if err != nil {
		return "", err
	}
	db, err := s.database()
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

// GetRows returns rows. If primary_record_type has been
// set and a number is given, return only those
// number of rows.
func (s *Storage) GetRows(table string) (*sql.Rows, error) {
	n, err := s.config.ReadPrimaryRecordLimit()
	if err != nil {
		return nil, err
	}
	if n != "" {
		return s.nRows(table, n)
	}
	return s.rows(table)

}

// Rows returns a list of all rows in a table
func (s *storage) Rows(table string) (*sql.Rows, error) {
	rows, err := s.conn.Query(fmt.Sprintf("SELECT * FROM `%s`", table))
	if err != nil {
		return rows, err
	}
	return rows, nil
}

func (s *Storage) nRows(table string, n string) (*sql.Rows, error) {
	column, err := s.primaryColumn(table)
	if err != nil {
		return nil, err
	}
	nRows, err := s.conn.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY %s DESC LIMIT ?", table, column), n)
	if err != nil {
		return nRows, err
	}
	return nRows, nil
}

// database returns the name of the database.
func (s *Storage) database() (string, error) {
	row := s.conn.QueryRow("SELECT DATABASE()")
	var db string
	err := row.Scan(&db)
	if err != nil {
		return "", err
	}
	return db, nil
}

// hostname returns the hostname
func (s *Storage) hostname() (string, error) {
	row := s.conn.QueryRow("SELECT @@hostname")
	var hostname string
	err := row.Scan(&hostname)
	if err != nil {
		return "", err
	}
	return hostname, nil
}

func (s *Storage) primaryColumn(table string) (string, error) {
	q := `SELECT COLUMN_NAME
FROM information_schema.COLUMNS
WHERE (TABLE_SCHEMA = ?)
AND (TABLE_NAME = ?)
AND (COLUMN_KEY = "PRI")`
	dbName, err := s.database()
	if err != nil {
		return "", fmt.Errorf("Could not get database name")
	}
	row := s.conn.QueryRow(q, dbName, table)
	var priKeyColName string
	serr := row.Scan(&priKeyColName)
	if serr != nil {
		return "", fmt.Errorf("Could not get the primary key column name: %v", serr)
	}
	return priKeyColName, nil
}
