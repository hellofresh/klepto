package database

import (
	"database/sql"
	"fmt"
	"time"
)

// MySQLDumper dumps a database's structure to a stram
type MySQLDumper struct {
	conn *sql.DB
}

// NewMySQLDumper is the constructor for MySQLDumper
func NewMySQLDumper(conn *sql.DB) *MySQLDumper {
	return &MySQLDumper{conn: conn}
}

// getPreamble puts a big old comment at the top of the database dump.
// Also acts as first query to check for errors.
func (d *MySQLDumper) getPreamble() (string, error) {
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
	row := d.conn.QueryRow("SELECT @@hostname")
	err := row.Scan(&hostname)
	if err != nil {
		return "", err
	}

	var database string
	row = d.conn.QueryRow("SELECT DATABASE()")
	err = row.Scan(&database)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(preamble, hostname, database, time.Now().Format(time.RFC1123Z)), nil
}

// GetTables gets a list of all tables in the database
func (d *MySQLDumper) GetTables() (tables []string, err error) {
	tables = make([]string, 0)
	var rows *sql.Rows
	if rows, err = d.conn.Query("SHOW FULL TABLES"); err != nil {
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

// getTableStructure gets the CREATE TABLE statement of the specified database table
func (d *MySQLDumper) getTableStructure(table string) (stmt string, err error) {
	row := d.conn.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table))
	var tableName string // We don't really care about this value but nevermind
	if err = row.Scan(&tableName, &stmt); err != nil {
		return "", err
	}

	return
}

// GetColumns returns the columns in the specified database table
func (d *MySQLDumper) GetColumns(table string) (columns []string, err error) {
	var rows *sql.Rows
	if rows, err = d.conn.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table)); err != nil {
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

// DumpStructure writes the database's structure to the provided stream
func (d *MySQLDumper) DumpStructure() (structure string, err error) {
	preamble, err := d.getPreamble()
	if err != nil {
		return
	}

	tables, err := d.GetTables()
	if err != nil {
		return
	}

	var tableStructure string
	for _, table := range tables {
		tableStructure, err = d.getTableStructure(table)
		if err != nil {
			return
		}
	}

	structure = fmt.Sprintf("%s\n%s;\n\n", preamble, tableStructure)
	return
}
