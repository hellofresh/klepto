package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/hellofresh/klepto/pkg/reader/generic"
	log "github.com/sirupsen/logrus"
)

// Storage ...
type storage struct {
	connection *sql.DB
}

// NewStorage ...
func NewStorage(conn *sql.DB) reader.Reader {
	return generic.NewSqlReader(&storage{connection: conn})
}

func (s *storage) GetConnection() *sql.DB {
	return s.connection
}

// GetTables gets a list of all tables in the database
func (s *storage) GetTables() ([]string, error) {
	log.Debug("Fetching table list")

	rows, err := s.connection.Query("SHOW FULL TABLES")
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

	log.WithField("tables", tables).Debug("Fetched table list")

	return tables, nil
}

// GetColumns returns the columns in the specified database table
func (s *storage) GetColumns(tableName string) ([]string, error) {
	rows, err := s.connection.Query(
		"SELECT `column_name` FROM `information_schema`.`columns` WHERE table_schema=DATABASE() AND table_name=?",
		tableName,
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

// GetStructure returns the SQL used to create the database tables structure
func (s *storage) GetStructure() (string, error) {
	tables, err := s.GetTables()
	if err != nil {
		return "", err
	}

	preamble, err := s.getPreamble()
	if err != nil {
		return "", err
	}

	buf := bytes.NewBufferString(preamble)
	buf.WriteString("SET FOREIGN_KEY_CHECKS=0;")
	for _, tableName := range tables {
		var stmtTableName, tableStmt string
		err := s.connection.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", s.QuoteIdentifier(tableName))).Scan(&stmtTableName, &tableStmt)
		if err != nil {
			return "", err
		}

		buf.WriteString(tableStmt)
		buf.WriteString(";\n")
	}

	buf.WriteString("SET FOREIGN_KEY_CHECKS=1;")

	return buf.String(), nil
}

func (s *storage) QuoteIdentifier(name string) string {
	return "`" + strings.Replace(name, "`", "``", -1) + "`"
}

func (s *storage) Close() error {
	return s.connection.Close()
}

// getPreamble puts a big old comment at the top of the database dump.
// Also acts as first query to check for errors.
func (s *storage) getPreamble() (string, error) {
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
	row := s.connection.QueryRow("SELECT @@hostname")
	err := row.Scan(&hostname)
	if err != nil {
		return "", err
	}

	var db string
	row = s.connection.QueryRow("SELECT DATABASE()")
	err = row.Scan(&db)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(preamble, hostname, db, time.Now().Format(time.RFC1123Z)), nil
}
