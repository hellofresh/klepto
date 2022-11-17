package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/hellofresh/klepto/pkg/reader/engine"
)

const (
	baseTable = "BASE TABLE"
)

type (
	storage struct {
		conn *sql.DB
	}
)

// NewStorage creates a new mysql reader.
func NewStorage(conn *sql.DB, timeout time.Duration) reader.Reader {
	return engine.New(&storage{
		conn: conn,
	}, timeout)
}

// GetTables gets a list of all tables in the database.
func (s *storage) GetTables() ([]string, error) {
	log.Debug("fetching table list")

	rows, err := s.conn.Query("SHOW FULL TABLES")
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
		if tableType == baseTable {
			tables = append(tables, tableName)
		}
	}

	log.WithField("tables", tables).Debug("fetched table list")

	return tables, nil
}

// GetColumns returns the columns in the specified database table
func (s *storage) GetColumns(tableName string) ([]string, error) {
	rows, err := s.conn.Query(
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

// GetStructure dumps the mysql database structure.
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
	buf.WriteString("SET FOREIGN_KEY_CHECKS=0;\n")
	for _, tableName := range tables {
		var stmtTableName, tableStmt string
		err := s.conn.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", s.QuoteIdentifier(tableName))).Scan(&stmtTableName, &tableStmt)
		if err != nil {
			return "", err
		}

		buf.WriteString(tableStmt)
		buf.WriteString(";\n")
	}

	buf.WriteString("SET FOREIGN_KEY_CHECKS=1;")

	return buf.String(), nil
}

// QuoteIdentifier ...
func (s *storage) QuoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", strings.Replace(name, "`", "``", -1))
}

// Close closes the mysql database connection.
func (s *storage) Close() error {
	err := s.conn.Close()
	if err != nil {
		return fmt.Errorf("failed to close mysql reader database connection: %w", err)
	}
	return nil
}

// Conn retrieves the storage connection
func (s *storage) Conn() *sql.DB { return s.conn }

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

SET SQL_MODE = '%s';
SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

`
	var hostname string
	row := s.conn.QueryRow("SELECT @@hostname")
	if err := row.Scan(&hostname); err != nil {
		return "", err
	}

	var db string
	row = s.conn.QueryRow("SELECT DATABASE()")
	if err := row.Scan(&db); err != nil {
		return "", err
	}

	var sqlMode string
	row = s.conn.QueryRow("SELECT @@GLOBAL.SQL_MODE")
	if err := row.Scan(&sqlMode); err != nil {
		return "", err
	}

	return fmt.Sprintf(preamble, hostname, db, time.Now().Format(time.RFC1123Z), sqlMode), nil
}
