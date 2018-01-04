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
	generic.SqlReader

	tables []string
}

// NewStorage ...
func NewStorage(conn *sql.DB) reader.Reader {
	return &storage{
		SqlReader: generic.SqlReader{
			Connection: conn,
			QuoteIdentifier: func(name string) string {
				return "`" + strings.Replace(name, "`", "``", -1) + "`"
			},
		},
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

// GetStructure returns the SQL used to create the database tables structure
func (s *storage) GetStructure() (string, error) {
	tables, err := s.GetTables()
	if err != nil {
		return "", err
	}

	buf := bytes.NewBufferString("SET FOREIGN_KEY_CHECKS=0;")
	for _, tableName := range tables {
		var stmtTableName, tableStmt string
		err := s.Connection.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", s.QuoteIdentifier(tableName))).Scan(&stmtTableName, &tableStmt)
		if err != nil {
			return "", err
		}

		buf.WriteString(tableStmt)
		buf.WriteString(";\n")
	}

	buf.WriteString("SET FOREIGN_KEY_CHECKS=1;")

	return buf.String(), nil
}
