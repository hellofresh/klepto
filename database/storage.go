package database

import (
	"database/sql"
	"fmt"
	"time"
)

// Store provides an interface to access data store referred to by fromDSN.
type Store interface {
	GetTables() ([]string, error)
	GetTableStructure(string) (string, error)
	GetColumns(string) ([]string, error)
	GetPreamble() (string, error)
	Rows(string) (*sql.Rows, error)
}

// Storage wraps db connection provided by fromDSN.
type Storage struct {
	conn *sql.DB
}

// CommonStore provides an interface to getTables()
type CommonStore interface {
	getTables() ([]string, error)
}

// IschemaStore provides an interface to access information_schema database.
// type IschemaStore interface {
// 	Relationships() ([]string, error)
// }

// CommonDBS ...
type CommonDBS struct {
	iSchemaConn *sql.DB
	fromDSNconn *sql.DB
}

// ForeignKeys . . .
type ForeignKeys struct {
	Fk      string
	Ctable  string
	Ccolumn string
	Ptable  string
	Pcol    string
}

// NewiSchemaStorage ...
func NewiSchemaStorage(iSchemaConn *sql.DB, fromDSNconn *sql.DB) *CommonDBS {
	return &CommonDBS{
		iSchemaConn: iSchemaConn,
		fromDSNconn: fromDSNconn,
	}
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
	hostname, err := hostname(s.conn)
	if err != nil {
		return preamble, err
	}
	db, err := database(s.conn)
	if err != nil {
		return preamble, err
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

// Relationships returns a list of all foreign key relationships for all tables.
func (c *CommonDBS) Relationships() ([]ForeignKeys, error) {

	// Get database name to which fromDSN(conn)ects
	db, err := database(c.fromDSNconn)
	if err != nil {
		return nil, err
	}

	foreignKeys := `SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME
	FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
	WHERE REFERENCED_TABLE_SCHEMA = "%s" AND REFERENCED_TABLE_NAME = "%s";`
	var relationships []ForeignKeys
	tables, _ := c.getTables()
	for _, table := range tables {

		rels, err := c.iSchemaConn.Query(fmt.Sprintf(foreignKeys, db, table))
		defer rels.Close()
		if err != nil {
			fmt.Printf("%v", err.Error())
			return []ForeignKeys{}, err
		}
		for rels.Next() {
			var tableName, columnName, fk, parentTable, parentCol []byte
			if err = rels.Scan(&tableName, &columnName, &fk, &parentTable, &parentCol); err != nil {
				return []ForeignKeys{}, err
			}
			relationships = append(relationships, ForeignKeys{
				Fk:      string(fk),
				Ctable:  string(tableName),
				Ccolumn: string(columnName),
				Ptable:  string(parentTable),
				Pcol:    string(parentCol),
			})
		}
	}
	fmt.Printf("%v", relationships[0])
	return relationships, nil
}

// Database returns the name of the database.
func database(c *sql.DB) (string, error) {
	var db string
	row := c.QueryRow("SELECT DATABASE()")
	err := row.Scan(&db)
	if err != nil {
		return "", err
	}
	return db, nil
}

// Hostname returns the hostname
func hostname(c *sql.DB) (string, error) {
	var hostname string
	row := c.QueryRow("SELECT @@hostname")
	if err := row.Scan(&hostname); err != nil {
		return "", err
	}
	return hostname, nil
}

// GetTables gets a list of all tables in the database
func (c *CommonDBS) getTables() (tables []string, err error) {
	tables = make([]string, 0)
	var rows *sql.Rows
	if rows, err = c.fromDSNconn.Query("SHOW FULL TABLES"); err != nil {
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
