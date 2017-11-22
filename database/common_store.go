package database

import (
	"database/sql"
	"fmt"
)

// CommonStore provides an interface to getTables()
type CommonStore interface {
	getTables() ([]string, error)
}

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
