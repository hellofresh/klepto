package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/malisit/kolpa"
	"github.com/spf13/viper"
)

// MySQLAnonymiser anonymises MySQL tables and
type MySQLAnonymiser struct {
	conn *sql.DB
}

// NewMySQLAnonymiser returns an initialised instance of MySQLAnonymiser
func NewMySQLAnonymiser(conn *sql.DB) *MySQLAnonymiser {
	return &MySQLAnonymiser{conn: conn}
}

// DumpTable grabs the data from the provided database table and runs Faker against some columns
func (a *MySQLAnonymiser) DumpTable(table string, rowChan chan<- []*Cell, endChan chan<- bool) error {
	rows, _ := a.conn.Query(fmt.Sprintf("SELECT * FROM `%s`", table))
	defer rows.Close()

	columns, _ := rows.Columns()

	for rows.Next() {
		row := make([]interface{}, len(columns))
		for idx := range columns {
			row[idx] = new(TypeScanner)
		}

		err := rows.Scan(row...)
		if err != nil {
			return err
		}

		k := kolpa.C()
		var cells []*Cell
		for idx, column := range columns {
			var cell *Cell
			replacement := a.shouldAnonymise(table, column)

			if replacement != "" {
				cell = &Cell{Column: column, Value: k.GenericGenerator(replacement)}
			} else {
				scanner := row[idx].(*TypeScanner)
				cell = &Cell{Column: column, Value: scanner.value}
			}

			cells = append(cells, cell)
		}

		rowChan <- cells
	}

	endChan <- true
	return nil
}

func (a *MySQLAnonymiser) shouldAnonymise(table, column string) string {
	return viper.GetString(fmt.Sprintf("anonymise.%s.%s", table, column))
}

// TypeScanner tries to determine the type of a provided value
type TypeScanner struct {
	valid bool
	value interface{}
}

func (scanner *TypeScanner) getBytes(src interface{}) []byte {
	if a, ok := src.([]uint8); ok {
		return a
	}
	return nil
}

// Scan accepts a value and attempts to determine its type
func (scanner *TypeScanner) Scan(src interface{}) error {
	switch src.(type) {
	case int64:
		if value, ok := src.(int64); ok {
			scanner.value = value
			scanner.valid = true
		}
	case float64:
		if value, ok := src.(float64); ok {
			scanner.value = value
			scanner.valid = true
		}
	case bool:
		if value, ok := src.(bool); ok {
			scanner.value = value
			scanner.valid = true
		}
	case string:
		value := scanner.getBytes(src)
		scanner.value = string(value)
		scanner.valid = true
	case []byte:
		value := scanner.getBytes(src)
		scanner.value = string(value)
		scanner.valid = true
	case time.Time:
		if value, ok := src.(time.Time); ok {
			scanner.value = value
			scanner.valid = true
		}
	case nil:
		scanner.value = nil
		scanner.valid = true
	}
	return nil
}
