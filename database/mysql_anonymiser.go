package database

import (
	"database/sql"
	"fmt"

	"github.com/hellofresh/klepto/utils"
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
			row[idx] = new(utils.TypeScanner)
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

			scanner := row[idx].(*utils.TypeScanner)

			if replacement != "" && scanner.Detected != "null" {
				cell = &Cell{Column: column, Type: scanner.Detected, Value: k.GenericGenerator(replacement)}
			} else {
				cell = &Cell{Column: column, Type: scanner.Detected, Value: scanner.Value}
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
