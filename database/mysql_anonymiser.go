package database

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/hellofresh/klepto/utils"
	"github.com/spf13/viper"
)

const literalPrefix = "literal:"

// MySQLAnonymiser anonymises MySQL tables and
type MySQLAnonymiser struct {
	utils.TypeScanner
	conn *sql.DB
}

// NewMySQLAnonymiser returns an initialised instance of MySQLAnonymiser
func NewMySQLAnonymiser(conn *sql.DB) *MySQLAnonymiser {
	return &MySQLAnonymiser{conn: conn}
}

// DumpTable grabs the data from the provided database table and runs Faker against
// columns specified in config file.
func (a *MySQLAnonymiser) DumpTable(table string, rowChan chan<- []*Cell, endChan chan<- bool) error {
	rows, err := a.conn.Query(fmt.Sprintf("SELECT * FROM `%s`", table))
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	for rows.Next() {
		fields := make([]interface{}, len(columns))
		var cells []*Cell
		for idx, column := range columns {
			var v interface{}
			fields[idx] = &v

			if replacement := a.shouldAnonymise(table, column); replacement != "" {
				cell, err := a.anonymiseCell(column, replacement)
				if err != nil {
					return err
				}
				cells = append(cells, cell)
			} else {
				log.Printf("Not marked for anonymising. Skipping cell in column: %v", column)
				// TODO: Make this approach more efficient.
				// Currently not very efficient because in each loop,
				// we create len(columns) number of interfaces so that rows.Scan() will not complain
				// about not having enough params. in reality, we only need
				// len(columns) - numberOfAnonymisedFields interfaces
				nFields := a.shouldNotAnonymise(columns)
				if err := rows.Scan(nFields...); err != nil {
					return err
				}
				seed := reflect.ValueOf(nFields[idx]).Elem()
				cell, err := a.keepsSeedValueUnchanged(column, seed, reflect.TypeOf(seed).Kind())
				if err != nil {
					return err
				}
				cells = append(cells, cell)
			}
		}

		if err := rows.Scan(fields...); err != nil {
			return err
		}

		rowChan <- cells
	}

	endChan <- true
	return nil
}

// - If the replacement is a literal, then we simply place it in the Cell. No faking here.
// - If the replacement is not a literal, and it is an actual replacement then call the faker function
// that it maps on to.
// - We do not anonymise fields in columns that are not listed in config.
func (a *MySQLAnonymiser) anonymiseCell(column, replacement string) (*Cell, error) {
	literal := strings.TrimPrefix(replacement, literalPrefix)

	if a.TypeScanner.Scan(literal); len(literal) != len(replacement) {
		return &Cell{Column: column, Type: a.Detected, Value: literal}, nil
	}

	if replacement != "" && len(literal) == len(replacement) {
		value := (utils.Functions[replacement]).Call([]reflect.Value{})[0]
		return &Cell{Column: column, Type: a.Detected, Value: value}, nil
	}
	return nil, fmt.Errorf("couldn't anonymise cell with column: %v", column)
}

func (a *MySQLAnonymiser) shouldAnonymise(table, column string) string {
	return viper.GetString(fmt.Sprintf("anonymise.%s.%s", table, column))
}

// Useful for keeping primary key or any other non-anonymous fields unchanged.
func (a *MySQLAnonymiser) keepsSeedValueUnchanged(column string, value interface{}, typ interface{}) (*Cell, error) {
	kind := fmt.Sprintf("%s", reflect.TypeOf(value).Kind())
	cell := &Cell{Column: column, Value: value, Type: kind}
	if cell.Type != "" {
		return cell, nil
	}
	return nil, fmt.Errorf("couldn't keep cell value unchaged for column: %v", column)
}

func (a *MySQLAnonymiser) shouldNotAnonymise(columns []string) []interface{} {
	fields := make([]interface{}, len(columns))
	for idx, _ := range columns {
		var v interface{}
		fields[idx] = &v
	}
	return fields
}
