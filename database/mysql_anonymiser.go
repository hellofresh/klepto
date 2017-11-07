package database

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/hellofresh/klepto/utils"
	"github.com/spf13/viper"
)

// LiteralPrefix defines the constant we use to prefix literals
const LiteralPrefix = "literal:"

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
				cell, err := a.AnonymiseCell(column, replacement)
				if err != nil {
					return err
				}
				cells = append(cells, cell)
			} else {
				// TODO: Make this approach more efficient.
				// Currently not very efficient because in each loop,
				// we create len(columns) number of interfaces so that rows.Scan() will not complain
				// about not having enough params. in reality, we only need
				// len(columns) - numberOfAnonymisedFields interfaces
				nFields := a.ShouldNotAnonymise(columns)
				if err := rows.Scan(nFields...); err != nil {
					return err
				}
				seed := reflect.ValueOf(nFields[idx]).Elem()
				cell, err := a.KeepsSeedValueUnchanged(column, seed, reflect.TypeOf(seed).Kind())
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

// AnonymiseCell replaces field value with one provided by faker.
// - If the replacement is a literal, then we simply place it in the Cell. No faking here.
// - If the replacement is not a literal, and it is an actual replacement then call the faker function
// that it maps on to.
// - We do not anonymise fields in columns that are not listed in config.
func (a *MySQLAnonymiser) AnonymiseCell(column, replacement string) (*Cell, error) {
	literal := strings.TrimPrefix(replacement, LiteralPrefix)

	if a.TypeScanner.Scan(literal); len(literal) != len(replacement) {
		return &Cell{Column: column, Type: a.Detected, Value: literal}, nil
	}

	if replacement != "" && len(literal) == len(replacement) {
		if foundFaker, err := a.FindsFaker(replacement, a.Fakers()); !foundFaker {
			return nil, err
		}

		value := (utils.Functions[replacement]).Call([]reflect.Value{})[0]
		return &Cell{Column: column, Type: a.Detected, Value: value}, nil
	}
	return nil, fmt.Errorf("couldn't anonymise cell with column: %v", column)
}

func (a *MySQLAnonymiser) shouldAnonymise(table, column string) string {
	return viper.GetString(fmt.Sprintf("anonymise.%s.%s", table, column))
}

// KeepsSeedValueUnchanged keeps primary key or any other non-anonymous fields unchanged.
func (a *MySQLAnonymiser) KeepsSeedValueUnchanged(column string, value, typ interface{}) (*Cell, error) {
	kind := fmt.Sprintf("%s", reflect.TypeOf(value).Kind())
	cell := &Cell{Column: column, Value: value, Type: kind}
	if cell.Type != "" {
		return cell, nil
	}
	return nil, fmt.Errorf("couldn't keep cell value unchaged for column: %v", column)
}

// ShouldNotAnonymise returns an []interface{} of pointers to fields. The number of fields
// is equal to the number of columns passed to it.
func (a *MySQLAnonymiser) ShouldNotAnonymise(columns []string) []interface{} {
	fields := make([]interface{}, len(columns))
	for idx := range columns {
		var v interface{} // Normally, not a good idea to use interfaces to store values. But in this case, we do not quite know
		// what values to expect when we rows.Scan() from the database. interface{} can hold any type, so we use that.
		fields[idx] = &v
	}
	return fields
}

// FindsFaker finds faker function
func (a *MySQLAnonymiser) FindsFaker(f string, fakers []string) (bool, error) {
	for _, f2 := range fakers {
		if f2 == f {
			return true, nil
		}
	}
	return false, fmt.Errorf("couldn't find that faker: %v", f)
}

// Fakers collects all faker functions by their string names
func (a *MySQLAnonymiser) Fakers() []string {
	fakers := make([]string, 0, len(utils.Functions))
	for k := range utils.Functions {
		fakers = append(fakers, k)
	}
	return fakers
}
