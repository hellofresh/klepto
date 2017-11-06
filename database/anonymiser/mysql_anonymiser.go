package anonymiser

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/hellofresh/klepto/database/seeder"

	"github.com/hellofresh/klepto/database"
	"github.com/hellofresh/klepto/utils"
	"github.com/spf13/viper"
)

// literalPrefix defines the constant we use to prefix literals
const literalPrefix = "literal:"

// MySQLAnonymiser anonymises MySQL tables and
type MySQLAnonymiser struct {
	conn *sql.DB
}

// MySQLSeeder describes mysql seeds
type MySQLSeeder struct {
	seeder.Seeder
}

type scanner struct {
	utils.TypeScanner
}

// NewMySQLAnonymiser returns an initialised instance of MySQLAnonymiser
func NewMySQLAnonymiser(conn *sql.DB) *MySQLAnonymiser {
	return &MySQLAnonymiser{conn: conn}
}

// DumpTable grabs the data from the provided database table and runs Faker against
// columns specified in config file.
func (a *MySQLAnonymiser) DumpTable(table string, rowChan chan<- []*database.Cell, endChan chan<- bool) error {
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
		var cells []*database.Cell
		for idx, column := range columns {
			var v interface{}
			fields[idx] = &v

			if replacement := readAnonymised(column, table); replacement != "" {
				cell, err := anonymise(column, replacement)
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
				nFields := doNotAnonymise(columns)
				if err := rows.Scan(nFields...); err != nil {
					return err
				}
				seed := reflect.ValueOf(nFields[idx]).Elem()
				cell, err := seeder.KeepSeedValueUnchanged(column, seed, reflect.TypeOf(seed).Kind())
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

// Anonymise replaces field value with one provided by faker.
// - If the replacement is a literal, then we simply place it in the Cell. No faking here.
// - If the replacement is not a literal, and it is an actual replacement then call the faker function
// that it maps on to.
// - We do not anonymise fields in columns that are not listed in config.
func anonymise(column, replacement string) (*database.Cell, error) {
	literal := strings.TrimPrefix(replacement, literalPrefix)
	scanner := new(scanner)

	if scanner.Scan(literal); len(literal) != len(replacement) {
		return &database.Cell{Column: column, Type: scanner.Detected, Value: literal}, nil
	}

	if replacement != "" && len(literal) == len(replacement) {
		if foundFaker, err := findFaker(replacement, fakers()); !foundFaker {
			return nil, err
		}

		value := (utils.Functions[replacement]).Call([]reflect.Value{})[0]
		return &database.Cell{Column: column, Type: scanner.Detected, Value: value}, nil
	}
	return nil, fmt.Errorf("couldn't anonymise cell with column: %v", column)
}

func readAnonymised(column, table string) string {
	return viper.GetString(fmt.Sprintf("anonymise.%s.%s", table, column))
}

// shouldNotAnonymise returns an []interface{} of pointers to fields. The number of fields
// is equal to the number of columns passed to it.
func doNotAnonymise(columns []string) []interface{} {
	fields := make([]interface{}, len(columns))
	for idx := range columns {
		var v interface{} // Normally, not a good idea to use interfaces to store values. But in this case, we do not quite know
		// what values to expect when we rows.Scan() from the database. interface{} can hold any type, so we use that.
		fields[idx] = &v
	}
	return fields
}

// FindsFaker finds faker function
func findFaker(f string, fakers []string) (bool, error) {
	for _, f2 := range fakers {
		if f2 == f {
			return true, nil
		}
	}
	return false, fmt.Errorf("couldn't find that faker: %v", f)
}

// Fakers collects all faker functions by their string names
func fakers() []string {
	fakers := make([]string, 0, len(utils.Functions))
	for k := range utils.Functions {
		fakers = append(fakers, k)
	}
	return fakers
}
