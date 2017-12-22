package anonymiser

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/spf13/viper"
)

// literalPrefix defines the constant we use to prefix literals
const literalPrefix = "literal:"

// anonymiser anonymises MySQL tables
type anonymiser struct {
	reader.Reader
}

// NewAnonymiser returns an initialised instance of MySQLAnonymiser
func NewAnonymiser(source reader.Reader) reader.Reader {
	return &anonymiser{source}
}

func (a *anonymiser) ReadTable(table string, rowChan chan<- *database.Row) error {
	// Find all columns that need to be anonimized
	columnFakers, err := a.fetchColumnToAnonimise(table)
	if err != nil {
		return err
	}

	// If there is nothing to fake don't even try
	if len(columnFakers) == 0 {
		return a.Reader.ReadTable(table, rowChan)
	}

	// Create read/write chanel
	rawChan := make(chan *database.Row)

	// Read from the reader
	go a.Reader.ReadTable(table, rawChan)

	// Anonimise the rows
	for {
		row := <-rawChan
		if row == nil {
			rowChan <- nil
			break
		}

		actualRow := *row
		for column, fakerType := range columnFakers {
			// TODO how do we handle errors?
			a.anonymiseCell(actualRow[column], fakerType)
		}

		rowChan <- row
	}

	return nil
}

func (a *anonymiser) fetchColumnToAnonimise(table string) (map[string]string, error) {
	columns, err := a.GetColumns(table)
	if err != nil {
		return nil, err
	}
	columnFakers := make(map[string]string, len(columns))
	for _, column := range columns {
		columnFakeType := readAnonymised(column, table)
		if columnFakeType == "" {
			continue
		}

		columnFakers[column] = columnFakeType
	}

	return columnFakers, nil
}

func (a *anonymiser) anonymiseCell(cell *database.Cell, fakerType string) error {
	// If we have a literal replacement then use it
	if strings.HasPrefix(fakerType, literalPrefix) {
		cell.Value = strings.TrimPrefix(fakerType, literalPrefix)
		return nil
	}

	// Find the faker type
	for name, faker := range Functions {
		if fakerType != name {
			continue
		}

		cell.Value = faker.Call([]reflect.Value{})[0]
		return nil
	}

	return fmt.Errorf("couldn't find that faker: %v", fakerType)
}

func readAnonymised(column, table string) string {
	return viper.GetString(fmt.Sprintf("anonymise.%s.%s", table, column))
}
