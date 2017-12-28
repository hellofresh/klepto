package anonymiser

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	log "github.com/sirupsen/logrus"
)

// literalPrefix defines the constant we use to prefix literals
const literalPrefix = "literal:"

// anonymiser anonymises MySQL tables
type anonymiser struct {
	reader.Reader
	tables config.Tables
}

// NewAnonymiser returns an initialised instance of MySQLAnonymiser
func NewAnonymiser(source reader.Reader, tables config.Tables) reader.Reader {
	return &anonymiser{source, tables}
}

func (a *anonymiser) ReadTable(tableName string, rowChan chan<- *database.Row) error {
	logger := log.WithField("table", tableName)

	logger.Info("Loading anonymiser config")
	table, err := a.tables.FindByName(tableName)
	if err != nil {
		logger.WithError(err).Warn("the table is not configured to be anonymised")
		return a.Reader.ReadTable(tableName, rowChan)
	}

	if len(table.Anonymise) == 0 {
		logger.Debug("Skipping anonymiser")
		return a.Reader.ReadTable(tableName, rowChan)
	}

	// Create read/write chanel
	rawChan := make(chan *database.Row)

	// Read from the reader
	go a.Reader.ReadTable(tableName, rawChan)

	// Anonimise the rows
	go func() {
		for {
			row := <-rawChan
			if row == nil {
				break
			}

			actualRow := *row
			for column, fakerType := range table.Anonymise {
				a.anonymiseCell(actualRow[column], fakerType)
			}

			rowChan <- row
		}
	}()

	return nil
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
