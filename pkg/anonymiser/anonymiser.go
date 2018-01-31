package anonymiser

import (
	"reflect"
	"strings"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/pkg/errors"
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

func (a *anonymiser) ReadTable(tableName string, rowChan chan<- *database.Table, opts reader.ReadTableOpt) error {
	logger := log.WithField("table", tableName)

	logger.Debug("Loading anonymiser config")
	tableConfig, err := a.tables.FindByName(tableName)
	if err != nil {
		logger.WithError(err).Debug("the table is not configured to be anonymised")
		return a.Reader.ReadTable(tableName, rowChan, opts)
	}

	if len(tableConfig.Anonymise) == 0 {
		logger.Debug("Skipping anonymiser")
		return a.Reader.ReadTable(tableName, rowChan, opts)
	}

	// Create read/write chanel
	rawChan := make(chan *database.Table)

	// Anonimise the rows
	go func() {
		for {
			table, more := <-rawChan
			if !more {
				close(rowChan)
				return
			}

			for column, fakerType := range tableConfig.Anonymise {
				if strings.HasPrefix(fakerType, literalPrefix) {
					table.Row[column] = strings.TrimPrefix(fakerType, literalPrefix)
					continue
				}

				for name, faker := range Functions {
					if fakerType != name {
						continue
					}

					table.Row[column] = faker.Call([]reflect.Value{})[0].String()
				}
			}

			rowChan <- table
		}
	}()

	// Read from the reader
	err = a.Reader.ReadTable(tableName, rawChan, opts)
	if err != nil {
		return errors.Wrap(err, "anonymiser: error while reading table")
	}

	return nil
}
