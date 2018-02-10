package anonymiser

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	// literalPrefix defines the constant we use to prefix literals
	literalPrefix = "literal:"
	email         = "EmailAddress"
	username      = "UserName"
)

// Anonymiser is responsible for anonymising columns
type Anonymiser struct {
	reader reader.Reader
	tables config.Tables
}

// NewAnonymiser returns an initialised instance of MySQLAnonymiser
func NewAnonymiser(source reader.Reader, tables config.Tables) *Anonymiser {
	return &Anonymiser{source, tables}
}

// ReadTable wraps reader.ReadTable method for anonymising rows published from the reader.Reader
func (a *Anonymiser) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt, configTables config.Tables) error {
	logger := log.WithField("table", tableName)
	logger.Debug("Loading anonymiser config")
	table, err := a.tables.FindByName(tableName)
	if err != nil {
		logger.WithError(err).Debug("the table is not configured to be anonymised")
		return a.reader.ReadTable(tableName, rowChan, opts, configTables)
	}

	if len(table.Anonymise) == 0 {
		logger.Debug("Skipping anonymiser")
		return a.reader.ReadTable(tableName, rowChan, opts, configTables)
	}

	// Create read/write chanel
	rawChan := make(chan database.Row)

	go func(rowChan chan<- database.Row, rawChan chan database.Row, table *config.Table) {
		for {
			row, more := <-rawChan
			if !more {
				close(rowChan)
				return
			}

			for column, fakerType := range table.Anonymise {
				if strings.HasPrefix(fakerType, literalPrefix) {
					row[column] = strings.TrimPrefix(fakerType, literalPrefix)
					continue
				}

				for name, faker := range Functions {
					if fakerType != name {
						continue
					}

					hash := ""
					// check if the anonymised value should be uniq
					if a.uniq(name) {
						b := make([]byte, 2)
						rand.Read(b)
						hash = hex.EncodeToString(b)
					}

					row[column] = fmt.Sprintf(
						"%s.%s",
						faker.Call([]reflect.Value{})[0].String(),
						hash,
					)
				}
			}

			rowChan <- row
		}
	}(rowChan, rawChan, table)

	if err := a.reader.ReadTable(tableName, rawChan, opts, configTables); err != nil {
		return errors.Wrap(err, "anonymiser: error while reading table")
	}

	return nil
}

func (a *Anonymiser) uniq(column string) bool {
	return column == email || column == username
}
