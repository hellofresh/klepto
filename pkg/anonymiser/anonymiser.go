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
	password      = "Password"
)

type (
	anonymiser struct {
		reader.Reader
		tables config.Tables
	}
)

// NewAnonymiser returns a new anonymiser reader.
func NewAnonymiser(source reader.Reader, tables config.Tables) reader.Reader {
	return &anonymiser{source, tables}
}

// ReadTable decorates reader.ReadTable method for anonymising rows published from the reader.Reader
func (a *anonymiser) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	logger := log.WithField("table", tableName)
	logger.Debug("Loading anonymiser config")
	table := a.tables.FindByName(tableName)
	if table == nil {
		logger.Debug("the table is not configured to be anonymised")
		return a.Reader.ReadTable(tableName, rowChan, opts)
	}

	if len(table.Anonymise) == 0 {
		logger.Debug("Skipping anonymiser")
		return a.Reader.ReadTable(tableName, rowChan, opts)
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

				faker, found := Functions[fakerType]
				if !found {
					logger.WithField("anonymiser", fakerType).Error("Anonymiser is not found")
					// TODO: actually we should stop the whole process here,
					// but currently there is no simple way of doing this, so as a workaround
					// we'll just break dump in case log error will be missed by the user
					row[column] = fmt.Sprintf("Invalid anonymiser: %s", fakerType)
					continue
				}

				var value string
				switch fakerType {
				case email, username:
					b := make([]byte, 2)
					rand.Read(b)
					value = fmt.Sprintf(
						"%s.%s",
						faker.Call([]reflect.Value{})[0].String(),
						hex.EncodeToString(b),
					)
				default:
					value = faker.Call([]reflect.Value{})[0].String()
				}
				row[column] = value
			}

			rowChan <- row
		}
	}(rowChan, rawChan, table)

	if err := a.Reader.ReadTable(tableName, rawChan, opts); err != nil {
		return errors.Wrap(err, "anonymiser: error while reading table")
	}

	return nil
}
