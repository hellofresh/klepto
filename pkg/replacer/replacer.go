package replacer

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
)

type (
	replacer struct {
		reader.Reader
		tables config.Tables
	}
)

func NewReplacer(source reader.Reader, tables config.Tables) reader.Reader {
	return &replacer{source, tables}
}

// ReadTable decorates reader.ReadTable method for modifying values published from the reader.Reader
func (r *replacer) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	logger := log.WithField("table", tableName)
	logger.Debug("Loading replacer config")
	table := r.tables.FindByName(tableName)

	if table == nil {
		logger.Debug("the table is not configured to have replacements")
		return r.Reader.ReadTable(tableName, rowChan, opts)
	}

	// if no replacements are specified
	if len(table.Replace) == 0 {
		logger.Debug("Skipping replacer")
		return r.Reader.ReadTable(tableName, rowChan, opts)
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

			for _, replace := range table.Replace {
				column := replace.Column

				if row[column] == nil {
					continue
				}

				row[column] = strings.Replace(fmt.Sprint(row[column]), replace.Before, replace.After, 1)
			}

			rowChan <- row
		}
	}(rowChan, rawChan, table)

	if err := r.Reader.ReadTable(tableName, rawChan, opts); err != nil {
		return fmt.Errorf("replacer: error while reading table: %w", err)
	}

	return nil
}
