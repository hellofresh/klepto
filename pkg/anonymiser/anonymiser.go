package anonymiser

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"

	"github.com/usoban/klepto/pkg/config"
	"github.com/usoban/klepto/pkg/database"
	"github.com/usoban/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	expr "github.com/antonmedv/expr"
)

const (
	// literalPrefix defines the constant we use to prefix literals
	literalPrefix = "literal:"
	conditionalPrefix = "cond:"
	noAnonymisation = "__DO_NOT_ANONYMIZE__"
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
func (a *anonymiser) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt, matchers config.Matchers) error {
	logger := log.WithField("table", tableName)
	logger.Debug("Loading anonymiser config")
	table, err := a.tables.FindByName(tableName)
	if err != nil {
		logger.WithError(err).Debug("the table is not configured to be anonymised")
		return a.Reader.ReadTable(tableName, rowChan, opts, matchers)
	}

	if len(table.Anonymise) == 0 {
		logger.Debug("Skipping anonymiser")
		return a.Reader.ReadTable(tableName, rowChan, opts, matchers)
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

				if strings.HasPrefix(fakerType, conditionalPrefix) {

					env := map[string]interface{}{
						"row": row,
						"column": row[column],
						"Value": func(row database.Row, key string) string {
							str := row[key].([]uint8)
							return string(str)
						},
						"Anon": func(fakerType string) string {
							return Anonymise(fakerType)
						},
						"Skip": func() string {
							return noAnonymisation
						},
					}

					conditionExpr := strings.TrimPrefix(fakerType, conditionalPrefix)
					// fmt.Println(row)
					// fmt.Println(conditionExpr)

					program, err := expr.Compile(conditionExpr, expr.Env(env))
					if err != nil {
						logger.WithError(err).Error("Eval rule compilation error")
						continue
					}

					output, err := expr.Run(program, env)
					if err != nil {
						logger.WithError(err).Error("Eval rule runtime error")
						continue
					}

					if output != noAnonymisation {
						row[column] = output
					}

					continue
				}

				row[column] = Anonymise(fakerType)
			}

			rowChan <- row
		}
	}(rowChan, rawChan, table)

	if err := a.Reader.ReadTable(tableName, rawChan, opts, matchers); err != nil {
		return errors.Wrap(err, "anonymiser: error while reading table")
	}

	return nil
}

func Anonymise(fakerType string) string {
	var value string

	for name, faker := range Functions {
		if fakerType != name {
			continue
		}

		switch name {
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
	}

	return value
}