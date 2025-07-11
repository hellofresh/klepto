package anonymiser

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
)

const (
	// literalPrefix defines the constant we use to prefix literals
	literalPrefix = "literal:"
	email         = "EmailAddress"
	username      = "UserName"
	latitude      = "Latitude"
	longitude     = "Longitude"
)

var requireArgs = map[string]bool{
	"CharactersN":   true,
	"DigitsN":       true,
	"ParagraphsN":   true,
	"SentencesN":    true,
	"WordsN":        true,
	"CreditCardNum": true,
	"Password":      true,
	"Year":          true,
}

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

// GetColumns decorates the reader.GetColumns method to add support for omitting columns for data transfer
func (a *anonymiser) GetColumns(tableName string) ([]string, error) {
	columns, err := a.Reader.GetColumns(tableName)
	if err != nil {
		return nil, err
	}

	table := a.tables.FindByName(tableName)
	if table == nil || len(table.Omit) == 0 {
		return columns, nil
	}

	omitMap := make(map[string]bool)
	for _, col := range table.Omit {
		omitMap[col] = true
	}

	filteredColumns := make([]string, 0, len(columns))
	for _, col := range columns {
		if !omitMap[col] {
			filteredColumns = append(filteredColumns, col)
		}
	}

	return filteredColumns, nil
}

// ReadTable decorates reader.ReadTable method for anonymising rows published from the reader.Reader
func (a *anonymiser) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	logger := log.WithField("table", tableName)
	logger.Debug("Loading anonymiser config")
	table := a.tables.FindByName(tableName)
	if table == nil {
		logger.Debug("the table is not configured to be anonymised or to have columns omitted")
		return a.Reader.ReadTable(tableName, rowChan, opts)
	}

	if len(table.Anonymise) == 0 && len(table.Omit) == 0 {
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

			// Remove omitted columns from the row
			for _, omitCol := range table.Omit {
				delete(row, omitCol)
			}

			// Anonymise specified columns with Faker
			for column, fakerType := range table.Anonymise {
				if strings.HasPrefix(fakerType, literalPrefix) {
					row[column] = strings.TrimPrefix(fakerType, literalPrefix)
					continue
				}

				fakerType, args := getTypeArgs(fakerType)
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
				case latitude, longitude:
					value = fmt.Sprintf("%f", faker.Call(args)[0].Float())
				default:
					value = faker.Call(args)[0].String()
				}
				row[column] = value
			}

			rowChan <- row
		}
	}(rowChan, rawChan, table)

	if err := a.Reader.ReadTable(tableName, rawChan, opts); err != nil {
		return fmt.Errorf("anonymiser: error while reading table: %w", err)
	}

	return nil
}

func getTypeArgs(fakerType string) (string, []reflect.Value) {
	parts := strings.Split(fakerType, ":")
	fType := parts[0]
	if !requireArgs[fType] {
		return fType, nil
	}

	return fType, parseArgs(Functions[fType], parts[1:])
}

func parseArgs(function reflect.Value, values []string) []reflect.Value {
	t := function.Type()
	argsN := t.NumIn()
	if argsN > len(values) {
		log.WithFields(log.Fields{"expected": argsN, "received": len(values)}).Warn("Not enough arguments passed. Falling back to defaults")
		values = append(values, make([]string, argsN-len(values))...)
	}

	argsV := make([]reflect.Value, argsN)
	for i := 0; i < argsN; i++ {
		argT := t.In(i)
		v := reflect.New(argT).Elem()
		switch argT.Kind() {
		case reflect.String:
			v.SetString(values[i])
		case reflect.Int:
			n, err := strconv.ParseInt(values[i], 10, 0)
			if err != nil {
				log.WithField("argument", values[i]).Warn("Failed to parse argument as string. Falling back to default")
			}
			v.SetInt(n)
		case reflect.Bool:
			b, err := strconv.ParseBool(values[i])
			if err != nil {
				log.WithField("argument", values[i]).Warn("Failed to parse argument as boolean. Falling back to default")
			}
			v.SetBool(b)
		}

		argsV[i] = v
	}
	return argsV
}
