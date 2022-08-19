package query

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	log "github.com/sirupsen/logrus"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
)

type (
	textDumper struct {
		reader reader.Reader
		output io.Writer
	}
)

// NewDumper returns a new text dumper implementation.
func NewDumper(output io.Writer, rdr reader.Reader) dumper.Dumper {
	return &textDumper{
		reader: rdr,
		output: output,
	}
}

// Dump executes the dump stream process.
func (d *textDumper) Dump(done chan<- struct{}, cfgTables config.Tables, concurrency int) error {
	tables, err := d.reader.GetTables()
	if err != nil {
		return fmt.Errorf("failed to get tables: %w", err)
	}

	structure, err := d.reader.GetStructure()
	if err != nil {
		return fmt.Errorf("could not get database structure: %w", err)
	}
	if _, err := io.WriteString(d.output, structure); err != nil {
		return fmt.Errorf("could not write structure to output: %w", err)
	}

	var wg sync.WaitGroup
	for _, tbl := range tables {
		var opts reader.ReadTableOpt
		logger := log.WithField("table", tbl)

		tableConfig := cfgTables.FindByName(tbl)
		if tableConfig == nil {
			logger.Debug("no configuration found for table")
		} else {
			if tableConfig.IgnoreData {
				logger.Debug("ignoring data to dump")
				continue
			}
			opts = reader.NewReadTableOpt(tableConfig)
		}

		// Create read/write chanel
		rowChan := make(chan database.Row)

		wg.Add(1)
		go func(tableName string) {
			defer wg.Done()

			for {
				row, more := <-rowChan
				if !more {
					return
				}

				columnMap, err := d.toSQLColumnMap(row)
				if err != nil {
					logger.WithError(err).Fatal("could not convert value to string")
				}

				insert := sq.Insert(tableName).SetMap(columnMap)
				if _, err := io.WriteString(d.output, sq.DebugSqlizer(insert)); err != nil {
					logger.WithError(err).Error("could not write insert statement to output")
				}
				if _, err := io.WriteString(d.output, "\n"); err != nil {
					logger.WithError(err).Error("could not write new line to output")
				}
			}
		}(tbl)

		for subsetIndex, subset := range opts.Subsets {
			if err := d.reader.ReadSubset(tbl, subsetIndex, rowChan, opts); err != nil {
				log.WithError(err).Error(fmt.Sprintf("Failed to read '%s' subset of table '%s'", subset.Name, tbl))
			}
		}
	}

	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	return nil
}

// Close closes the output stream.
func (d *textDumper) Close() error {
	closer, ok := d.output.(io.WriteCloser)
	if ok {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("failed to close output stream: %w", err)
		}
		return nil
	}

	return errors.New("unable to close output: wrong closer type")
}

func (d *textDumper) toSQLColumnMap(row database.Row) (map[string]interface{}, error) {
	sqlColumnMap := make(map[string]interface{})

	for column, value := range row {
		strValue, err := d.toSQLStringValue(value)
		if err != nil {
			return sqlColumnMap, err
		}

		sqlColumnMap[column] = fmt.Sprintf("%v", strValue)
	}

	return sqlColumnMap, nil
}

// ResolveType accepts a value and attempts to determine its type
func (d *textDumper) toSQLStringValue(src interface{}) (string, error) {
	switch src.(type) {
	case int64:
		if value, ok := src.(int64); ok {
			return strconv.FormatInt(value, 10), nil
		}
	case float64:
		if value, ok := src.(float64); ok {
			return fmt.Sprintf("%v", value), nil
		}
	case bool:
		if value, ok := src.(bool); ok {
			return strconv.FormatBool(value), nil
		}
	case string:
		if value, ok := src.(string); ok {
			return value, nil
		}
	case []byte:
		// TODO handle blobs?
		if value, ok := src.([]byte); ok {
			return string(value), nil
		}
	case time.Time:
		if value, ok := src.(time.Time); ok {
			return value.String(), nil
		}
	case nil:
		return "NULL", nil
	case *interface{}:
		if src == nil {
			return "NULL", nil
		}
		return d.toSQLStringValue(*(src.(*interface{})))
	default:
		return "", errors.New("could not parse type")
	}

	return "", nil
}
