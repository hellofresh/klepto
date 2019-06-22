package query

import (
	"fmt"
	"io"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/usoban/klepto/pkg/config"
	"github.com/usoban/klepto/pkg/database"
	"github.com/usoban/klepto/pkg/dumper"
	"github.com/usoban/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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
func (d *textDumper) Dump(done chan<- struct{}, spec *config.Spec, concurrency int) error {
	tables, err := d.reader.GetTables()
	if err != nil {
		return errors.Wrap(err, "failed to get tables")
	}

	structure, err := d.reader.GetStructure()
	if err != nil {
		return errors.Wrap(err, "could not get database structure")
	}
	io.WriteString(d.output, structure)

	for _, tbl := range tables {
		var opts reader.ReadTableOpt

		table, err := spec.Tables.FindByName(tbl)
		if err != nil {
			log.WithError(err).WithField("table", tbl).Debug("no configuration found for table")
		}

		if table != nil {
			opts = reader.ReadTableOpt{
				Limit:         table.Filter.Limit,
				Relationships: d.relationshipConfigToOptions(table.Relationships),
			}
		}

		// Create read/write chanel
		rowChan := make(chan database.Row)

		go func(tableName string) {
			for {
				row, more := <-rowChan
				if !more {
					done <- struct{}{}
					return
				}

				columnMap, err := d.toSQLColumnMap(row)
				if err != nil {
					log.WithError(err).Fatal("could not convert value to string")
				}

				insert := sq.Insert(tableName).SetMap(columnMap)
				io.WriteString(d.output, sq.DebugSqlizer(insert))
				io.WriteString(d.output, "\n")
			}
		}(tbl)

		if err := d.reader.ReadTable(tbl, rowChan, opts, spec.Matchers); err != nil {
			log.WithError(err).WithField("table", tbl).Error("error while reading table")
		}
	}

	return nil
}

// Close closes the output stream.
func (d *textDumper) Close() error {
	closer, ok := d.output.(io.WriteCloser)
	if ok {
		if err := closer.Close(); err != nil {
			return errors.Wrap(err, "failed to close output stream")
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

func (d *textDumper) relationshipConfigToOptions(relationshipsConfig []*config.Relationship) []*reader.RelationshipOpt {
	var opts []*reader.RelationshipOpt

	for _, r := range relationshipsConfig {
		opts = append(opts, &reader.RelationshipOpt{
			ReferencedTable: r.ReferencedTable,
			ReferencedKey:   r.ReferencedKey,
			ForeignKey:      r.ForeignKey,
		})
	}

	return opts
}
