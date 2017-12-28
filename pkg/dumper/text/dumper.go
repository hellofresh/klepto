package text

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apex/log"
	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
)

// textDumper dumps a database's structure to a stream
type textDumper struct {
	reader       reader.Reader
	configTables config.Tables
}

// NewDumper is the constructor for MySQLDumper
func NewDumper(rdr reader.Reader, configTables config.Tables) dumper.Dumper {
	return &textDumper{
		reader:       rdr,
		configTables: configTables,
	}
}

func (d *textDumper) Dump() error {
	tables, err := d.reader.GetTables()
	if err != nil {
		return err
	}

	buf := os.Stdout

	structure, err := d.reader.GetStructure()
	if err != nil {
		return err
	}
	buf.WriteString(structure)

	for _, tbl := range tables {
		table, err := d.configTables.FindByName(tbl)
		if err != nil {
			log.WithError(err).WithField("table", tbl).Debug("no configuration found for table")
		}

		columns, err := d.reader.GetColumns(tbl)
		if err != nil {
			return err
		}

		insert := fmt.Sprintf("\nINSERT INTO `%s` (%s) VALUES ", tbl, strings.Join(columns, ", "))
		// Create read/write chanel
		rowChan := make(chan *database.Row)

		if table != nil {
			opts := reader.ReadTableOpt{
				Limit:         table.Filter.Limit,
				Relationships: d.relationshipConfigToOptions(table.Relationships),
			}
			go d.reader.ReadTable(tbl, rowChan, opts)
		} else {
			go d.reader.ReadTable(tbl, rowChan, reader.ReadTableOpt{})
		}

		for {
			rowFromChan := <-rowChan
			if rowFromChan == nil {
				break
			}
			row := *rowFromChan

			buf.WriteString(insert)
			buf.WriteString("(")
			for i, column := range columns {
				data := row[column]

				if i > 0 {
					buf.WriteString(",")
				}

				buf.WriteString(d.toSqlStringValue(data.Value))
			}
			buf.WriteString(")")
			buf.WriteString(";")
		}
	}

	return nil
}

// ResolveType accepts a value and attempts to determine its type
func (d *textDumper) toSqlStringValue(src interface{}) string {
	switch src.(type) {
	case int64:
		if value, ok := src.(int64); ok {
			return strconv.FormatInt(value, 10)
		}
	case float64:
		if value, ok := src.(float64); ok {
			return fmt.Sprintf("%v", value)
		}
	case bool:
		if value, ok := src.(bool); ok {
			return strconv.FormatBool(value)
		}
	case string:
		if value, ok := src.(string); ok {
			return strconv.Quote(string(value))
		}
	case []byte:
		// TODO handle blobs?
		if value, ok := src.([]uint8); ok {
			return strconv.Quote(string(value))
		}
	case time.Time:
		if value, ok := src.(time.Time); ok {
			return strconv.Quote(value.String())
		}
	case nil:
		return "NULL"
	case *interface{}:
		if src == nil {
			return "NULL"
		}
		return d.toSqlStringValue(*(src.(*interface{})))
	default:
		panic(src)
	}

	return ""
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
