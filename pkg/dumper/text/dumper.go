package text

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
)

// textDumper dumps a database's structure to a stream
type textDumper struct {
	reader reader.Reader
}

// NewDumper is the constructor for MySQLDumper
func NewDumper(rdr reader.Reader) dumper.Dumper {
	return &textDumper{
		reader: rdr,
	}
}

func (d *textDumper) Dump(done chan<- bool) error {
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
		columns, err := d.reader.GetColumns(tbl)
		if err != nil {
			return err
		}

		insert := fmt.Sprintf("\nINSERT INTO `%s` (%s) VALUES ", tbl, strings.Join(columns, ", "))

		// Create read/write chanel
		rowChan := make(chan *database.Row)
		go d.reader.ReadTable(tbl, rowChan)

		go func() {
			for {
				rowFromChan, more := <-rowChan
				if more {
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
				} else {
					done <- true
					return
				}
			}
		}()
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
