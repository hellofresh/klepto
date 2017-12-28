package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"io"

	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
)

// textDumper dumps a database's structure to a stream
type textDumper struct {
	reader reader.Reader
	output io.Writer
}

// NewDumper is the constructor for MySQLDumper
func NewDumper(output io.Writer, rdr reader.Reader) dumper.Dumper {
	return &textDumper{
		reader: rdr,
		output: output,
	}
}

func (d *textDumper) Dump(done chan<- struct{}) error {
	tables, err := d.reader.GetTables()
	if err != nil {
		return err
	}

	structure, err := d.reader.GetStructure()
	if err != nil {
		return err
	}
	io.WriteString(d.output, structure)

	for _, tbl := range tables {
		columns, err := d.reader.GetColumns(tbl)
		if err != nil {
			return err
		}

		insert := fmt.Sprintf("\nINSERT INTO `%s` (%s) VALUES ", tbl, strings.Join(columns, ", "))

		// Create read/write chanel
		rowChan := make(chan database.Row)
		go d.reader.ReadTable(tbl, rowChan)

		go func() {
			for {
				row, more := <-rowChan
				if !more {
					done <- struct{}{}
					return
				}

				io.WriteString(d.output, insert)
				io.WriteString(d.output, "(")
				for i, column := range columns {
					data := row[column]

					if i > 0 {
						io.WriteString(d.output, ",")
					}

					io.WriteString(d.output, d.toSQLStringValue(data.Value))
				}
				io.WriteString(d.output, ")")
				io.WriteString(d.output, ";")
			}
		}()
	}

	return nil
}

func (d *textDumper) Close() error {
	closer, ok := d.output.(io.WriteCloser)
	if ok {
		closer.Close()
	}

	return nil
}

// ResolveType accepts a value and attempts to determine its type
func (d *textDumper) toSQLStringValue(src interface{}) string {
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
		return d.toSQLStringValue(*(src.(*interface{})))
	default:
		panic(src)
	}

	return ""
}
