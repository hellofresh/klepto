package generic

import (
	"database/sql"
	"fmt"

	"github.com/hellofresh/klepto/pkg/database"
	log "github.com/sirupsen/logrus"
)

// SqlReader is a base class for sql related readers
type SqlReader struct {
	Connection *sql.DB
}

// GetColumns returns the columns in the specified database table
func (s *SqlReader) GetColumns(table string) (columns []string, err error) {
	// TODO fix since it fails for empty tables
	var rows *sql.Rows
	if rows, err = s.Connection.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", table)); err != nil {
		return
	}
	defer rows.Close()

	if columns, err = rows.Columns(); err != nil {
		return
	}

	for k, column := range columns {
		columns[k] = fmt.Sprintf("%s", column)
	}
	return
}

// ReadTable returns a list of all rows in a table
func (s *SqlReader) ReadTable(table string, rowChan chan<- database.Row) error {
	log.WithField("table", table).Info("Fetching rows")
	rows, err := s.Connection.Query(fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		return err
	}

	return s.PublishRows(rows, rowChan)
}

func (s *SqlReader) PublishRows(rows *sql.Rows, rowChan chan<- database.Row) error {
	// this ensures that there is no more jobs to be done
	defer close(rowChan)
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for rows.Next() {
		row := make(database.Row, len(columns))
		fields := s.createFieldsSlice(len(columns))

		err := rows.Scan(fields...)
		if err != nil {
			log.WithError(err).Warning("Failed to fetch row")
			continue
		}

		for idx, column := range columns {
			row[column.Name()] = &database.Cell{
				Value: fields[idx],
				Type:  column.ScanType().Kind().String(),
			}
		}

		rowChan <- row
	}

	return nil
}

func (s *SqlReader) createFieldsSlice(size int) []interface{} {
	fields := make([]interface{}, size)
	for i := 0; i < size; i++ {
		var v interface{}
		fields[i] = &v
	}

	return fields
}
