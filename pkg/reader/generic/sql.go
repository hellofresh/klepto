package generic

import (
	"database/sql"

	"github.com/hellofresh/klepto/pkg/database"
	log "github.com/sirupsen/logrus"
)

// SqlReader is a base class for sql related readers
type SqlReader struct {
	Connection *sql.DB
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

func (s *SqlReader) Close() error {
	return s.Connection.Close()
}
