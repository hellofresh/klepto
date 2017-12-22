package generic

import (
	"database/sql"
	"fmt"

	"github.com/hellofresh/klepto/pkg/database"
)

// SqlReader is a base class for sql related readers
type SqlReader struct {
	Connection *sql.DB
}

// GetColumns returns the columns in the specified database table
func (s *SqlReader) GetColumns(table string) (columns []string, err error) {
	// TODO fix since it fails for empty tables
	var rows *sql.Rows
	if rows, err = s.Connection.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table)); err != nil {
		return
	}
	defer rows.Close()

	if columns, err = rows.Columns(); err != nil {
		return
	}

	for k, column := range columns {
		columns[k] = fmt.Sprintf("`%s`", column)
	}
	return
}

// Rows returns a list of all rows in a table
func (s *SqlReader) ReadTable(table string, rowChan chan<- *database.Row) error {
	rows, err := s.Connection.Query(fmt.Sprintf("SELECT * FROM `%s`", table))
	if err != nil {
		return err
	}

	return s.PublishRows(rows, rowChan)
}

func (s *SqlReader) PublishRows(rows *sql.Rows, rowChan chan<- *database.Row) error {
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for rows.Next() {
		row := make(database.Row, len(columns))
		fields := make([]interface{}, len(columns))

		// TODO find out how to handle errors
		rows.Scan(fields...)

		for idx, column := range columns {
			row[column.Name()] = &database.Cell{
				Value: fields[idx],
				Type:  column.ScanType().Kind().String(),
			}
		}

		rowChan <- &row
	}

	rowChan <- nil

	return nil
}
