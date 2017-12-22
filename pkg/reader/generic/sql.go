package generic

import (
	"database/sql"

	"github.com/hellofresh/klepto/pkg/database"
)

func PublishRows(rows *sql.Rows, rowChan chan<- *database.Row) error {
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
