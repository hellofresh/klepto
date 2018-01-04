package generic

import (
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	log "github.com/sirupsen/logrus"
)

// SqlReader is a base class for sql related readers
type SqlReader struct {
	Connection      *sql.DB
	QuoteIdentifier func(string) string
}

// GetColumns returns the columns in the specified database table
func (s *SqlReader) GetColumns(table string) (columns []string, err error) {
	table = s.QuoteIdentifier(table)

	// TODO fix since it fails for empty tables
	var rows *sql.Rows
	if rows, err = sq.Select("*").From(table).Limit(1).RunWith(s.Connection).Query(); err != nil {
		return
	}
	defer rows.Close()

	if columns, err = rows.Columns(); err != nil {
		return
	}

	for k, column := range columns {
		columns[k] = column
	}
	return
}

// ReadTable returns a list of all rows in a table
func (s *SqlReader) ReadTable(table string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	log.WithField("table", table).Info("Fetching rows")
	columns, err := s.GetColumns(table)
	if err != nil {
		return err
	}

	query := sq.Select(columns...).From(table)

	for _, r := range opts.Relationships {
		query = query.Join(fmt.Sprintf(
			"%s ON %s = %s",
			s.QuoteIdentifier(r.ReferencedTable),
			s.QuoteIdentifier(r.ForeignKey),
			s.QuoteIdentifier(r.ReferencedKey),
		))
	}

	if opts.Limit > 0 {
		query = query.Limit(opts.Limit)
	}

	rows, err := query.RunWith(s.Connection).Query()
	if err != nil {
		return err
	}

	return s.PublishRows(rows, rowChan)
}

func (s *SqlReader) Close() error {
	return s.Connection.Close()
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
			row[column.Name()] = fields[idx]
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
