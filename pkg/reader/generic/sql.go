package generic

import (
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// SqlReader is a base class for sql related readers
type SqlReader struct {
	Connection      *sql.DB
	QuoteIdentifier func(string) string
}

// GetColumns returns the columns in the specified database table
func (s *SqlReader) GetColumns(tableName string) (columns []string, err error) {
	// TODO fix since it fails for empty tables
	tableNameQuoted := s.QuoteIdentifier(tableName)
	var rows *sql.Rows
	if rows, err = sq.Select("*").From(tableNameQuoted).Limit(1).RunWith(s.Connection).Query(); err != nil {
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
func (s *SqlReader) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	logger := log.WithField("table", tableName)
	logger.Info("Fetching rows")

	if len(opts.Columns) == 0 {
		columns, err := s.GetColumns(tableName)
		if err != nil {
			return errors.Wrap(err, "failed to get columns")
		}
		opts.Columns = s.formatColumns(tableName, columns)
	}

	query, err := s.BuildQuery(tableName, opts)
	if err != nil {
		close(rowChan)
		return errors.Wrapf(err, "failed to build query for %s", tableName)
	}

	for _, r := range opts.Relationships {
		subselectJoin, err := s.BuildQuery(r.ReferencedTable, reader.ReadTableOpt{
			Columns: []string{r.ReferencedKey},
			Limit:   opts.Limit,
		})
		if err != nil {
			return errors.Wrapf(err, "could not build query for relationship %s", r.ReferencedTable)
		}

		subselectJoinStr, _, err := subselectJoin.ToSql()
		if err != nil {
			return errors.Wrapf(err, "could create SQL string for relationship %s", r.ReferencedTable)
		}

		subselectAlias := fmt.Sprintf("%s_%s", tableName, r.ReferencedTable)

		query = query.Join(fmt.Sprintf(
			"(%s) AS %s ON %s = %s",
			subselectJoinStr,
			subselectAlias,
			fmt.Sprintf("%s.%s", s.QuoteIdentifier(tableName), s.QuoteIdentifier(r.ForeignKey)),
			fmt.Sprintf("%s.%s", s.QuoteIdentifier(subselectAlias), s.QuoteIdentifier(r.ReferencedKey)),
		))
	}

	rows, err := query.RunWith(s.Connection).Query()
	if err != nil {
		close(rowChan)

		querySQL, queryParams, _ := query.ToSql()
		logger.WithFields(log.Fields{
			"query":  querySQL,
			"params": queryParams,
		}).Debug("failed to query rows")

		return errors.Wrap(err, "failed to query rows")
	}

	logger.Debug("Publishing rows")
	return s.PublishRows(rows, rowChan)
}

// BuildQuery builds the query that will be used to read the table
func (s *SqlReader) BuildQuery(tableName string, opts reader.ReadTableOpt) (sq.SelectBuilder, error) {
	var query sq.SelectBuilder

	query = sq.Select(opts.Columns...).From(s.QuoteIdentifier(tableName))

	if opts.Limit > 0 {
		query = query.Limit(opts.Limit)
	}

	return query, nil
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

		fields := make([]interface{}, len(columns))
		fieldPointers := make([]interface{}, len(columns))
		for i := 0; i < len(columns); i++ {
			fieldPointers[i] = &fields[i]
		}

		err := rows.Scan(fieldPointers...)
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

// FormatColumn returns a escaped table+column string
func (s *SqlReader) FormatColumn(tableName string, columnName string) string {
	return fmt.Sprintf("%s.%s", s.QuoteIdentifier(tableName), s.QuoteIdentifier(columnName))
}

func (s *SqlReader) formatColumns(tableName string, columns []string) []string {
	for i, c := range columns {
		columns[i] = s.FormatColumn(tableName, c)
	}

	return columns
}
