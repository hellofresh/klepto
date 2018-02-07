package generic

import (
	"database/sql"
	"fmt"
	"sync"

	sq "github.com/Masterminds/squirrel"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// sqlReader is a base class for sql related readers
type (
	sqlReader struct {
		reader.SqlEngine

		// tables is a cache variable for all tables in the db
		tables []string
		// columns is a cache variable for tables and there columns in the db
		columns sync.Map
	}
)

func NewSqlReader(engine reader.SqlEngine) reader.Reader {
	return &sqlReader{
		SqlEngine: engine,
	}
}

func (s *sqlReader) GetSQLEngine() reader.SqlEngine {
	return s.SqlEngine
}

// GetTables gets a list of all tables in the database
func (s *sqlReader) GetTables() ([]string, error) {
	if s.tables == nil {
		tables, err := s.SqlEngine.GetTables()
		if err != nil {
			return nil, err
		}

		s.tables = tables
	}

	return s.tables, nil
}

// GetColumns returns the columns in the specified database table
func (s *sqlReader) GetColumns(tableName string) ([]string, error) {
	columns, ok := s.columns.Load(tableName)
	if !ok {
		var err error

		columns, err = s.SqlEngine.GetColumns(tableName)
		if err != nil {
			return nil, err
		}

		s.columns.Store(tableName, columns)
	}

	return columns.([]string), nil
}

// ReadTable returns a list of all rows in a table
func (s *sqlReader) ReadTable(tableName string, rowChan chan<- *database.Table, opts reader.ReadTableOpt) error {
	defer close(rowChan)

	logger := log.WithField("table", tableName)
	logger.Debug("Reading table data")

	if len(opts.Columns) == 0 {
		columns, err := s.GetColumns(tableName)
		if err != nil {
			return errors.Wrap(err, "failed to get columns")
		}
		opts.Columns = s.formatColumns(tableName, columns)
	}

	query, err := s.buildQuery(tableName, opts)
	if err != nil {
		return errors.Wrapf(err, "failed to build query for %s", tableName)
	}

	rows, err := query.RunWith(s.GetConnection()).Query()
	if err != nil {
		querySQL, queryParams, _ := query.ToSql()
		logger.WithFields(log.Fields{
			"query":  querySQL,
			"params": queryParams,
		}).Warn("failed to query rows")

		return errors.Wrap(err, "failed to query rows")
	}

	logger.Debug("publishing rows")
	if err := s.publishRows(tableName, rows, rowChan, opts); err != nil {
		logger.Debug("failed to publish rows")
		return err
	}

	logger.Debug("rows published")

	return nil
}

func (s *sqlReader) publishRows(tableName string, rows *sql.Rows, rowChan chan<- *database.Table, opts reader.ReadTableOpt) error {
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	columnCount := len(columnTypes)
	columns := make([]string, columnCount)
	for i, col := range columnTypes {
		columns[i] = col.Name()
	}

	fieldPointers := make([]interface{}, columnCount)

	for rows.Next() {
		table := database.NewTable(tableName)
		fields := make([]interface{}, columnCount)

		for i := 0; i < columnCount; i++ {
			fieldPointers[i] = &fields[i]
		}

		if err := rows.Scan(fieldPointers...); err != nil {
			log.WithError(err).Warning("Failed to fetch row")
			continue
		}

		for idx, column := range columns {
			table.Row[column] = fields[idx]
		}

		rowChan <- table
	}

	return nil
}

// BuildQuery builds the query that will be used to read the table
func (s *sqlReader) buildQuery(tableName string, opts reader.ReadTableOpt) (sq.SelectBuilder, error) {
	var query sq.SelectBuilder

	query = sq.Select(opts.Columns...).From(s.QuoteIdentifier(tableName))

	if opts.Limit > 0 {
		query = query.Limit(opts.Limit)
	}

	return query, nil
}

// FormatColumn returns a escaped table+column string
func (s *sqlReader) FormatColumn(tableName string, columnName string) string {
	return fmt.Sprintf("%s.%s", s.QuoteIdentifier(tableName), s.QuoteIdentifier(columnName))
}

func (s *sqlReader) formatColumns(tableName string, columns []string) []string {
	formatted := make([]string, len(columns))
	for i, c := range columns {
		formatted[i] = s.FormatColumn(tableName, c)
	}

	return formatted
}
