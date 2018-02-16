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
	SqlReader struct {
		SqlEngine
		// tables is a cache variable for all tables in the db
		tables []string
		// columns is a cache variable for tables and there columns in the db
		columns sync.Map
	}

	SqlEngine interface {
		// GetConnection return the sql.DB connection
		GetConnection() *sql.DB

		// GetStructure returns the SQL used to create the database tables
		GetStructure() (string, error)

		// GetTables return a list of all database tables
		GetTables() ([]string, error)

		// GetColumns return a list of all columns for a given table
		GetColumns(string) ([]string, error)

		// QuoteIdentifier returns a quoted instance of a identifier (table, column etc.)
		QuoteIdentifier(string) string

		// Close closes the connection and other resources and releases them.
		Close() error
	}
)

// NewSqlReader creates a new sql reader
func NewSqlReader(se SqlEngine) *SqlReader {
	return &SqlReader{SqlEngine: se}
}

// GetTables gets a list of all tables in the database
func (s *SqlReader) GetTables() ([]string, error) {
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
func (s *SqlReader) GetColumns(tableName string) ([]string, error) {
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
func (s *SqlReader) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	defer close(rowChan)

	logger := log.WithField("table", tableName)
	logger.Debug("reading table data")

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

	return s.publishRows(rows, rowChan, tableName)
}

// BuildQuery builds the query that will be used to read the table
func (s *SqlReader) buildQuery(tableName string, opts reader.ReadTableOpt) (sq.SelectBuilder, error) {
	var query sq.SelectBuilder

	query = sq.Select(opts.Columns...).From(s.QuoteIdentifier(tableName))

	for _, r := range opts.Relationships {
		query = query.Join(fmt.Sprintf(
			"%s ON %s.%s = %s.%s",
			r.ReferencedTable,
			tableName,
			r.ForeignKey,
			r.ReferencedTable,
			r.ReferencedKey,
		))
	}

	if len(opts.Relationships) > 0 {
		query = query.GroupBy(fmt.Sprintf("%s.id", tableName))
	}

	if opts.Match != "" {
		query = query.Where(opts.Match)
	}

	if opts.Limit > 0 {
		query = query.Limit(opts.Limit)
	}

	return query, nil
}

// FormatColumn returns a escaped table+column string
func (s *SqlReader) FormatColumn(tableName string, columnName string) string {
	return fmt.Sprintf(
		"%s.%s",
		s.QuoteIdentifier(tableName),
		s.QuoteIdentifier(columnName),
	)
}

func (s *SqlReader) publishRows(rows *sql.Rows, rowChan chan<- database.Row, tableName string) error {
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
		row := make(database.Row, columnCount)
		fields := make([]interface{}, columnCount)

		for i := 0; i < columnCount; i++ {
			fieldPointers[i] = &fields[i]
		}

		if err := rows.Scan(fieldPointers...); err != nil {
			log.WithError(err).Warn("failed to fetch row")
			continue
		}

		for idx, column := range columns {
			row[column] = fields[idx]
		}

		rowChan <- row
	}

	return nil
}

func (s *SqlReader) formatColumns(tableName string, columns []string) []string {
	formatted := make([]string, len(columns))
	for i, c := range columns {
		formatted[i] = s.FormatColumn(tableName, c)
	}

	return formatted
}
