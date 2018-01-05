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

// sqlReader is a base class for sql related readers
type (
	sqlReader struct {
		SqlEngine

		// tables is a cache variable for all tables in the db
		tables []string
		// columns is a cache variable for tables and there columns in the db
		columns map[string][]string
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

func NewSqlReader(engine SqlEngine) reader.Reader {
	return &sqlReader{
		SqlEngine: engine,
		columns:   map[string][]string{},
	}
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
	columns, ok := s.columns[tableName]
	if !ok {
		var err error

		columns, err = s.SqlEngine.GetColumns(tableName)
		if err != nil {
			return nil, err
		}

		s.columns[tableName] = columns
	}

	return columns, nil
}

// ReadTable returns a list of all rows in a table
func (s *sqlReader) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	logger := log.WithField("table", tableName)
	logger.Info("Fetching rows")

	if len(opts.Columns) == 0 {
		columns, err := s.GetColumns(tableName)
		if err != nil {
			return errors.Wrap(err, "failed to get columns")
		}
		opts.Columns = s.formatColumns(tableName, columns)
	}

	query, err := s.buildQuery(tableName, opts)
	if err != nil {
		close(rowChan)
		return errors.Wrapf(err, "failed to build query for %s", tableName)
	}

	for _, r := range opts.Relationships {
		query, err = s.buildJoinQuery(tableName, query, r)
		if err != nil {
			return errors.Wrapf(err, "failed to build a join query for %s with %s", tableName, r.ReferencedTable)
		}
	}

	rows, err := query.RunWith(s.GetConnection()).Query()
	if err != nil {
		close(rowChan)

		querySQL, queryParams, _ := query.ToSql()
		logger.WithFields(log.Fields{
			"query":  querySQL,
			"params": queryParams,
		}).Warn("failed to query rows")

		return errors.Wrap(err, "failed to query rows")
	}

	logger.Debug("Publishing rows")
	return s.publishRows(rows, rowChan)
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

func (s *sqlReader) buildJoinQuery(tableName string, query sq.SelectBuilder, r *reader.RelationshipOpt) (sq.SelectBuilder, error) {
	// TODO: Fetch the reference table configuration from the config file if it's defined.

	subselectJoin, err := s.buildQuery(r.ReferencedTable, reader.ReadTableOpt{
		Columns: []string{r.ReferencedKey},
	})
	if err != nil {
		return query, errors.Wrapf(err, "could not build query for relationship %s", r.ReferencedTable)
	}

	subselectJoinStr, _, err := subselectJoin.ToSql()
	if err != nil {
		return query, errors.Wrapf(err, "could create SQL string for relationship %s", r.ReferencedTable)
	}

	subselectAlias := fmt.Sprintf("%s_%s", tableName, r.ReferencedTable)

	return query.Join(fmt.Sprintf(
		"(%s) AS %s ON %s = %s",
		subselectJoinStr,
		subselectAlias,
		fmt.Sprintf("%s.%s", s.QuoteIdentifier(tableName), s.QuoteIdentifier(r.ForeignKey)),
		fmt.Sprintf("%s.%s", s.QuoteIdentifier(subselectAlias), s.QuoteIdentifier(r.ReferencedKey)),
	)), nil
}

// FormatColumn returns a escaped table+column string
func (s *sqlReader) FormatColumn(tableName string, columnName string) string {
	return fmt.Sprintf("%s.%s", s.QuoteIdentifier(tableName), s.QuoteIdentifier(columnName))
}

func (s *sqlReader) publishRows(rows *sql.Rows, rowChan chan<- database.Row) error {
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

func (s *sqlReader) formatColumns(tableName string, columns []string) []string {
	for i, c := range columns {
		columns[i] = s.FormatColumn(tableName, c)
	}

	return columns
}
