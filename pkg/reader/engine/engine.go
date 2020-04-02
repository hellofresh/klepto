package engine

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type (
	// Engine is responsible for sql related read operations.
	Engine struct {
		Storage
		// tables is a cache variable for all tables in the db
		tables []string
		// columns is a cache variable for tables and there columns in the db
		columns sync.Map
		// timeout is the sql read operation timeout
		timeout time.Duration
	}

	// Storage is the read storage database interface.
	Storage interface {
		// GetStructure returns the SQL used to create the database tables
		GetStructure() (string, error)
		// GetTables return a list of all database tables
		GetTables() ([]string, error)
		// GetColumns return a list of all columns for a given table
		GetColumns(string) ([]string, error)
		// QuoteIdentifier returns a quoted instance of a identifier (table, column etc.)
		QuoteIdentifier(string) string
		// Conn return the sql.DB connection
		Conn() *sql.DB
		// Close closes the reader resources and releases them.
		Close() error
	}
)

// New creates a new sql reader engine.
func New(s Storage, timeout time.Duration) *Engine {
	return &Engine{Storage: s, timeout: timeout}
}

// GetTables gets a list of all tables in the database
func (e *Engine) GetTables() ([]string, error) {
	if e.tables == nil {
		tables, err := e.Storage.GetTables()
		if err != nil {
			return nil, err
		}

		e.tables = tables
	}

	return e.tables, nil
}

// GetColumns returns the columns in the specified database table
func (e *Engine) GetColumns(tableName string) ([]string, error) {
	columns, ok := e.columns.Load(tableName)
	if !ok {
		var err error
		columns, err = e.Storage.GetColumns(tableName)
		if err != nil {
			return nil, err
		}

		e.columns.Store(tableName, columns)
	}

	return columns.([]string), nil
}

// ReadTable returns a list of all rows in a table
func (e *Engine) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	defer close(rowChan)

	logger := log.WithField("table", tableName)
	logger.Debug("reading table data")

	if len(opts.Columns) == 0 {
		columns, err := e.GetColumns(tableName)
		if err != nil {
			return errors.Wrap(err, "failed to get columns")
		}
		opts.Columns = e.formatColumns(tableName, columns)
	}

	var (
		query sq.SelectBuilder
		err   error
	)
	query, err = e.buildQuery(tableName, opts)
	if err != nil {
		return errors.Wrapf(err, "failed to build query for %s", tableName)
	}

	var rows *sql.Rows
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	errchan := make(chan error)
	go func() {
		defer close(errchan)
		rows, err = query.RunWith(e.Conn()).QueryContext(ctx)
		errchan <- err
	}()

	select {
	case <-ctx.Done():
		return errors.Wrapf(ctx.Err(), fmt.Sprintf("timeout during read %s table", tableName))
	case err := <-errchan:
		if err != nil {
			querySQL, queryParams, _ := query.ToSql()
			logger.WithError(err).
				WithFields(log.Fields{
					"query":  querySQL,
					"params": queryParams,
				}).Warn("failed to query rows")
			return errors.Wrap(err, "failed to query rows")
		}
		break
	}

	return e.publishRows(rows, rowChan, tableName)
}

// BuildQuery builds the query that will be used to read the table
func (e *Engine) buildQuery(tableName string, opts reader.ReadTableOpt) (sq.SelectBuilder, error) {
	var query sq.SelectBuilder

	query = sq.Select(opts.Columns...).From(e.QuoteIdentifier(tableName))
	for _, r := range opts.Relationships {
		if r.Table == "" {
			r.Table = tableName
		}
		query = query.Join(fmt.Sprintf(
			"%s ON %s.%s = %s.%s",
			r.ReferencedTable,
			r.ReferencedTable,
			r.ReferencedKey,
			r.Table,
			r.ForeignKey,
		))
	}

	if opts.Match != "" {
		query = query.Where(opts.Match)
	}

	for k, v := range opts.Sorts {
		query = query.OrderBy(fmt.Sprintf("%s %s", k, v))
	}

	if opts.Limit > 0 {
		query = query.Limit(opts.Limit)
	}

	return query, nil
}

// FormatColumn returns a escaped table+column string
func (e *Engine) FormatColumn(tableName string, columnName string) string {
	return fmt.Sprintf(
		"%s.%s",
		e.QuoteIdentifier(tableName),
		e.QuoteIdentifier(columnName),
	)
}

func (e *Engine) publishRows(rows *sql.Rows, rowChan chan<- database.Row, tableName string) error {
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return errors.Wrap(err, "failed to get column types")
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
			log.WithError(err).WithField("table", tableName).Warn("failed to fetch row")
			continue
		}

		for idx, column := range columns {
			row[column] = fields[idx]
		}

		rowChan <- row
	}

	return nil
}

func (e *Engine) formatColumns(tableName string, columns []string) []string {
	formatted := make([]string, len(columns))
	for i, c := range columns {
		formatted[i] = e.FormatColumn(tableName, c)
	}

	return formatted
}
