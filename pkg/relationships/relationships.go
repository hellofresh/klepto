package relationships

import (
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// anonymiser anonymises MySQL tables
type relationships struct {
	reader.Reader
	sqlEngine reader.SqlEngine
	tables    config.Tables
}

// New returns an initialised instance of MySQLAnonymiser
func New(source reader.Reader, engine reader.SqlEngine, tables config.Tables) reader.Reader {
	return &relationships{Reader: source, sqlEngine: engine, tables: tables}
}

func (a *relationships) ReadTable(tableName string, rowChan chan<- *database.Table, opts reader.ReadTableOpt) error {
	logger := log.WithField("table", tableName)

	// Create read/write chanel
	rawChan := make(chan *database.Table)
	relationships := a.tables.FlattenRelationships()

	if _, ok := relationships[tableName]; ok {
		return nil
	}

	// Anonimise the rows
	go func(tableName string, rowChan chan<- *database.Table, rawChan <-chan *database.Table) {
		for {
			table, more := <-rawChan
			if !more {
				close(rowChan)
				return
			}

			tableConfig, err := a.tables.FindByName(table.Name)
			if err != nil {
				// logger.WithError(err).Warn("there is no relationships declared")
				rowChan <- table
				continue
			}

			for _, r := range tableConfig.Relationships {
				relationshipOpts := reader.ReadTableOpt{}
				relationshipColumns, err := a.GetColumns(r.ReferencedTable)
				if err != nil {
					logger.WithError(err).Warn("could not get columns")
					continue
				}
				relationshipOpts.Columns = a.formatColumns(r.ReferencedTable, relationshipColumns)

				value, _ := table.Row[r.ForeignKey]
				rowValue, err := database.ToSQLStringValue(value)
				if err != nil {
					log.WithField("column", r.ForeignKey).WithError(err).Error("Failed to parse an SQL value for column")
					continue
				}

				q, _ := a.buildQuery(r.ReferencedTable, relationshipOpts)
				q = q.Where(fmt.Sprintf(
					"%s = '%v'",
					r.ReferencedKey,
					rowValue,
				))

				relationshipRows, err := q.RunWith(a.sqlEngine.GetConnection()).Query()
				if err != nil {
					querySQL, queryParams, _ := q.ToSql()
					log.WithError(err).WithFields(log.Fields{
						"query":  querySQL,
						"params": queryParams,
					}).Error("failed to query relationship rows")

					continue
				}

				if err := a.publishRows(r.ReferencedTable, relationshipRows, rowChan, relationshipOpts); err != nil {
					logger.WithError(err).Error("failed to publish rows for relationship")
				}

				logger.Debug("Reading relationship table data")
				if _, ok := relationships[tableName]; !ok {
					rowChan <- table
				}
			}
		}
	}(tableName, rowChan, rawChan)

	// Read from the reader
	err := a.Reader.ReadTable(tableName, rawChan, opts)
	if err != nil {
		return errors.Wrap(err, "anonymiser: error while reading table")
	}

	return nil
}

func (a *relationships) formatColumns(tableName string, columns []string) []string {
	formatted := make([]string, len(columns))
	for i, c := range columns {
		formatted[i] = a.FormatColumn(tableName, c)
	}

	return formatted
}

// BuildQuery builds the query that will be used to read the table
func (a *relationships) buildQuery(tableName string, opts reader.ReadTableOpt) (sq.SelectBuilder, error) {
	var query sq.SelectBuilder

	query = sq.Select(opts.Columns...).From(a.sqlEngine.QuoteIdentifier(tableName))

	if opts.Limit > 0 {
		query = query.Limit(opts.Limit)
	}

	return query, nil
}

func (a *relationships) publishRows(tableName string, rows *sql.Rows, rowChan chan<- *database.Table, opts reader.ReadTableOpt) error {
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
