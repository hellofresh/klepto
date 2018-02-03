package mysql

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/dumper/generic"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// myDumper dumps a database into a mysql db
type myDumper struct {
	conn   *sql.DB
	reader reader.Reader
}

func NewDumper(conn *sql.DB, rdr reader.Reader) dumper.Dumper {
	return generic.NewSqlDumper(
		rdr,
		&myDumper{
			conn:   conn,
			reader: rdr,
		},
	)
}

func (p *myDumper) DumpStructure(sql string) error {
	if _, err := p.conn.Exec(sql); err != nil {
		return err
	}

	return nil
}

func (p *myDumper) DumpTable(tableName string, rowChan <-chan *database.Table) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	txn, err := p.conn.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to open transaction")
	}

	insertedRows, err := p.insertIntoTable(ctx, txn, tableName, rowChan)
	if err != nil {
		return errors.Wrap(err, "failed to insert rows")
	}

	log.WithField("inserted", insertedRows).Debug("inserted rows")

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

func (p *myDumper) Close() error {
	return p.conn.Close()
}

func (p *myDumper) insertIntoTable(ctx context.Context, txn *sql.Tx, tableName string, rowChan <-chan *database.Table) (int64, error) {
	columns, err := p.reader.GetColumns(tableName)
	if err != nil {
		return 0, err
	}

	columnsQuoted := make([]string, len(columns))
	for i, column := range columns {
		columnsQuoted[i] = p.quoteIdentifier(column)
	}
	query := fmt.Sprintf(
		"LOAD DATA LOCAL INFILE 'Reader::%s' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' (%s)",
		tableName,
		p.quoteIdentifier(tableName),
		strings.Join(columnsQuoted, ","),
	)

	logger := log.WithFields(log.Fields{
		"table":   tableName,
		"columns": columns,
	})
	logger.Debug("Preparing copy in")

	// Write all rows as csv to the pipe
	rowReader, rowWriter := io.Pipe()
	mysql.RegisterReaderHandler(tableName, func() io.Reader { return rowReader })
	defer mysql.DeregisterReaderHandler(tableName)

	logger.Debug("reader handler registered")

	var inserted int64

	sem := make(chan struct{}, 1)
	sem <- struct{}{}

	go func(writer *io.PipeWriter, rowChan <-chan *database.Table, sem <-chan struct{}) {
		defer writer.Close()
		defer func(sem <-chan struct{}) { <-sem }(sem)

		w := csv.NewWriter(writer)
		defer w.Flush()

		for {
			table, ok := <-rowChan
			if !ok {
				logger.Debug("rowChan was closed")
				break
			}

			columnsForTable, _ := p.reader.GetColumns(table.Name)

			// Put the data in the correct order and format
			rowValues := make([]string, len(columnsForTable))
			for i, col := range columnsForTable {
				rowValues[i], err = database.ToSQLStringValue(table.Row[col])
				if err != nil {
					logger.WithError(err).Error("could not assert type for row value")
				}
			}

			log.WithField("table_name", tableName).Debug("inserting row record")
			if err := w.Write(rowValues); err != nil {
				logger.WithError(err).Error("error writing record to mysql")
			} else {
				log.WithField("table_name", tableName).Debug("row record inserted")
				atomic.AddInt64(&inserted, 1)
			}
		}
	}(rowWriter, rowChan, sem)

	logger.Debug("executing set foreign_key_checks for load data")
	if _, err := txn.ExecContext(ctx, "SET foreign_key_checks = 0;"); err != nil {
		logger.WithError(err).Error("Could not set foreign_key_checks for query")
		return 0, err
	}

	logger.Debug("executing query reader for load data")
	if _, err := txn.ExecContext(ctx, query); err != nil {
		logger.WithError(err).Error("Could not insert data")
		return 0, err
	}

	logger.Debug("load query reader executed")

	return inserted, nil
}

func (p *myDumper) toSQLColumnMap(row database.Row) map[string]interface{} {
	sqlColumnMap := make(map[string]interface{})

	for column, value := range row {
		stringValue, err := database.ToSQLStringValue(value)
		if err != nil {
			log.WithError(err).Error("could not assert type for row value")
		}
		sqlColumnMap[column] = stringValue
	}

	return sqlColumnMap
}

func (p *myDumper) quoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", strings.Replace(name, "`", "``", -1))
}
