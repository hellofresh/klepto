package mysql

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

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

func (p *myDumper) DumpTable(tableName string, rowChan <-chan database.Table) error {
	txn, err := p.conn.Begin()
	if err != nil {
		return errors.Wrap(err, "failed to open transaction")
	}

	insertedRows, err := p.insertIntoTable(txn, tableName, rowChan)
	if err != nil {
		txn.Rollback()
		return errors.Wrap(err, "failed to insert rows")
	}

	log.WithFields(log.Fields{
		"inserted": insertedRows,
	}).Debug("inserted rows")

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

func (p *myDumper) Close() error {
	return p.conn.Close()
}

func (p *myDumper) insertIntoTable(txn *sql.Tx, tableName string, rowChan <-chan database.Table) (int64, error) {
	columns, err := p.reader.GetColumns(tableName)
	if err != nil {
		return 0, err
	}
	// Create query
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

	// Write all rows as csv to the pipe
	rowReader, rowWriter := io.Pipe()
	var inserted int64
	go func(writer *io.PipeWriter) {
		defer writer.Close()

		w := csv.NewWriter(writer)
		defer w.Flush()

		for {
			table, more := <-rowChan
			if !more {
				break
			}

			columnsForTable, _ := p.reader.GetColumns(table.Name)

			// Put the data in the correct order and format
			rowValues := make([]string, len(columnsForTable))
			for i, col := range columnsForTable {
				switch v := table.Row[col].(type) {
				case nil:
					rowValues[i] = "NULL"
				case string:
					rowValues[i] = table.Row[col].(string)
				case []uint8:
					rowValues[i] = string(table.Row[col].([]uint8))
				default:
					log.WithField("type", v).Info("we have an unhandled type. attempting to convert to a string \n")
					rowValues[i] = table.Row[col].(string)
				}
			}

			if err := w.Write(rowValues); err != nil {
				log.WithError(err).Error("error writing record to mysql")
			}

			atomic.AddInt64(&inserted, 1)
		}
	}(rowWriter)

	// Register the reader for reading the csv
	mysql.RegisterReaderHandler(tableName, func() io.Reader {
		return rowReader
	})
	defer mysql.DeregisterReaderHandler(tableName)

	// Execute the query
	txn.Exec("SET foreign_key_checks = 0;")
	if _, err := txn.Exec(query); err != nil {
		log.WithError(err).WithFields(log.Fields{
			"table": tableName,
		}).Error("Could not insert data")
	}

	return inserted, nil
}

func (p *myDumper) quoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", strings.Replace(name, "`", "``", -1))
}
