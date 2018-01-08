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

func (p *myDumper) DumpTable(tableName string, rowChan <-chan database.Row) error {
	txn, err := p.conn.Begin()
	if err != nil {
		return errors.Wrap(err, "failed to open transaction")
	}

	insertedRows, err := p.insertIntoTable(txn, tableName, rowChan)
	if err != nil {
		return errors.Wrap(err, "failed to insert rows")
	}
	log.WithField("table", tableName).WithField("inserted", insertedRows).Debug("inserted rows")

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

func (p *myDumper) Close() error {
	return p.conn.Close()
}

func (p *myDumper) insertIntoTable(txn *sql.Tx, tableName string, rowChan <-chan database.Row) (int64, error) {
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

		for {
			row, more := <-rowChan
			if !more {
				break
			}

			// Put the data in the correct order and format
			rowValues := make([]string, len(columns))
			for i, col := range columns {
				if row[col] == nil {
					rowValues[i] = "NULL"
				} else {
					rowValues[i] = string(row[col].([]uint8))
				}
			}

			if err := w.Write(rowValues); err != nil {
				log.WithError(err).Error("error writing record to mysql")
			}

			atomic.AddInt64(&inserted, 1)
		}

		w.Flush()
	}(rowWriter)

	// Register the reader for reading the csv
	mysql.RegisterReaderHandler(tableName, func() io.Reader {
		return rowReader
	})
	defer mysql.DeregisterReaderHandler(tableName)

	// Execute the query
	txn.Exec("SET foreign_key_checks = 0;")
	if _, err := txn.Exec(query); err != nil {
		return 0, err
	}

	return inserted, nil
}

func (p *myDumper) quoteIdentifier(name string) string {
	return "`" + strings.Replace(name, "`", "``", -1) + "`"
}
