package mysql

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"

	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/dumper/engine"
	"github.com/hellofresh/klepto/pkg/reader"
)

const (
	null = "NULL"
)

type (
	myDumper struct {
		conn                *sql.DB
		reader              reader.Reader
		setGlobalInline     sync.Once
		disableGlobalInline bool
	}
)

// NewDumper returns a new mysql dumper.
func NewDumper(conn *sql.DB, rdr reader.Reader) dumper.Dumper {
	return engine.New(rdr, &myDumper{
		conn:   conn,
		reader: rdr,
	})
}

// DumpStructure dump the mysql database structure.
func (d *myDumper) DumpStructure(sql string) error {
	if _, err := d.conn.Exec(sql); err != nil {
		return err
	}

	return nil
}

// DumpTable dumps a mysql table.
func (d *myDumper) DumpTable(tableName string, rowChan <-chan database.Row) error {
	var err error
	d.setGlobalInline.Do(func() {
		var allowLocalInline bool
		r := d.conn.QueryRow("SELECT @@GLOBAL.local_infile")
		if err = r.Scan(&allowLocalInline); err != nil {
			return
		}

		if allowLocalInline {
			return
		}

		if _, err = d.conn.Exec("SET GLOBAL local_infile=1"); err != nil {
			return
		}
		d.disableGlobalInline = true
	})
	if err != nil {
		return err
	}

	txn, err := d.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to open transaction: %w", err)
	}

	insertedRows, err := d.insertIntoTable(txn, tableName, rowChan)
	if err != nil {
		defer func() {
			if err := txn.Rollback(); err != nil {
				log.WithError(err).Error("failed to rollback")
			}
		}()
		err = fmt.Errorf("failed to insert rows: %w", err)
		return err
	}

	log.WithFields(log.Fields{
		"table":    tableName,
		"inserted": insertedRows,
	}).Debug("inserted rows")

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Close closes the mysql database connection.
func (d *myDumper) Close() error {
	var errGlobalInline error
	if d.disableGlobalInline {
		_, errGlobalInline = d.conn.Exec("SET GLOBAL local_infile=0")
	}

	err := d.conn.Close()
	if err != nil {
		if errGlobalInline != nil {
			return fmt.Errorf("failed to close mysql connection and `SET GLOBAL local_infile=0`: %w", errGlobalInline)
		}

		return fmt.Errorf("failed to close mysql connection: %w", err)
	}

	if errGlobalInline != nil {
		return fmt.Errorf("failed `SET GLOBAL local_infile=0` please do this manually! : %w", errGlobalInline)
	}

	return nil
}

func (d *myDumper) insertIntoTable(txn *sql.Tx, tableName string, rowChan <-chan database.Row) (int64, error) {
	columns, err := d.reader.GetColumns(tableName)
	if err != nil {
		return 0, fmt.Errorf("failed to get columns: %w", err)
	}

	columnsQuoted := make([]string, len(columns))
	for i, column := range columns {
		columnsQuoted[i] = d.quoteIdentifier(column)
	}

	query := fmt.Sprintf(
		"LOAD DATA LOCAL INFILE 'Reader::%s' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' (%s)",
		tableName,
		d.quoteIdentifier(tableName),
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
			row, more := <-rowChan
			if !more {
				break
			}

			// Put the data in the correct order and format
			rowValues := make([]string, len(columns))
			for i, col := range columns {
				switch v := row[col].(type) {
				case nil:
					rowValues[i] = null
				case string:
					rowValues[i] = v
				case []uint8:
					rowValues[i] = string(v)
				default:
					log.WithField("type", v).Info("we have an unhandled type. attempting to convert to a string \n")
					stringValue, ok := row[col].(string)
					if ok {
						rowValues[i] = stringValue
					} else {
						log.WithField("type", v).Errorf("failed to convert to string, writing as %v", v)
						rowValues[i] = fmt.Sprintf("%v", v)
					}
				}
			}

			if err := w.Write(rowValues); err != nil {
				log.WithError(err).Error("error writing record to mysql")
			}

			atomic.AddInt64(&inserted, 1)
		}
	}(rowWriter)

	// Register the reader for reading the csv
	mysql.RegisterReaderHandler(tableName, func() io.Reader { return rowReader })
	defer mysql.DeregisterReaderHandler(tableName)

	if _, err := txn.Exec("SET foreign_key_checks = 0;"); err != nil {
		return 0, fmt.Errorf("failed to disable foreign key checks: %w", err)
	}

	if _, err := txn.Exec(query); err != nil {
		return 0, fmt.Errorf("failed to execute query: %w", err)
	}

	return inserted, nil
}

func (d *myDumper) quoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", strings.Replace(name, "`", "``", -1))
}
