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
	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
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
	return &myDumper{
		conn:   conn,
		reader: rdr,
	}
}

func (p *myDumper) Dump(done chan<- struct{}, configTables config.Tables) error {
	if err := p.dumpStructure(); err != nil {
		return err
	}

	return p.dumpTables(done, configTables)
}

func (p *myDumper) Close() error {
	return p.conn.Close()
}

func (p *myDumper) dumpStructure() error {
	log.Debug("Dumping structure...")
	structureSQL, err := p.reader.GetStructure()
	if err != nil {
		return errors.Wrap(err, "failed to get structure")
	}

	_, err = p.conn.Exec(structureSQL)
	if err != nil {
		return errors.Wrap(err, "failed to dump structure")
	}

	log.Debug("Structure dumped")
	return nil
}

func (p *myDumper) dumpTables(done chan<- struct{}, configTables config.Tables) error {
	// Get the tables
	tables, err := p.reader.GetTables()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(len(tables))
	for _, tbl := range tables {
		var opts reader.ReadTableOpt

		table, err := configTables.FindByName(tbl)
		if err != nil {
			log.WithError(err).WithField("table", tbl).Debug("no configuration found for table")
		}

		if table != nil {
			opts = reader.ReadTableOpt{
				Limit:         table.Filter.Limit,
				Relationships: p.relationshipConfigToOptions(table.Relationships),
			}
		}

		// Create read/write chanel
		rowChan := make(chan database.Row)

		go func(tableName string, rowChan <-chan database.Row) {
			if err := p.dumpTable(tableName, rowChan); err != nil {
				log.WithError(err).WithField("table", tableName).Error("Failed to dump table")
			}

			wg.Done()
		}(tbl, rowChan)

		go func(tableName string, opts reader.ReadTableOpt, rowChan chan<- database.Row) {
			if err := p.reader.ReadTable(tableName, rowChan, opts); err != nil {
				log.WithError(err).WithField("table", tableName).Error("Failed to read table")
			}
		}(tbl, opts, rowChan)
	}

	go func() {
		// Wait for all table to be dumped
		wg.Wait()

		done <- struct{}{}
	}()

	return nil
}

func (p *myDumper) dumpTable(tableName string, rowChan <-chan database.Row) error {
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

func (p *myDumper) relationshipConfigToOptions(relationshipsConfig []*config.Relationship) []*reader.RelationshipOpt {
	var opts []*reader.RelationshipOpt

	for _, r := range relationshipsConfig {
		opts = append(opts, &reader.RelationshipOpt{
			ReferencedTable: r.ReferencedTable,
			ReferencedKey:   r.ReferencedKey,
			ForeignKey:      r.ForeignKey,
		})
	}

	return opts
}

func (p *myDumper) quoteIdentifier(name string) string {
	return "`" + strings.Replace(name, "`", "``", -1) + "`"
}
