package postgres

import (
	"database/sql"
	"sync"

	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// pgDumper dumps a database into a postgres db
type pgDumper struct {
	conn   *sql.DB
	reader reader.Reader
}

func NewDumper(conn *sql.DB, rdr reader.Reader) dumper.Dumper {
	return &pgDumper{
		conn:   conn,
		reader: rdr,
	}
}

func (p *pgDumper) Dump(done chan<- struct{}) error {
	if err := p.dumpStructure(); err != nil {
		return err
	}

	return p.dumpTables(done)
}

func (p *pgDumper) Close() error {
	return p.conn.Close()
}

func (p *pgDumper) dumpStructure() error {
	structureSQL, err := p.reader.GetStructure()
	if err != nil {
		return err
	}

	_, err = p.conn.Exec(structureSQL)
	log.Info("Dumped structure")

	return err
}

func (p *pgDumper) dumpTables(done chan<- struct{}) error {
	// Get the tables
	tables, err := p.reader.GetTables()
	if err != nil {
		return err
	}

	// Loop over every table and dump it
	var wg sync.WaitGroup
	wg.Add(len(tables))
	for _, tableName := range tables {
		go func(tableName string) {
			defer wg.Done()
			logger := log.WithField("table", tableName)
			logger.Debug("Dumping table")

			insertedRows, err := p.dumpTable(tableName)
			if err != nil {
				logger.WithError(err).Error("Failed to dump table")
				return
			}

			logger.WithField("rowCount", insertedRows).Info("Dumped table")
		}(tableName)
	}

	go func() {
		// Wait for all table to be dumped
		wg.Wait()

		done <- struct{}{}
	}()

	return nil
}

func (p *pgDumper) dumpTable(tableName string) (int64, error) {
	// Create read/write chanel
	rowChan := make(chan database.Row)

	// Read the table rows
	go p.reader.ReadTable(tableName, rowChan)

	// Write the rows in a transaction to the db
	txn, err := p.conn.Begin()
	if err != nil {
		return 0, err
	}

	insertedRows, err := p.insertIntoTable(txn, tableName, rowChan)
	if err != nil {
		return 0, err
	}

	if err := txn.Commit(); err != nil {
		return 0, err
	}

	return insertedRows, nil
}

func (p *pgDumper) insertIntoTable(txn *sql.Tx, tableName string, rowChan <-chan database.Row) (int64, error) {
	columns, err := p.reader.GetColumns(tableName)
	if err != nil {
		return 0, err
	}

	stmt, err := txn.Prepare(pq.CopyIn(tableName, columns...))
	if err != nil {
		return 0, err
	}

	var inserted int64
	for {
		row, more := <-rowChan
		if !more {
			break
		}

		// Put the data in the correct order
		rowValues := make([]interface{}, len(columns))
		for i, col := range columns {
			rowValues[i] = row[col].Value
		}

		// Insert
		_, err := stmt.Exec(rowValues...)
		if err != nil {
			return 0, err
		}

		inserted++
	}

	if _, err := stmt.Exec(); err != nil {
		return 0, err
	}

	if err = stmt.Close(); err != nil {
		return 0, err
	}

	return inserted, nil
}
