package postgres

import (
	"database/sql"
	"sync"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/lib/pq"
	"github.com/pkg/errors"
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

func (p *pgDumper) Dump(done chan<- struct{}, configTables config.Tables) error {
	if err := p.dumpStructure(); err != nil {
		return err
	}

	return p.dumpTables(done, configTables)
}

func (p *pgDumper) Close() error {
	return p.conn.Close()
}

func (p *pgDumper) dumpStructure() error {
	log.Debug("Dumping structure...")
	structureSQL, err := p.reader.GetStructure()
	if err != nil {
		return err
	}

	_, err = p.conn.Exec(structureSQL)
	log.Debug("Structure dumped")

	return err
}

func (p *pgDumper) dumpTables(done chan<- struct{}, configTables config.Tables) error {
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

		p.reader.ReadTable(tbl, rowChan, opts)
	}

	go func() {
		// Wait for all table to be dumped
		wg.Wait()

		done <- struct{}{}
	}()

	return nil
}

func (p *pgDumper) dumpTable(tableName string, rowChan <-chan database.Row) error {
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

func (p *pgDumper) insertIntoTable(txn *sql.Tx, tableName string, rowChan <-chan database.Row) (int64, error) {
	columns, err := p.reader.GetColumns(tableName)
	if err != nil {
		return 0, err
	}

	logger := log.WithFields(log.Fields{
		"table":   tableName,
		"columns": columns,
	})
	logger.Debug("Preparing copy in")

	stmt, err := txn.Prepare(pq.CopyIn(tableName, columns...))
	if err != nil {
		return 0, errors.Wrap(err, "failed to prepare copy in")
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
			val := row[col]
			switch val.(type) {
			case []byte:
				val = string(val.([]byte))
			}

			rowValues[i] = val
		}

		// Insert
		_, err := stmt.Exec(rowValues...)
		if err != nil {
			return 0, errors.Wrap(err, "failed to copy in row")
		}

		inserted++
	}

	logger.Debug("Executing copy in")
	if _, err := stmt.Exec(); err != nil {
		return 0, errors.Wrap(err, "failed to exec copy in")
	}

	if err = stmt.Close(); err != nil {
		return 0, errors.Wrap(err, "failed to close copy in statement")
	}

	return inserted, nil
}

func (p *pgDumper) relationshipConfigToOptions(relationshipsConfig []*config.Relationship) []*reader.RelationshipOpt {
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
