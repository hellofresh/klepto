package postgres

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/dumper/generic"
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
	return generic.NewSqlDumper(
		rdr,
		&pgDumper{
			conn:   conn,
			reader: rdr,
		},
	)
}

func (p *pgDumper) DumpStructure(sql string) error {
	if _, err := p.conn.Exec(sql); err != nil {
		return err
	}

	return nil
}

func (p *pgDumper) DumpTable(tableName string, rowChan <-chan database.Row) error {
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

// PreDumpTables Disable triggers on all tables to avoid foreign key constraints
func (p *pgDumper) PreDumpTables(tables []string) error {
	// We can't use `SET session_replication_role = replica` because multiple connections and stuff
	for _, tbl := range tables {
		query := fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER ALL", strconv.Quote(tbl))
		if _, err := p.conn.Exec(query); err != nil {
			return errors.Wrapf(err, "Failed to disable triggers for %s", tbl)
		}
	}

	return nil
}

// PostDumpTables Enable triggers on all tables to enforce foreign key constraints
func (p *pgDumper) PostDumpTables(tables []string) error {
	// We can't use `SET session_replication_role = DEFAULT` because multiple connections and stuff
	for _, tbl := range tables {
		query := fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER ALL", strconv.Quote(tbl))
		if _, err := p.conn.Exec(query); err != nil {
			return errors.Wrap(err, "Failed to enable triggers")
		}
	}

	return nil
}

func (p *pgDumper) Close() error {
	return p.conn.Close()
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
	defer func() {
		if err = stmt.Close(); err != nil {
			log.WithError(err).Error("failed to close copy in statement")
		}
	}()

	var inserted int64
	for {
		row, more := <-rowChan
		if !more {
			logger.Debug("rowChan was closed")
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
