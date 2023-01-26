package postgres

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"

	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/dumper/engine"
	"github.com/hellofresh/klepto/pkg/reader"
)

type (
	foreignKeyInfo struct {
		tableName            string
		constraintName       string
		constraintDefinition string
	}

	pgDumper struct {
		conn        *sql.DB
		reader      reader.Reader
		isRDS       bool
		foreignKeys []foreignKeyInfo
	}
)

// NewDumper returns a new postgres dumper.
func NewDumper(opts dumper.ConnOpts, conn *sql.DB, rdr reader.Reader) dumper.Dumper {
	return engine.New(rdr, &pgDumper{
		conn:   conn,
		reader: rdr,
		isRDS:  opts.IsRDS,
	})
}

// DumpStructure dump the mysql database structure.
func (d *pgDumper) DumpStructure(sql string) error {
	if _, err := d.conn.Exec(sql); err != nil {
		return err
	}

	return nil
}

// DumpTable dumps a postgres table.
func (d *pgDumper) DumpTable(tableName string, rowChan <-chan database.Row) error {
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

// PreDumpTables Disable triggers on all tables to avoid foreign key constraints
func (d *pgDumper) PreDumpTables(tables []string) error {
	// We can't use `SET session_replication_role = replica` because multiple connections and stuff
	// For RDS databases, the superuser does not have the required permission to call
	// DISABLE TRIGGER ALL, so manually remove and re-add all Foreign Keys
	if !d.isRDS {
		log.Debug("Disabling triggers")
		for _, tbl := range tables {
			query := fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER ALL", strconv.Quote(tbl))
			if _, err := d.conn.Exec(query); err != nil {
				return fmt.Errorf("failed to disable triggers for %s: %w", tbl, err)
			}
		}
		return nil
	}

	log.Debug("Removing foreign keys")
	query := `SELECT conrelid::regclass::varchar tableName,
		conname constraintName,
		pg_catalog.pg_get_constraintdef(r.oid, true) constraintDefinition
		FROM pg_catalog.pg_constraint r
		WHERE r.contype = 'f'
		AND r.connamespace = (SELECT n.oid FROM pg_namespace n WHERE n.nspname = current_schema())
		`
	rows, err := d.conn.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query ForeignKeys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var fk foreignKeyInfo
		if err := rows.Scan(&fk.tableName, &fk.constraintName, &fk.constraintDefinition); err != nil {
			return fmt.Errorf("failed to load ForeignKeyInfo: %w", err)
		}

		tableName := strings.ReplaceAll(fk.tableName, "\"", "")
		query := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", strconv.Quote(tableName), strconv.Quote(fk.constraintName))
		if _, err := d.conn.Exec(query); err != nil {
			return fmt.Errorf("failed to drop constraint %s.%s: %w", fk.tableName, fk.constraintName, err)
		}
		d.foreignKeys = append(d.foreignKeys, fk)
	}
	return nil
}

// PostDumpTables enable triggers on all tables to enforce foreign key constraints
func (d *pgDumper) PostDumpTables(tables []string) error {
	// We can't use `SET session_replication_role = DEFAULT` because multiple connections and stuff
	if !d.isRDS {
		log.Debug("Reenabling triggers")
		for _, tbl := range tables {
			tableName := strings.ReplaceAll(tbl, "\"", "")
			query := fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER ALL", strconv.Quote(tableName))
			if _, err := d.conn.Exec(query); err != nil {
				return fmt.Errorf("failed to enable triggers for %s: %w", tbl, err)
			}
		}
		return nil
	}

	log.Debug("Recreating foreign keys")
	for _, fk := range d.foreignKeys {
		tableName := strings.ReplaceAll(fk.tableName, "\"", "")
		query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", strconv.Quote(tableName), strconv.Quote(fk.constraintName), fk.constraintDefinition)
		if _, err := d.conn.Exec(query); err != nil {
			return fmt.Errorf("failed to re-create ForeignKey %s.%s: %w", fk.tableName, fk.constraintName, err)
		}
	}
	return nil
}

// Close closes the postgres database connection.
func (d *pgDumper) Close() error {
	err := d.conn.Close()
	if err != nil {
		return fmt.Errorf("failed to close postgres connection: %w", err)
	}
	return nil
}

func (d *pgDumper) insertIntoTable(txn *sql.Tx, tableName string, rowChan <-chan database.Row) (int64, error) {
	columns, err := d.reader.GetColumns(tableName)
	if err != nil {
		return 0, fmt.Errorf("failed to get columns: %w", err)
	}

	logger := log.WithFields(log.Fields{
		"table":   tableName,
		"columns": columns,
	})
	logger.Debug("preparing copy in")

	stmt, err := txn.Prepare(pq.CopyIn(tableName, columns...))
	if err != nil {
		return 0, fmt.Errorf("failed to prepare copy in: %w", err)
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
			if bytesVal, ok := val.([]byte); ok {
				val = string(bytesVal)
			}

			rowValues[i] = val
		}

		// Insert
		_, err := stmt.Exec(rowValues...)
		if err != nil {
			return 0, fmt.Errorf("failed to copy in row: %w", err)
		}

		inserted++
	}

	logger.Debug("executing copy in")
	if _, err := stmt.Exec(); err != nil {
		return 0, fmt.Errorf("failed to exec copy in: %w", err)
	}

	return inserted, nil
}
