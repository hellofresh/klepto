package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
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
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*20)
	defer cancel()

	txn, err := p.conn.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to open transaction")
	}

	insertedRows, err := p.insertIntoTable(txn, tableName, rowChan)
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

func (p *myDumper) insertIntoTable(txn *sql.Tx, tableName string, rowChan <-chan *database.Table) (int64, error) {
	var inserted int64

	log.Debug("executing set foreign_key_checks for load data")
	if _, err := txn.Exec("SET foreign_key_checks = 0;"); err != nil {
		log.WithError(err).Error("Could not set foreign_key_checks for query")
		return 0, err
	}

	for {
		table, more := <-rowChan
		if !more {
			break
		}

		insert := sq.Insert(table.Name).SetMap(p.toSQLColumnMap(table.Row))
		_, err := insert.RunWith(txn).Exec()
		if err != nil {
			log.WithError(err).WithField("table", tableName).Error("Could not insert record")
		}

		inserted++
	}

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
