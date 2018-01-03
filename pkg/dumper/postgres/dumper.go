package postgres

import (
	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
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

		go func(tableName string) {
			for {
				row, more := <-rowChan
				if !more {
					done <- struct{}{}
					return
				}

				insert := sq.Insert(tableName).SetMap(p.toSQLColumnMap(row)).PlaceholderFormat(sq.Dollar)
				_, err := insert.RunWith(p.conn).Exec()
				if err != nil {
					log.WithError(err).WithField("table", tableName).Error("Could not insert record")
				}
			}
		}(tbl)

		p.reader.ReadTable(tbl, rowChan, opts)
	}

	return nil
}

func (p *pgDumper) toSQLColumnMap(row database.Row) map[string]interface{} {
	sqlColumnMap := make(map[string]interface{})

	for column, value := range row {
		sqlColumnMap[column] = value
	}

	return sqlColumnMap
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
