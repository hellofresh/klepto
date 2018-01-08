package generic

import (
	"sync"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type (
	sqlDumper struct {
		SqlEngine

		reader reader.Reader
	}

	SqlEngine interface {
		DumpStructure(sql string) error

		DumpTable(tableName string, rowChan <-chan database.Row) error

		// Close closes the dumper resources and releases them.
		Close() error
	}
)

func NewSqlDumper(rdr reader.Reader, engine SqlEngine) dumper.Dumper {
	return &sqlDumper{
		SqlEngine: engine,
		reader:    rdr,
	}
}

func (p *sqlDumper) Dump(done chan<- struct{}, configTables config.Tables) error {
	if err := p.readAndDumpStructure(); err != nil {
		return err
	}

	return p.readAndDumpTables(done, configTables)
}

func (p *sqlDumper) readAndDumpStructure() error {
	log.Debug("Dumping structure...")
	structureSQL, err := p.reader.GetStructure()
	if err != nil {
		return errors.Wrap(err, "failed to get structure")
	}

	if err := p.DumpStructure(structureSQL); err != nil {
		return errors.Wrap(err, "failed to dump structure")
	}

	log.Debug("Structure dumped")
	return nil
}

func (p *sqlDumper) readAndDumpTables(done chan<- struct{}, configTables config.Tables) error {
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
			if err := p.DumpTable(tableName, rowChan); err != nil {
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

func (p *sqlDumper) relationshipConfigToOptions(relationshipsConfig []*config.Relationship) []*reader.RelationshipOpt {
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
