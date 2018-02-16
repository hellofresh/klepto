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

	SqlEngineAdvanced interface {
		PreDumpTables([]string) error
		PostDumpTables([]string) error
	}
)

func NewSqlDumper(rdr reader.Reader, engine SqlEngine) dumper.Dumper {
	return &sqlDumper{
		SqlEngine: engine,
		reader:    rdr,
	}
}

func (p *sqlDumper) Dump(done chan<- struct{}, configTables config.Tables, concurrency int) error {
	if err := p.readAndDumpStructure(); err != nil {
		return err
	}

	return p.readAndDumpTables(done, configTables, concurrency)
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

func (p *sqlDumper) readAndDumpTables(done chan<- struct{}, configTables config.Tables, concurrency int) error {
	tables, err := p.reader.GetTables()
	if err != nil {
		return err
	}

	// Trigger pre dump tables
	if adv, ok := p.SqlEngine.(SqlEngineAdvanced); ok {
		if err := adv.PreDumpTables(tables); err != nil {
			return err
		}
	}

	semChan := make(chan struct{}, concurrency)

	var wg sync.WaitGroup
	wg.Add(len(tables))

	go func() {
		// Wait for all table to be dumped
		wg.Wait()
		close(semChan)

		// Trigger post dump tables
		if adv, ok := p.SqlEngine.(SqlEngineAdvanced); ok {
			if err := adv.PostDumpTables(tables); err != nil {
				log.WithError(err).Error("Post dump tables failed")
			}
		}

		done <- struct{}{}
	}()

	for _, tbl := range tables {
		semChan <- struct{}{}

		// Create read/write chanel
		rowChan := make(chan database.Row)

		go func(tableName string, rowChan <-chan database.Row) {
			defer wg.Done()
			defer func(semChan <-chan struct{}) { <-semChan }(semChan)

			if err := p.DumpTable(tableName, rowChan); err != nil {
				log.WithError(err).WithField("table", tableName).Error("Failed to dump table")
			}
		}(tbl, rowChan)

		go func(tableName string, rowChan chan<- database.Row) {
			tableConfig, err := configTables.FindByName(tableName)
			if err != nil {
				log.WithError(err).WithField("table", tableName).Debug("no configuration found for table")
			}

			var opts reader.ReadTableOpt
			if tableConfig != nil {
				opts = reader.ReadTableOpt{
					Match:         tableConfig.Filter.Match,
					Limit:         tableConfig.Filter.Limit,
					Relationships: p.relationshipConfigToOptions(tableConfig.Relationships),
				}
			}

			if err := p.reader.ReadTable(tableName, rowChan, opts, configTables); err != nil {
				log.WithError(err).WithField("table", tableName).Error("Failed to read table")
			}
		}(tbl, rowChan)
	}

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
