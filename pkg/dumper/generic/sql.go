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

		DumpTable(tableName string, rowChan <-chan *database.Table) error

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

	// Trigger pre dump tables
	if adv, ok := p.SqlEngine.(SqlEngineAdvanced); ok {
		if err := adv.PreDumpTables(tables); err != nil {
			return err
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(configTables.FilterRelashionships(tables)))

	// TODO make the amount on concurrent dumps configurable
	semaphoreChan := make(chan struct{}, 10)

	go func() {
		// Wait for all table to be dumped
		wg.Wait()
		close(semaphoreChan)

		// Trigger post dump tables
		if adv, ok := p.SqlEngine.(SqlEngineAdvanced); ok {
			if err := adv.PostDumpTables(tables); err != nil {
				log.WithError(err).Error("Post dump tables failed")
			}
		}

		done <- struct{}{}
	}()

	for _, tbl := range tables {
		semaphoreChan <- struct{}{}
		// Create read/write chanel
		rowChan := make(chan *database.Table)

		go func(tableName string, rowChan <-chan *database.Table, wg *sync.WaitGroup) {
			defer wg.Done()
			defer func(semaphoreChan <-chan struct{}) { <-semaphoreChan }(semaphoreChan)

			if err := p.DumpTable(tableName, rowChan); err != nil {
				log.WithError(err).Error("Failed to dump table")
			}
		}(tbl, rowChan, &wg)

		go func(tableName string, rowChan chan<- *database.Table, wg *sync.WaitGroup) {
			var opts reader.ReadTableOpt

			tableConfig, err := configTables.FindByName(tableName)
			if err != nil {
				log.WithError(err).WithField("table", tableName).Debug("no configuration found for table")
			}

			if tableConfig != nil {
				opts = reader.ReadTableOpt{
					Limit:         tableConfig.Filter.Limit,
					Relationships: p.relationshipConfigToOptions(tableConfig.Relationships),
				}
			}

			if err := p.reader.ReadTable(tableName, rowChan, opts); err != nil {
				log.WithError(err).WithField("table", tableName).Error("Failed to read table")
			}
		}(tbl, rowChan, &wg)
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
