package engine

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
	// Engine is the engine which dispatches and orchestrates a dump.
	Engine struct {
		Dumper
		reader reader.Reader
	}

	// Dumper is the dump engine.
	Dumper interface {
		// DumpStructure dumps database structure given a sql.
		DumpStructure(sql string) error
		// DumpTable dumps a table by name.
		DumpTable(tableName string, rowChan <-chan database.Row) error
		// Close closes the dumper resources and releases them.
		Close() error
	}

	// Hooker are the actions you perform before or after a specified database operation.
	Hooker interface {
		// PreDumpTables performs a action before dumping tables before dumping tables.
		PreDumpTables([]string) error
		// PostDumpTables performs a action after dumping tables before dumping tables.
		PostDumpTables([]string) error
	}
)

// New creates a new engine given the reader and dumper.
func New(rdr reader.Reader, dumper Dumper) dumper.Dumper {
	return &Engine{
		Dumper: dumper,
		reader: rdr,
	}
}

// Dump executes the dump process.
func (e *Engine) Dump(done chan<- struct{}, spec *config.Spec, concurrency int) error {
	if err := e.readAndDumpStructure(); err != nil {
		return err
	}

	return e.readAndDumpTables(done, spec, concurrency)
}

func (e *Engine) readAndDumpStructure() error {
	log.Debug("dumping structure...")
	sql, err := e.reader.GetStructure()
	if err != nil {
		return errors.Wrap(err, "failed to get structure")
	}

	if err := e.DumpStructure(sql); err != nil {
		return errors.Wrap(err, "failed to dump structure")
	}

	log.Debug("structure was dumped")
	return nil
}

func (e *Engine) readAndDumpTables(done chan<- struct{}, spec *config.Spec, concurrency int) error {
	tables, err := e.reader.GetTables()
	if err != nil {
		return errors.Wrap(err, "failed to read and dump tables")
	}

	// Trigger pre dump tables
	if adv, ok := e.Dumper.(Hooker); ok {
		if err := adv.PreDumpTables(tables); err != nil {
			return errors.Wrap(err, "failed to execute pre dump tables")
		}
	}

	semChan := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, tbl := range tables {
		logger := log.WithField("table", tbl)
		tableConfig := spec.Tables.FindByName(tbl)
		if tableConfig == nil {
			logger.Debug("no configuration found for table")
		}

		var opts reader.ReadTableOpt
		if tableConfig != nil {
			if tableConfig.IgnoreData {
				logger.Debug("ignoring data to dump")
				continue
			}

			opts = reader.ReadTableOpt{
				Match:         tableConfig.Filter.Match,
				Sorts:         tableConfig.Filter.Sorts,
				Limit:         tableConfig.Filter.Limit,
				Relationships: e.relationshipConfigToOptions(tableConfig.Relationships),
			}
		}

		// Create read/write chanel
		rowChan := make(chan database.Row)
		semChan <- struct{}{}
		wg.Add(1)

		go func(tableName string, rowChan <-chan database.Row, logger *log.Entry) {
			defer wg.Done()
			defer func(semChan <-chan struct{}) { <-semChan }(semChan)

			if err := e.DumpTable(tableName, rowChan); err != nil {
				logger.WithError(err).Error("Failed to dump table")
			}
		}(tbl, rowChan, logger)

		go func(tableName string, opts reader.ReadTableOpt, rowChan chan<- database.Row, logger *log.Entry) {
			if err := e.reader.ReadTable(tableName, rowChan, opts, spec.Matchers); err != nil {
				logger.WithError(err).Error("Failed to read table")
			}
		}(tbl, opts, rowChan, logger)
	}

	go func() {
		// Wait for all table to be dumped
		wg.Wait()
		close(semChan)

		// Trigger post dump tables
		if adv, ok := e.Dumper.(Hooker); ok {
			if err := adv.PostDumpTables(tables); err != nil {
				log.WithError(err).Error("post dump tables failed")
			}
		}

		done <- struct{}{}
	}()

	return nil
}

func (e *Engine) relationshipConfigToOptions(relationshipsConfig []*config.Relationship) []*reader.RelationshipOpt {
	var opts []*reader.RelationshipOpt

	for _, r := range relationshipsConfig {
		opts = append(opts, &reader.RelationshipOpt{
			Table:           r.Table,
			ReferencedTable: r.ReferencedTable,
			ReferencedKey:   r.ReferencedKey,
			ForeignKey:      r.ForeignKey,
		})
	}

	return opts
}
