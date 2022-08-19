package engine

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
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
func (e *Engine) Dump(done chan<- struct{}, cfgTables config.Tables, concurrency int) error {
	if err := e.readAndDumpStructure(); err != nil {
		return err
	}

	return e.readAndDumpTables(done, cfgTables, concurrency)
}

func (e *Engine) readAndDumpStructure() error {
	log.Debug("dumping structure...")
	sql, err := e.reader.GetStructure()
	if err != nil {
		return fmt.Errorf("failed to get structure: %w", err)
	}

	if err := e.DumpStructure(sql); err != nil {
		return fmt.Errorf("failed to dump structure: %w", err)
	}

	log.Debug("structure was dumped")
	return nil
}

func (e *Engine) readAndDumpTables(done chan<- struct{}, cfgTables config.Tables, concurrency int) error {
	tables, err := e.reader.GetTables()
	if err != nil {
		return fmt.Errorf("failed to read and dump tables: %w", err)
	}

	// Trigger pre dump tables
	if adv, ok := e.Dumper.(Hooker); ok {
		if err := adv.PreDumpTables(tables); err != nil {
			return fmt.Errorf("failed to execute pre dump tables: %w", err)
		}
	}

	semChan := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, tbl := range tables {
		logger := log.WithField("table", tbl)
		tableConfig := cfgTables.FindByName(tbl)
		if tableConfig == nil {
			logger.Debug("no configuration found for table")
		}

		var opts reader.ReadTableOpt
		if tableConfig != nil {
			if tableConfig.IgnoreData {
				logger.Debug("ignoring data to dump")
				continue
			}

			opts = reader.NewReadTableOpt(tableConfig)
		} else {
			opts = reader.NewBlankReadTableOpt()
		}

		for i, subset := range opts.Subsets {
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

			go func(tableName string, subsetIndex int, subsetName string, opts reader.ReadTableOpt, rowChan chan<- database.Row, logger *log.Entry) {
				if err := e.reader.ReadSubset(tableName, subsetIndex, rowChan, opts); err != nil {
					logger.WithError(err).Error(fmt.Sprintf("Failed to read '%s' subset of table '%s'", subsetName, tableName))
				}
			}(tbl, i, subset.Name, opts, rowChan, logger)
		}
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
