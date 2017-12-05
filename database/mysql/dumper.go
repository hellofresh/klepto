package mysql

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/fatih/color"
	"github.com/hellofresh/klepto/database"
)

// Dumper dumps a database's structure to a stream
type Dumper struct {
	store  database.Store
	anon   database.Anonymiser
	config database.ConfigReader
	out    chan []*database.Cell
	done   chan bool
}

// NewDumper is the constructor for MySQLDumper
func NewDumper(s database.Store, a database.Anonymiser, c database.ConfigReader) *Dumper {
	return &Dumper{
		store:  s,
		anon:   a,
		config: c,
		out:    make(chan []*database.Cell, 1000),
		done:   make(chan bool),
	}
}

// DumpStructure writes the database's structure to the provided stream
func (d *Dumper) DumpStructure() (structure string, err error) {
	preamble, err := d.store.GetPreamble()
	if err != nil {
		return
	}

	tables, err := d.setTables()
	if err != nil {
		return
	}

	var tableStructure string
	for _, table := range tables {
		tableStructure, err = d.store.GetTableStructure(table)
		if err != nil {
			return
		}
	}

	structure = fmt.Sprintf("%s\n%s;\n\n", preamble, tableStructure)
	return
}

// WaitGroupBufferer buffers table contents for each wait group.
func (d *Dumper) WaitGroupBufferer() []*bytes.Buffer {
	anonymiser := d.anon
	tables, err := d.setTables()
	if err != nil {
		color.Red("Error setting tables to dump: %s", err.Error())
	}

	var (
		wg           sync.WaitGroup
		tableBuffers []*bytes.Buffer
	)

	wg.Add(len(tables))

	for _, table := range tables {
		columns, err := d.store.GetColumns(table)
		buf := bytes.NewBufferString(fmt.Sprintf("\nINSERT INTO `%s` (%s) VALUES", table, strings.Join(columns, ", ")))

		go d.bufferer(buf, d.out, d.done, &wg)

		err = anonymiser.AnonymiseRows(table, d.out, d.done)
		if err != nil {
			color.Red("Error stealing data: %s", err.Error())
			return tableBuffers
		}

		b := buf.Bytes()
		b = b[:len(b)-1]
		b = append(b, []byte(";")...)
		tableBuffers = append(tableBuffers, buf)
	}

	close(d.out)
	wg.Wait()

	return tableBuffers
}

func (d *Dumper) bufferer(buf *bytes.Buffer, rowChan chan []*database.Cell, done chan bool, wg *sync.WaitGroup) {
	for {
		select {
		case cells, more := <-rowChan:
			if !more {
				done <- true
				return
			}

			len := len(cells)
			for i, c := range cells {
				if i == 0 {
					buf.WriteString("\n(")
				}

				if c.Type == "string" {
					buf.WriteString(fmt.Sprintf("\"%s\"", c.Value))
				} else {
					buf.WriteString(fmt.Sprintf("%s", c.Value))
				}

				if i == len-1 {
					buf.WriteString("),")
				} else {
					buf.WriteString(", ")
				}
			}
		case <-done:
			wg.Done()
			return
		}
	}
}

// Get tables either:
// - from config (if specified) or
// - from the db
// but don't do both.
func (d *Dumper) setTables() (tables []string, err error) {
	table, err := d.config.ReadPrimaryRecord()
	if err != nil {
		return
	}

	if table != "" {
		tables = append(tables, table)
	} else {
		dbTables, gerr := d.store.GetTables()
		if err != nil {
			return nil, gerr
		}
		tables = dbTables
	}
	return tables, nil
}
