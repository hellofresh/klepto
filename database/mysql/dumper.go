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
	store database.Store
	anon  database.Anonymiser
	out   chan []*database.Cell
	done  chan bool
}

// NewDumper is the constructor for MySQLDumper
func NewDumper(s database.Store, a database.Anonymiser) *Dumper {
	return &Dumper{
		store: s,
		anon:  a,
		out:   make(chan []*database.Cell, 1000),
		done:  make(chan bool),
	}
}

// DumpStructure writes the database's structure to the provided stream
func (d *Dumper) DumpStructure() (structure string, err error) {
	preamble, err := d.store.GetPreamble()
	if err != nil {
		return
	}

	tables, err := d.store.GetTables()
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

// DumpInserts uses a wait group of concurrent processes (one buffer per table)
// and creates insert statements for each table.
func (d *Dumper) DumpInserts() []*bytes.Buffer {
	anonymiser := d.anon
	tables, err := d.store.GetTables()
	if err != nil {
		color.Red("Error getting tables: %s", err.Error())
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
