package text

import (
	"fmt"

	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
)

// textDumper dumps a database's structure to a stream
type textDumper struct {
	reader reader.Reader
}

// NewDumper is the constructor for MySQLDumper
func NewDumper(rdr reader.Reader) dumper.Dumper {
	return &textDumper{
		reader: rdr,
	}
}

func (d *textDumper) Dump() error {
	tables, err := d.reader.GetTables()
	if err != nil {
		return err
	}

	for _, tbl := range tables {
		// Create read/write chanel
		rowChan := make(chan *database.Row)

		go d.reader.ReadTable(tbl, rowChan)

		for {
			row := <-rowChan
			if row == nil {
				rowChan <- nil
				break
			}

			fmt.Sprintf("%v", row)
		}
	}

	return nil
}

//// DumpStructure writes the database's structure to the provided stream
//func (d *textDumper) dumpStructure() (structure string, err error) {
//	preamble, err := d.reader.GetPreamble()
//	if err != nil {
//		return
//	}
//
//	tables, err := d.reader.GetTables()
//	if err != nil {
//		return
//	}
//
//	var tableStructure string
//	for _, table := range tables {
//		tableStructure, err = d.reader.GetTableStructure(table)
//		if err != nil {
//			return
//		}
//	}
//
//	structure = fmt.Sprintf("%s\n%s;\n\n", preamble, tableStructure)
//	return
//}
//
//// WaitGroupBufferer buffers table contents for each wait group.
//func (d *textDumper) WaitGroupBufferer() []*bytes.Buffer {
//	anonymiser := d.anon
//	tables, err := d.store.GetTables()
//	if err != nil {
//		color.Red("Error getting tables: %s", err.Error())
//	}
//
//	var (
//		wg           sync.WaitGroup
//		tableBuffers []*bytes.Buffer
//	)
//
//	wg.Add(len(tables))
//
//	for _, table := range tables {
//		columns, err := d.store.GetColumns(table)
//		buf := bytes.NewBufferString(fmt.Sprintf("\nINSERT INTO `%s` (%s) VALUES", table, strings.Join(columns, ", ")))
//
//		go d.bufferer(buf, d.out, d.done, &wg)
//
//		err = anonymiser.AnonymiseRows(table, d.out, d.done)
//		if err != nil {
//			color.Red("Error stealing data: %s", err.Error())
//			return tableBuffers
//		}
//
//		b := buf.Bytes()
//		b = b[:len(b)-1]
//		b = append(b, []byte(";")...)
//		tableBuffers = append(tableBuffers, buf)
//	}
//
//	close(d.out)
//	wg.Wait()
//
//	return tableBuffers
//}
//
//func (d *textDumper) bufferer(buf *bytes.Buffer, rowChan chan []*database.Cell, done chan bool, wg *sync.WaitGroup) {
//	for {
//		select {
//		case cells, more := <-rowChan:
//			if !more {
//				done <- true
//				return
//			}
//
//			length := len(cells)
//			for i, c := range cells {
//				if i == 0 {
//					buf.WriteString("\n(")
//				}
//
//				if c.Type == "string" {
//					buf.WriteString(fmt.Sprintf("\"%s\"", c.Value))
//				} else {
//					buf.WriteString(fmt.Sprintf("%s", c.Value))
//				}
//
//				if i == length-1 {
//					buf.WriteString("),")
//				} else {
//					buf.WriteString(", ")
//				}
//			}
//		case <-done:
//			wg.Done()
//			return
//		}
//	}
//}
