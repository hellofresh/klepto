package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/fatih/color"
	"github.com/gernest/wow"
	"github.com/gernest/wow/spin"
	"github.com/hellofresh/klepto/database"
	"github.com/hellofresh/klepto/database/anonymiser"
	"github.com/spf13/cobra"
)

// RunSteal is the handler for the rootCmd.
func RunSteal(cmd *cobra.Command, args []string) {
	inputConn, err := database.Connect(fromDSN)
	if err != nil {
		color.Red("Error connecting to input database: %s", err.Error())
		return
	}
	defer inputConn.Close()

	dumper := database.NewMySQLDumper(inputConn)
	structure, err := dumper.DumpStructure()
	if err != nil {
		color.Red("Error connecting to input database: %s", err.Error())
		return
	}

	out := make(chan []*database.Cell, 1000)
	done := make(chan bool)
	tables, err := dumper.GetTables()
	if err != nil {
		color.Red("Error stealing data: %s", err.Error())
	}

	var wg sync.WaitGroup
	spinner := wow.New(os.Stdout, spin.Get(spin.Smiley), " Stealing...")
	spinner.Start()

	var tableBuffers []*bytes.Buffer

	anonymiser := anonymiser.NewMySQLAnonymiser(inputConn)

	tableBuffers = waitGroupBufferer(tables, anonymiser, dumper, out, done, &wg)

	close(out)
	spinner.Stop()

	// Output everything
	fmt.Print(structure)
	for _, tbl := range tableBuffers {
		io.Copy(os.Stdout, tbl)
	}

	// outputConn, err := dbConnect(*toDSN)
	// if err != nil {
	// 	return err
	// }
}

func bufferer(buf *bytes.Buffer, rowChan chan []*database.Cell, done chan bool, wg *sync.WaitGroup) {
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

func waitGroupBufferer(tables []string, anonymiser *anonymiser.MySQLAnonymiser, dumper *database.MySQLDumper, out chan []*database.Cell, done chan bool, wg *sync.WaitGroup) []*bytes.Buffer {

	var tableBuffers []*bytes.Buffer
	for _, table := range tables {
		columns, err := dumper.GetColumns(table)
		buf := bytes.NewBufferString(fmt.Sprintf("\nINSERT INTO `%s` (%s) VALUES", table, strings.Join(columns, ", ")))

		wg.Add(1)
		go bufferer(buf, out, done, wg)

		err = anonymiser.DumpTable(table, out, done)
		if err != nil {
			color.Red("Error stealing data: %s", err.Error())
			return tableBuffers
		}

		wg.Wait()

		b := buf.Bytes()
		b = b[:len(b)-1]
		b = append(b, []byte(";")...)
		tableBuffers = append(tableBuffers, buf)
	}

	return tableBuffers
}
