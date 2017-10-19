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
	"github.com/spf13/cobra"
)

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
	cmd.Print(structure)

	out := make(chan []*database.Cell, 1000)
	done := make(chan bool)
	tables, err := dumper.GetTables()
	if err != nil {
		color.Red("Error stealing data: %s", err.Error())
	}

	var wg sync.WaitGroup
	w := wow.New(os.Stdout, spin.Get(spin.Smiley), " Stealing...")
	for _, table := range tables {
		columns, err := dumper.GetColumns(table)
		buf := bytes.NewBufferString(fmt.Sprintf("\nINSERT INTO `%s` (%s) VALUES", table, strings.Join(columns, ", ")))

		wg.Add(1)
		go bufferer(buf, out, done, &wg)

		anonymiser := database.NewMySQLAnonymiser(inputConn)
		err = anonymiser.DumpTable(table, out, done)
		if err != nil {
			color.Red("Error stealing data: %s", err.Error())
			return
		}

		w.Start()
		wg.Wait()
		w.Stop()

		// TODO: How 2 buf.TrimSuffix(",")
		buf.WriteString(";\n")
		io.Copy(os.Stdout, buf)
	}

	<-done
	close(out)
	os.Exit(0)

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
				wg.Done()
				return
			}

			len := len(cells)
			for i, c := range cells {
				if i == 0 {
					buf.WriteString("\n(")
				}

				if i == len-1 {
					buf.WriteString(fmt.Sprintf("\"%s\"),", c.Value))
				} else {
					buf.WriteString(fmt.Sprintf("\"%s\", ", c.Value))
				}
			}
		case <-done:
			wg.Done()
			return
		}
	}
}
