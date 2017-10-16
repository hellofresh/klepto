package cmd

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
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
	go func() {
		for {
			var sql string
			cells, more := <-out
			if !more {
				done <- true
				return
			}

			len := len(cells)
			for i, c := range cells {
				if i == 0 {
					sql += "("
				}

				if i == len-1 {
					sql += fmt.Sprintf("\"%s\"),", c.Value)
				} else {
					sql += fmt.Sprintf("\"%s\", ", c.Value)
				}
			}

			fmt.Println(sql)
		}
	}()

	tables, err := dumper.GetTables()
	if err != nil {
		color.Red("Error stealing data: %s", err.Error())
	}

	for _, table := range tables {
		columns, err := dumper.GetColumns(table)
		fmt.Printf("\nINSERT INTO `%s` (%s) VALUES\n", table, strings.Join(columns, ", "))

		anonymiser := database.NewMySQLAnonymiser(inputConn)
		err = anonymiser.DumpTable(table, out)
		if err != nil {
			color.Red("Error stealing data: %s", err.Error())
			return
		}
	}

	close(out)
	<-done

	// outputConn, err := dbConnect(*toDSN)
	// if err != nil {
	// 	return err
	// }
}
