package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/fatih/color"
	"github.com/gernest/wow"
	"github.com/gernest/wow/spin"
	"github.com/hellofresh/klepto/database"
	"github.com/hellofresh/klepto/database/mysql"
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

	spinner := wow.New(os.Stdout, spin.Get(spin.Smiley), " Stealing...")
	spinner.Start()

	store := database.NewStorage(inputConn)
	anonyimiser := mysql.NewAnonymiser(store)
	dumper := mysql.NewDumper(store, anonyimiser)
	structure, err := dumper.DumpStructure()
	if err != nil {
		color.Red("Error dumping database structure: %s", err.Error())
		return
	}
	var tableBuffers []*bytes.Buffer
	tableBuffers = dumper.DumpInserts()

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
