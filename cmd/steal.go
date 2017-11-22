package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"

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
	// TODO: do something interesting with the keys. a.k.a. resolve dependencies :)
	_ = findForeignKeys(fromDSN)

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
	tableBuffers = dumper.WaitGroupBufferer()

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

// findForeignKeys connects to information_schema and retrieves all foreign keys.
func findForeignKeys(dsn string) (err error) {
	if pRecordType == "" {
		return
	}

	// append 'information_schema' to trimmed dsn.
	r, err := trimDBname(fromDSN)
	if err != nil {
		return err
	}
	iSchemaDsn := fmt.Sprintf("%s%s", r, "information_schema")
	iSchemaConn, err := database.Connect(iSchemaDsn)
	if err != nil {
		color.Red("Error connecting to information_schema: %s", err.Error())
		return err
	}
	defer iSchemaConn.Close()

	inputConn, err := database.Connect(fromDSN)
	if err != nil {
		color.Red("Error connecting to input database: %s", err.Error())
		return err
	}

	iStore := database.NewiSchemaStorage(iSchemaConn, inputConn)
	res, err := iStore.Relationships()
	fmt.Printf("%+v \n", res)
	if err != nil {
		color.Red("Error retrieving relationships from information_schema: %s", err.Error())
		return err
	}

	return
}

// trim strips db name from the dsn
func trimDBname(dsn string) (string, error) {
	r, err := regexp.Compile(".*/")
	if err != nil {
		return "", err
	}
	return r.FindString(dsn), nil

}
