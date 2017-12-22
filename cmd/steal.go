package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/database/mysql"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// StealOptions represents the command options
type StealOptions struct {
	from string
	to   string
	rows int
}

// NewStealCmd creates a new steal command
func NewStealCmd() *cobra.Command {
	opts := &StealOptions{}

	cmd := &cobra.Command{
		Use:     "steal",
		Short:   "Steals and anonymises databases",
		PreRunE: initConfig,
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunSteal(opts)
		},
	}

	cmd.PersistentFlags().StringVarP(&opts.from, "from", "f", "root:root@tcp(localhost:3306)/klepto", "Database dsn to steal from")
	cmd.PersistentFlags().StringVarP(&opts.to, "to", "t", "", "Database to output to (default writes to stdOut)")
	cmd.PersistentFlags().IntVarP(&opts.rows, "number", "n", 1000, "Number of rows you want to steal")

	return cmd
}

// RunSteal is the handler for the rootCmd.
func RunSteal(opts *StealOptions) error {

	inputConn, err := database.Connect(opts.from)
	if err != nil {
		log.WithError(err).Fatal("Error connecting to input database")
	}

	defer inputConn.Close()

	log.Info("Stealing...")

	configReader := database.NewConfigReader(viper.GetViper())
	store := database.NewStorage(inputConn, *configReader)
	anonyimiser := mysql.NewAnonymiser(store)
	dumper := mysql.NewDumper(store, anonyimiser, *configReader)
	structure, err := dumper.DumpStructure()
	if err != nil {
		return errors.Wrap(err, "Error dumping database structure")
	}

	var tableBuffers []*bytes.Buffer
	tableBuffers = dumper.WaitGroupBufferer()

	// Output everything
	fmt.Print(structure)
	for _, tbl := range tableBuffers {
		io.Copy(os.Stdout, tbl)
	}
	// outputConn, err := dbConnect(*toDSN)
	// if err != nil {
	// 	return err
	// }

	log.Info("Done!")
	return nil
}
