package cmd

import (
	"github.com/hellofresh/klepto/pkg/anonymiser"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	// imports dumpers and reders
	_ "github.com/hellofresh/klepto/pkg/dumper/mysql"
	_ "github.com/hellofresh/klepto/pkg/dumper/postgres"
	_ "github.com/hellofresh/klepto/pkg/dumper/query"
	_ "github.com/hellofresh/klepto/pkg/reader/mysql"
	_ "github.com/hellofresh/klepto/pkg/reader/postgres"
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
func RunSteal(opts *StealOptions) (err error) {
	source, err := reader.Connect(opts.from)
	failOnError(err, "Error connecting to reader")

	source = anonymiser.NewAnonymiser(source, globalConfig.Tables)

	target, err := dumper.NewDumper(opts.to, source)
	failOnError(err, "Error creating dumper")

	log.Info("Stealing...")

	done := make(chan struct{})
	defer close(done)
	failOnError(target.Dump(done, globalConfig.Tables), "Error while dumping")

	<-done
	log.Info("Done!")

	return nil
}
