package cmd

import (
	"time"

	"github.com/hellofresh/klepto/pkg/anonymiser"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	// imports dumpers and readers
	_ "github.com/hellofresh/klepto/pkg/dumper/mysql"
	_ "github.com/hellofresh/klepto/pkg/dumper/postgres"
	_ "github.com/hellofresh/klepto/pkg/dumper/query"
	_ "github.com/hellofresh/klepto/pkg/reader/mysql"
	_ "github.com/hellofresh/klepto/pkg/reader/postgres"
)

// StealOptions represents the command options
type StealOptions struct {
	from            string
	to              string
	timeout         string
	maxConnLifetime string
	maxConns        int
	maxIdleConns    int
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
	cmd.PersistentFlags().StringVarP(&opts.to, "to", "t", "os://stdout/", "Database to output to (default writes to stdOut)")

	cmd.PersistentFlags().StringVar(&opts.timeout, "timeout", "30s", "Sets the timeout for all the operations")
	cmd.PersistentFlags().StringVar(&opts.maxConnLifetime, "conn-lifetime", "0", "Sets the maximum amount of time a connection may be reused")
	cmd.PersistentFlags().IntVarP(&opts.maxConns, "max-conns", "m", 10, "Sets the maximum number of open connections to the database")
	cmd.PersistentFlags().IntVarP(&opts.maxIdleConns, "max-idle-conns", "i", 0, "Sets the maximum number of connections in the idle connection pool")

	return cmd
}

// RunSteal is the handler for the rootCmd.
func RunSteal(opts *StealOptions) (err error) {
	timeout, err := time.ParseDuration(opts.timeout)
	failOnError(err, "Failed to parse the timeout duration")

	maxConnLifetime, err := time.ParseDuration(opts.maxConnLifetime)
	failOnError(err, "Failed to parse the timeout duration")

	source, err := reader.Connect(reader.ConnOpts{
		DSN:             opts.from,
		Timeout:         timeout,
		MaxConnLifetime: maxConnLifetime,
		MaxConns:        opts.maxConns,
		MaxIdleConns:    opts.maxIdleConns,
	})
	failOnError(err, "Error connecting to reader")
	defer source.Close()

	source = anonymiser.NewAnonymiser(source, globalConfig.Tables)
	target, err := dumper.NewDumper(dumper.ConnOpts{
		DSN:          opts.to,
		Timeout:      timeout,
		MaxConns:     opts.maxConns,
		MaxIdleConns: opts.maxIdleConns,
	}, source)
	failOnError(err, "Error creating dumper")
	defer target.Close()

	log.Info("Stealing...")

	done := make(chan struct{})
	defer close(done)
	failOnError(target.Dump(done, globalConfig.Tables), "Error while dumping")

	<-done
	log.Info("Done!")

	return nil
}
