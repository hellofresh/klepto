package cmd

import (
	"runtime"
	"time"

	wErrors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/hellofresh/klepto/pkg/anonymiser"
	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"

	// imports dumpers and readers
	_ "github.com/hellofresh/klepto/pkg/dumper/mysql"
	_ "github.com/hellofresh/klepto/pkg/dumper/postgres"
	_ "github.com/hellofresh/klepto/pkg/dumper/query"
	_ "github.com/hellofresh/klepto/pkg/reader/mysql"
	_ "github.com/hellofresh/klepto/pkg/reader/postgres"
)

type (
	// StealOptions represents the command options
	StealOptions struct {
		configPath string
		cfgTables  config.Tables

		from        string
		to          string
		concurrency int
		readOpts    connOpts
		writeOpts   connOpts
	}
	connOpts struct {
		timeout         time.Duration
		maxConnLifetime time.Duration
		maxConns        int
		maxIdleConns    int
	}
)

// NewStealCmd creates a new steal command
func NewStealCmd() *cobra.Command {
	opts := new(StealOptions)
	cmd := &cobra.Command{
		Use:   "steal",
		Short: "Steals and anonymises databases",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			cfgTables, err := config.LoadSpecFromFile(opts.configPath)
			if err != nil {
				return err
			}

			opts.cfgTables = cfgTables
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunSteal(opts)
		},
	}

	cmd.PersistentFlags().StringVarP(&opts.configPath, "config", "c", config.DefaultConfigFileName, "Path to config file")
	cmd.PersistentFlags().StringVarP(&opts.from, "from", "f", "mysql://root:root@tcp(localhost:3306)/klepto", "Database dsn to steal from")
	cmd.PersistentFlags().StringVarP(&opts.to, "to", "t", "os://stdout/", "Database to output to (default writes to stdOut)")
	cmd.PersistentFlags().IntVar(&opts.concurrency, "concurrency", runtime.NumCPU(), "Sets the amount of dumps to be performed concurrently")
	cmd.PersistentFlags().DurationVar(&opts.readOpts.timeout, "read-timeout", 5*time.Minute, "Sets the timeout for read operations")
	cmd.PersistentFlags().DurationVar(&opts.readOpts.maxConnLifetime, "read-conn-lifetime", 0, "Sets the maximum amount of time a connection may be reused on the read database")
	cmd.PersistentFlags().IntVar(&opts.readOpts.maxConns, "read-max-conns", 5, "Sets the maximum number of open connections to the read database")
	cmd.PersistentFlags().IntVar(&opts.readOpts.maxIdleConns, "read-max-idle-conns", 0, "Sets the maximum number of connections in the idle connection pool for the read database")
	cmd.PersistentFlags().DurationVar(&opts.writeOpts.timeout, "write-timeout", 30*time.Second, "Sets the timeout for write operations")
	cmd.PersistentFlags().DurationVar(&opts.writeOpts.maxConnLifetime, "write-conn-lifetime", 0, "Sets the maximum amount of time a connection may be reused on the write database")
	cmd.PersistentFlags().IntVar(&opts.writeOpts.maxConns, "write-max-conns", 5, "Sets the maximum number of open connections to the write database")
	cmd.PersistentFlags().IntVar(&opts.writeOpts.maxIdleConns, "write-max-idle-conns", 0, "Sets the maximum number of connections in the idle connection pool for the write database")

	return cmd
}

// RunSteal is the handler for the rootCmd.
func RunSteal(opts *StealOptions) (err error) {
	source, err := reader.Connect(reader.ConnOpts{
		DSN:             opts.from,
		Timeout:         opts.readOpts.timeout,
		MaxConnLifetime: opts.readOpts.maxConnLifetime,
		MaxConns:        opts.readOpts.maxConns,
		MaxIdleConns:    opts.readOpts.maxIdleConns,
	})
	if err != nil {
		return wErrors.Wrap(err, "Could not connecting to reader")
	}
	defer func() {
		if err := source.Close(); err != nil {
			log.WithError(err).Error("Something is not ok with closing source connection")
		}
	}()

	source = anonymiser.NewAnonymiser(source, opts.cfgTables)
	target, err := dumper.NewDumper(dumper.ConnOpts{
		DSN:             opts.to,
		Timeout:         opts.writeOpts.timeout,
		MaxConnLifetime: opts.writeOpts.maxConnLifetime,
		MaxConns:        opts.writeOpts.maxConns,
		MaxIdleConns:    opts.writeOpts.maxIdleConns,
	}, source)
	if err != nil {
		return wErrors.Wrap(err, "Error creating dumper")
	}
	defer func() {
		if err := target.Close(); err != nil {
			log.WithError(err).Error("Something is not ok with closing target connection")
		}
	}()

	log.Info("Stealing...")

	done := make(chan struct{})
	defer close(done)

	start := time.Now()
	if err := target.Dump(done, opts.cfgTables, opts.concurrency); err != nil {
		return wErrors.Wrap(err, "Error while dumping")
	}

	<-done
	log.WithField("total_time", time.Since(start)).Info("Done!")

	return nil
}
