package cmd

import (
	"fmt"
	"runtime"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/hellofresh/klepto/pkg/anonymiser"
	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/hellofresh/klepto/pkg/replacer"

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
		toRDS       bool
		concurrency int
		readOpts    connOpts
		writeOpts   connOpts
		dataOnly    bool
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
			var err error
			opts.cfgTables, err = config.LoadFromFile(opts.configPath)
			if err != nil {
				return err
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunSteal(opts)
		},
	}

	persistentFlags := cmd.PersistentFlags()
	persistentFlags.StringVarP(&opts.configPath, "config", "c", config.DefaultConfigFileName, "Path to config file")
	persistentFlags.StringVarP(&opts.from, "from", "f", "mysql://root:root@tcp(localhost:3306)/klepto", "Database dsn to steal from")
	persistentFlags.StringVarP(&opts.to, "to", "t", "os://stdout/", "Database to output to (default writes to stdOut)")
	persistentFlags.BoolVar(&opts.toRDS, "to-rds", false, "If the output server is an AWS RDS server")
	persistentFlags.IntVar(&opts.concurrency, "concurrency", runtime.NumCPU(), "Sets the amount of dumps to be performed concurrently")
	persistentFlags.DurationVar(&opts.readOpts.timeout, "read-timeout", 5*time.Minute, "Sets the timeout for read operations")
	persistentFlags.DurationVar(&opts.readOpts.maxConnLifetime, "read-conn-lifetime", 0, "Sets the maximum amount of time a connection may be reused on the read database")
	persistentFlags.IntVar(&opts.readOpts.maxConns, "read-max-conns", 5, "Sets the maximum number of open connections to the read database")
	persistentFlags.IntVar(&opts.readOpts.maxIdleConns, "read-max-idle-conns", 0, "Sets the maximum number of connections in the idle connection pool for the read database")
	persistentFlags.DurationVar(&opts.writeOpts.timeout, "write-timeout", 30*time.Second, "Sets the timeout for write operations")
	persistentFlags.DurationVar(&opts.writeOpts.maxConnLifetime, "write-conn-lifetime", 0, "Sets the maximum amount of time a connection may be reused on the write database")
	persistentFlags.IntVar(&opts.writeOpts.maxConns, "write-max-conns", 5, "Sets the maximum number of open connections to the write database")
	persistentFlags.IntVar(&opts.writeOpts.maxIdleConns, "write-max-idle-conns", 0, "Sets the maximum number of connections in the idle connection pool for the write database")
	persistentFlags.BoolVar(&opts.dataOnly, "data-only", false, "Only steal data; requires that the target database structure already exists")

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
		return fmt.Errorf("could not connecting to reader: %w", err)
	}
	defer func() {
		if err := source.Close(); err != nil {
			log.WithError(err).Error("Something is not ok with closing source connection")
		}
	}()

	source = anonymiser.NewAnonymiser(source, opts.cfgTables)
	source = replacer.NewReplacer(source, opts.cfgTables)
	target, err := dumper.NewDumper(dumper.ConnOpts{
		DSN:             opts.to,
		IsRDS:           opts.toRDS,
		Timeout:         opts.writeOpts.timeout,
		MaxConnLifetime: opts.writeOpts.maxConnLifetime,
		MaxConns:        opts.writeOpts.maxConns,
		MaxIdleConns:    opts.writeOpts.maxIdleConns,
	}, source)
	if err != nil {
		return fmt.Errorf("error creating dumper: %w", err)
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
	if err := target.Dump(done, opts.cfgTables, opts.concurrency, opts.dataOnly); err != nil {
		return fmt.Errorf("error while dumping: %w", err)
	}

	<-done
	log.WithField("total_time", time.Since(start)).Info("Done!")

	return nil
}
