package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/hellofresh/klepto/pkg/formatter"
)

var (
	verbose bool

	// RootCmd steals and anonymises databases
	RootCmd = &cobra.Command{
		Use:   "klepto",
		Short: "Steals and anonymises databases",
		Long: `Klepto by HelloFresh.
		Takes the structure and data from one (mysql) database (--from),
		anonymises the data according to the provided configuration file,
		and inserts that data into another mysql database (--to).
	
		Perfect for bringing your live data to staging!`,
		Example: "klepto steal -c .klepto.toml|yaml|json --from root:root@localhost:3306/fromDb --to root:root@localhost:3306/toDb",
	}
)

func init() {
	RootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Make the operation more talkative")
	RootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		if verbose {
			log.SetLevel(log.DebugLevel)
		}
	}

	RootCmd.AddCommand(NewVersionCmd())
	RootCmd.AddCommand(NewUpdateCmd())
	RootCmd.AddCommand(NewInitCmd())
	RootCmd.AddCommand(NewStealCmd())

	log.SetOutput(os.Stderr)
	log.SetFormatter(&formatter.CliFormatter{})
}
