package cmd

import (
	"os/user"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/formatter"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var (
	globalConfig   *config.Spec
	configFile     string
	configFileName = ".klepto.toml"
	verbose        bool

	// RootCmd steals and anonymises databases
	RootCmd = &cobra.Command{
		Use:   "klepto",
		Short: "Steals and anonymises databases",
		Long: `Klepto by HelloFresh.
		Takes the structure and data from one (mysql) database (--from),
		anonymises the data according to the provided configuration file,
		and inserts that data into another mysql database (--to).
	
		Perfect for bringing your live data to staging!`,
		Example: "klepto steal -c klepto.toml --from root:root@localhost:3306/example --to root:root@localhost:3306/klepto",
	}
)

func init() {
	RootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "Path to config file (default is $HOME/.klepto.toml)")

	RootCmd.AddCommand(NewStealCmd())
	RootCmd.AddCommand(NewVersionCmd())
	RootCmd.AddCommand(NewUpdateCmd())
	RootCmd.AddCommand(NewInitCmd())

	log.SetFormatter(&formatter.CliFormatter{})
}

func initConfig(c *cobra.Command, args []string) error {
	if verbose {
		log.SetLevel(log.DebugLevel)
	}

	log.Debugf("Reading config from %s...", configFileName)

	if configFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName(".klepto")
		viper.AddConfigPath(workingDir())
		viper.AddConfigPath(".")
	}

	err := viper.ReadInConfig()
	if err != nil {
		return errors.Wrap(err, "Could not read configurations")
	}

	err = viper.Unmarshal(&globalConfig)
	if err != nil {
		return errors.Wrap(err, "Could not unmarshal config file")
	}

	return nil
}
func workingDir() string {
	cwd, err := os.Getwd()
	failOnError(err, "Can't find the working directory")

	return cwd
}

func failOnError(err error, message string) {
	if err != nil {
		log.WithError(err).Fatal(message)
	}
}
