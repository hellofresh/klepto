package cmd

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var configFile string
var fromDSN string
var toDSN string
var nRows string

var Klepto = &cobra.Command{
	Use:   "klepto",
	Short: "Steals and anonymises databases",
	Long: `Klepto by HelloFresh.
	Takes the structure and data from one (mysql) database (--from),
	anonymises the data according to the provided configuration file,
	and inserts that data into another mysql database (--to).

	Perfect for bringing your live data to staging!`,
	Example: "klepto --config klepto.toml --from root:root@localhost:3306/example --to root:root@localhost:3306/klepto",
	Run:     RunSteal,
}

func init() {
	cobra.OnInitialize(initConfig)
	Klepto.PersistentFlags().StringVarP(&configFile, "config", "c", "", "Path to config file (default is $HOME/.klepto.toml)")
	Klepto.PersistentFlags().StringVarP(&fromDSN, "from", "f", "root:root@tcp(localhost:3306)/klepto", "MySQL database dsn to steal from")
	Klepto.PersistentFlags().StringVarP(&toDSN, "to", "t", "", "MySQL database to output to (default writes to stdOut)")
	Klepto.PersistentFlags().StringVarP(&nRows, "rows", "r", "10000", "Number of rows you want to steal")
	viper.BindPFlag("fromDSN", Klepto.PersistentFlags().Lookup("from"))
	viper.BindPFlag("toDSN", Klepto.PersistentFlags().Lookup("to"))
	viper.BindPFlag("nRows", Klepto.PersistentFlags().Lookup("rows"))
}

func initConfig() {
	if configFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(configFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			color.Red("Can't read home directory: %s", err)
			os.Exit(1)
		}

		viper.SetConfigFile(fmt.Sprintf("%s/.klepto.toml", home))
	}

	if err := viper.ReadInConfig(); err != nil {
		color.Red("Can't read config: %s", err)
		os.Exit(1)
	}
}
