package cmd

import (
	"fmt"
	"os"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var configFile string
var fromDSN string
var toDSN string

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
	viper.BindPFlag("fromDSN", Klepto.PersistentFlags().Lookup("from"))
	viper.BindPFlag("toDSN", Klepto.PersistentFlags().Lookup("to"))
}

func initConfig() {
	// Don't forget to read config either from cfgFile or from home directory!
	if configFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(configFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		viper.SetConfigFile(fmt.Sprintf("%s/.klepto.toml", home))
	}

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("Can't read config:", err)
		os.Exit(1)
	}
}
