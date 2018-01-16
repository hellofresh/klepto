package cmd

import (
	"bufio"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/hellofresh/klepto/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// NewInitCmd creates a new init command
func NewInitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create a fresh config file herp derp this is a test",
		Run: func(cmd *cobra.Command, args []string) {
			RunInit()
		},
	}

	return cmd
}

// RunInit runs the init command
func RunInit() {
	log.Infof("Initializing %s", configFileName)

	_, err := os.Stat(configFileName)
	if !os.IsNotExist(err) {
		log.Fatal("Config file already exists, refusing to overwrite")
	}

	f, err := os.Create(configFileName)
	failOnError(err, "Could not create the file")

	e := toml.NewEncoder(bufio.NewWriter(f))
	err = e.Encode(config.Spec{
		Tables: []*config.Table{
			{
				Name: "orders",
				Filter: config.Filter{
					Limit: 100,
					Sorts: map[string]string{"orderNr": "asc"},
				},
				Anonymise: map[string]string{"firstName": "FirstName", "email": "EmailAddress"},
				Relationships: []*config.Relationship{
					{
						ReferencedTable: "customers",
						ReferencedKey:   "id",
						ForeignKey:      "customer_id",
					},
				},
			},
		},
	})
	failOnError(err, "Could not encode config")

	log.Infof("Created %s!", configFileName)
}
