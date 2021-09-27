package cmd

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/hellofresh/klepto/pkg/config"
)

// NewInitCmd creates a new init command
func NewInitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create a fresh config file",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunInit()
		},
	}

	return cmd
}

// RunInit runs the init command
func RunInit() error {
	log.Infof("Initializing %s", config.DefaultConfigFileName)

	_, err := os.Stat(config.DefaultConfigFileName)
	if !os.IsNotExist(err) {
		log.Fatal("Config file already exists, refusing to overwrite")
	}

	f, err := os.Create(config.DefaultConfigFileName)
	if err != nil {
		return fmt.Errorf("could not create file: %w", err)
	}

	if err := config.WriteSample(f); err != nil {
		return fmt.Errorf("could not write config: %w", err)
	}

	log.Infof("Created %s!", config.DefaultConfigFileName)

	return nil
}
