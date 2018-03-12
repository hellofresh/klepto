package cmd

import (
	"github.com/italolelis/goupdater"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	githubOwner = "hellofresh"
	githubRepo  = "klepto"
)

type (
	// UpdateOptions are the command flags
	UpdateOptions struct {
		token string
	}
)

// NewUpdateCmd creates a new update command
func NewUpdateCmd() *cobra.Command {
	opts := new(UpdateOptions)
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Check for new versions of kepto",
		Run: func(cmd *cobra.Command, args []string) {
			RunUpdate(opts)
		},
	}

	cmd.PersistentFlags().StringVar(&opts.token, "token", "", "the github token that will be used to check for new versions")

	return cmd
}

// RunUpdate runs the update command
func RunUpdate(opts *UpdateOptions) {
	resolver, err := goupdater.NewGithub(opts.token, githubOwner, githubRepo)
	failOnError(err, "could not create the updater client")

	updated, err := goupdater.Update(resolver, version)
	failOnError(err, "could not update binary")

	if updated {
		log.Info("You are now using the latest version of klepto")
	} else {
		log.Info("You already have the latest version of klepto")
	}
}
