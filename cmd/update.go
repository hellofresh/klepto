package cmd

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/hellofresh/updater-go"
	"github.com/palantir/stacktrace"
	wErrors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	githubOwner = "hellofresh"
	githubRepo  = "klepto"
)

// UpdateOptions are the command flags
type UpdateOptions struct {
	token   string
	version string
	dryRun  bool
}

// NewUpdateCmd creates a new update command
func NewUpdateCmd() *cobra.Command {
	opts := new(UpdateOptions)
	cmd := &cobra.Command{
		Use:     "update",
		Aliases: []string{"self-update"},
		Short:   "Check for new versions of kepto",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunUpdate(opts)
		},
	}

	cmd.PersistentFlags().StringVar(&opts.token, "token", "", "Github token that will be used to check for Klepto! versions. If not set GITHUB_TOKEN environment variable value is used.")
	cmd.PersistentFlags().StringVar(&opts.version, "version", "", "Update to specific version instead of the latest stable.")
	cmd.PersistentFlags().BoolVar(&opts.dryRun, "dry-run", false, "check the version available but do not run actual update")

	return cmd
}

// RunUpdate runs the update command
func RunUpdate(opts *UpdateOptions) error {
	log.Info("Checking for new versions of Klepto!")

	if opts.token == "" {
		opts.token = os.Getenv("GITHUB_TOKEN")
	}

	// Check to which version we need to update
	versionFilter := updater.StableRelease
	updateToVersion := opts.version
	if updateToVersion != "" {
		versionFilter = func(name string, _ bool, _ bool) bool {
			return updateToVersion == name
		}
	}

	// Create release locator
	locator := newReleaseLocator(opts.token, versionFilter)

	// Find the release
	updateTo, err := locateRelease(locator, updateToVersion)
	if rootErr := stacktrace.RootCause(err); rootErr == updater.ErrNoRepository {
		// fatal exits with code 1
		log.Fatal("Unable to access the Klepto! repository.")
	}
	if err != nil {
		return wErrors.Wrap(err, "failed to retrieve the update release")
	}

	if updateTo.Name != version {
		// Fetch the release and update
		if !opts.dryRun {
			if err := updater.SelfUpdate(updateTo); err != nil {
				return wErrors.Wrapf(err, "failed to update to version %s", updateTo.Name)
			}
		}

		log.Infof("Klepto! updated to version %s", updateTo.Name)
	} else {
		log.Infof("No updates available for your version %s", version)
	}

	return nil
}

func newReleaseLocator(token string, filter updater.ReleaseFilter) updater.ReleaseLocator {
	return updater.NewGithubClient(
		githubOwner,
		githubRepo,
		token,
		filter,
		func(asset string) bool {
			return strings.Contains(asset, fmt.Sprintf("_%s_%s", runtime.GOOS, runtime.GOARCH))
		},
	)
}
func locateRelease(locator updater.ReleaseLocator, version string) (updater.Release, error) {
	// No specific version use the latest
	if version == "" {
		return updater.LatestRelease(locator)
	}

	// Find a specific release
	var release updater.Release
	updates, err := locator.ListReleases(1)
	if err != nil {
		return release, err
	}

	if len(updates) == 0 {
		return release, fmt.Errorf("unable to locate release %s", version)
	}

	if len(updates) > 1 {
		return release, fmt.Errorf("multiple releases locate for %s", version)
	}

	return updates[0], nil
}
