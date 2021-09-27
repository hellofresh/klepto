package cmd

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/hellofresh/updater-go/v3"
	"github.com/palantir/stacktrace"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	githubOwner              = "hellofresh"
	githubRepo               = "klepto"
	defaultConnectionTimeout = time.Duration(5 * time.Second)
)

// UpdateOptions are the command flags
type UpdateOptions struct {
	token   string
	version string
	dryRun  bool
	timeout time.Duration
}

// NewUpdateCmd creates a new update command
func NewUpdateCmd() *cobra.Command {
	opts := new(UpdateOptions)
	cmd := &cobra.Command{
		Use:     "update",
		Aliases: []string{"self-update"},
		Short:   "Check for new versions of kepto",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunUpdate(cmd.Context(), opts)
		},
	}

	cmd.PersistentFlags().StringVar(&opts.token, "token", "", "Github token that will be used to check for Klepto! versions. If not set GITHUB_TOKEN environment variable value is used.")
	cmd.PersistentFlags().StringVar(&opts.version, "version", "", "Update to specific version instead of the latest stable.")
	cmd.PersistentFlags().BoolVar(&opts.dryRun, "dry-run", false, "Check the version available but do not run actual update")
	cmd.PersistentFlags().DurationVar(&opts.timeout, "timeout", defaultConnectionTimeout, "Connection timeout")

	return cmd
}

// RunUpdate runs the update command
func RunUpdate(ctx context.Context, opts *UpdateOptions) error {
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
	locator := newReleaseLocator(ctx, opts.token, versionFilter, opts.timeout)

	// Find the release
	updateTo, err := locateRelease(ctx, locator, updateToVersion)
	if rootErr := stacktrace.RootCause(err); rootErr == updater.ErrNoRepository {
		// fatal exits with code 1
		log.Fatal("Unable to access the Klepto! repository.")
	}
	if err != nil {
		return fmt.Errorf("failed to retrieve the update release: %w", err)
	}

	if updateTo.Name != version {
		// Fetch the release and update
		if !opts.dryRun {
			if err := updater.SelfUpdate(ctx, updateTo); err != nil {
				return fmt.Errorf("failed to update to version %s: %w", updateTo.Name, err)
			}
		}

		log.Infof("Klepto! updated to version %s", updateTo.Name)
	} else {
		log.Infof("No updates available for your version %s", version)
	}

	return nil
}

func newReleaseLocator(ctx context.Context, token string, filter updater.ReleaseFilter, timeout time.Duration) updater.ReleaseLocator {
	return updater.NewGithubClient(
		ctx,
		githubOwner,
		githubRepo,
		token,
		filter,
		func(asset string) bool {
			return strings.Contains(asset, fmt.Sprintf("_%s_%s", runtime.GOOS, runtime.GOARCH))
		},
		timeout,
	)
}
func locateRelease(ctx context.Context, locator updater.ReleaseLocator, version string) (updater.Release, error) {
	// No specific version use the latest
	if version == "" {
		return updater.LatestRelease(ctx, locator)
	}

	// Find a specific release
	var release updater.Release
	updates, err := locator.ListReleases(ctx, 1)
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
