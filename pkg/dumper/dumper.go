package dumper

import (
	"fmt"
	"time"

	wErrors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/reader"
)

type (
	// Driver is a driver interface used to support multiple drivers
	Driver interface {
		// IsSupported checks if the given dsn connection string is supported.
		IsSupported(dsn string) bool
		// NewConnection creates a new database connection and retrieves a dumper implementation.
		NewConnection(ConnOpts, reader.Reader) (Dumper, error)
	}

	// A Dumper writes a database's structure to the provided stream.
	Dumper interface {
		// Dump executes the dump process.
		Dump(chan<- struct{}, config.Tables, int) error
		// Close closes the dumper resources and releases them.
		Close() error
	}

	// ConnOpts are the options to create a connection
	ConnOpts struct {
		// DSN is the connection address.
		DSN string
		// Timeout is the timeout for dump operations.
		Timeout time.Duration
		// MaxConnLifetime is the maximum amount of time a connection may be reused on the read database.
		MaxConnLifetime time.Duration
		// MaxConns is the maximum number of open connections to the target database.
		MaxConns int
		// MaxIdleConns is the maximum number of connections in the idle connection pool for the write database.
		MaxIdleConns int
	}
)

// NewDumper is a factory method that will create a dumper based on the provided DSN
func NewDumper(opts ConnOpts, rdr reader.Reader) (dumper Dumper, err error) {
	drivers.Range(func(key, value interface{}) bool {
		driver, ok := value.(Driver)
		if !ok || !driver.IsSupported(opts.DSN) {
			return true
		}
		log.WithField("driver", key).Debug("found driver")

		dumper, err = driver.NewConnection(opts, rdr)
		return false
	})

	if dumper == nil && err == nil {
		err = fmt.Errorf("no supported driver found for dumper DSN %q", opts.DSN)
	}

	err = wErrors.Wrapf(err, "could not create dumper for DSN: %q", opts.DSN)

	return
}
