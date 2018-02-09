package dumper

import (
	"time"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type (
	// Driver is a driver interface used to support multiple drivers
	Driver interface {
		IsSupported(dsn string) bool
		NewConnection(ConnectionOpts, reader.Reader) (Dumper, error)
	}

	// A Dumper writes a database's stucture to the provided stream.
	Dumper interface {
		Dump(chan<- struct{}, config.Tables) error
		// Close closes the dumper resources and releases them.
		Close() error
	}

	// ConnectionOpts are the options to create a connection
	ConnectionOpts struct {
		DSN                string
		Timeout            time.Duration
		MaxConnLifetime    time.Duration
		MaxConnections     int
		MaxIdleConnections int
	}
)

// NewDumper is a factory method that will create a dumper based on the provided DSN
func NewDumper(opts ConnectionOpts, rdr reader.Reader) (dumper Dumper, err error) {
	drivers.Range(func(key, value interface{}) bool {
		driver, ok := value.(Driver)
		if !ok || !driver.IsSupported(opts.DSN) {
			return true
		}
		log.WithField("driver", key).Debug("Found driver")

		dumper, err = driver.NewConnection(opts, rdr)
		return false
	})

	if dumper == nil && err == nil {
		err = errors.New("no supported driver found")
	}

	err = errors.Wrapf(err, "could not create dumper for dsn: '%v'", opts.DSN)

	return
}
