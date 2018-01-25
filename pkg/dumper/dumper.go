package dumper

import (
	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type (
	// Driver is a driver interface used to support multiple drivers
	Driver interface {
		IsSupported(dsn string) bool
		NewConnection(string, reader.Reader) (Dumper, error)
	}

	// A Dumper writes a database's stucture to the provided stream.
	Dumper interface {
		Dump(chan<- struct{}, config.Tables) error
		// Close closes the dumper resources and releases them.
		Close() error
	}
)

// NewDumper is a factory method that will create a dumper based on the provided DSN
func NewDumper(dsn string, rdr reader.Reader) (dumper Dumper, err error) {
	drivers.Range(func(key, value interface{}) bool {
		driver, _ := value.(Driver)
		if !driver.IsSupported(dsn) {
			return true
		}
		log.WithField("driver", key).Debug("Found driver")

		dumper, err = driver.NewConnection(dsn, rdr)
		return false
	})

	if dumper == nil && err == nil {
		err = errors.New("no supported driver found")
	}

	err = errors.Wrapf(err, "could not create dumper for dsn: '%v'", dsn)

	return
}
