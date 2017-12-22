package dumper

import (
	"errors"

	"github.com/hellofresh/klepto/pkg/reader"
)

var (
	ErrUnsupportedDsn = errors.New("Unsupported dsn")
)

type (
	// Driver is a driver interface used to support multiple drivers
	Driver interface {
		IsSupported(dsn string) bool
		NewConnection(string, reader.Reader) (Dumper, error)
	}

	// A Dumper writes a database's stucture to the provided stream.
	Dumper interface {
		Dump() error
	}
)

func NewDumper(dsn string, rdr reader.Reader) (Dumper, error) {
	driversMu.RLock()
	defer driversMu.RUnlock()

	for _, driver := range drivers {
		if !driver.IsSupported(dsn) {
			continue
		}

		return driver.NewConnection(dsn, rdr)
	}

	return nil, ErrUnsupportedDsn
}
