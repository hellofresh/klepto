package reader

import (
	"errors"

	"github.com/hellofresh/klepto/pkg/database"
)

var (
	ErrUnsupportedDsn = errors.New("Unsupported dsn")
)

type (
	// Driver is a driver interface used to support multiple drivers
	Driver interface {
		IsSupported(dsn string) bool
		NewConnection(dsn string) (Reader, error)
	}

	// Reader provides an interface to access database stores.
	Reader interface {
		GetTables() ([]string, error)
		GetTableStructure(string) (string, error)
		GetColumns(string) ([]string, error)
		GetPreamble() (string, error)
		ReadTable(string, chan<- *database.Row) error
	}
)

func Connect(dsn string) (Reader, error) {
	driversMu.RLock()
	defer driversMu.RUnlock()

	for _, driver := range drivers {
		if !driver.IsSupported(dsn) {
			continue
		}

		return driver.NewConnection(dsn)
	}

	return nil, ErrUnsupportedDsn
}
