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
		// GetTables returns a list of all databases tables
		GetTables() ([]string, error)
		// GetColumns return a list of all columns for a given table
		GetColumns(string) ([]string, error)
		// GetStructure returns the SQL used to create the database tables
		GetStructure() (string, error)
		GetPreamble() (string, error)
		// ReadTable returns a channel with all database rows
		ReadTable(string, chan<- database.Row) error
		// Close will close any connection/file etc.
		Close() error
	}
)

func Connect(dsn string) (reader Reader, err error) {
	drivers.Range(func(key, value interface{}) bool {
		driver, _ := value.(Driver)

		if !driver.IsSupported(dsn) {
			return true
		}

		reader, err = driver.NewConnection(dsn)
		return false
	})

	if reader == nil {
		err = ErrUnsupportedDsn
	}

	return
}
