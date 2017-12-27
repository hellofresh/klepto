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
