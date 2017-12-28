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
		Dump(chan<- struct{}) error
		Close() error
	}
)

func NewDumper(dsn string, rdr reader.Reader) (dumper Dumper, err error) {
	drivers.Range(func(key, value interface{}) bool {
		driver, _ := value.(Driver)

		if !driver.IsSupported(dsn) {
			return true
		}

		dumper, err = driver.NewConnection(dsn, rdr)
		return false
	})

	if dumper == nil {
		err = ErrUnsupportedDsn
	}

	return
}
