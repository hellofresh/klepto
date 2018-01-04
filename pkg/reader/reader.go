package reader

import (
	"github.com/hellofresh/klepto/pkg/database"
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
		// FormatColumn returns a escaped table.column string
		FormatColumn(tableName string, columnName string) string
		// ReadTable returns a channel with all database rows
		ReadTable(string, chan<- database.Row, ReadTableOpt) error
		// Close closes the reader resources and releases them.
		Close() error
	}

	ReadTableOpt struct {
		Limit         uint64
		Relationships []*RelationshipOpt
	}

	RelationshipOpt struct {
		ReferencedTable string
		ReferencedKey   string
		ForeignKey      string
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

	return
}
