package reader

import (
	"time"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
)

type (
	// Driver is a driver interface used to support multiple drivers
	Driver interface {
		IsSupported(string) bool
		NewConnection(ConnOpts) (Reader, error)
	}

	// Reader provides an interface to access database stores.
	Reader interface {
		// GetStructure returns the SQL used to create the database tables
		GetStructure() (string, error)
		// GetTables returns a list of all databases tables
		GetTables() ([]string, error)
		// GetColumns return a list of all columns for a given table
		GetColumns(string) ([]string, error)
		// FormatColumn returns a escaped table.column string
		FormatColumn(tableName string, columnName string) string
		// ReadTable returns a channel with all database rows
		ReadTable(string, chan<- database.Row, ReadTableOpt, config.Matchers) error
		// Close closes the reader resources and releases them.
		Close() error
	}

	// ReadTableOpt represents the read table options
	ReadTableOpt struct {
		// Columns contains the (quoted) column of the table
		Columns []string
		// Match is a condition field to dump only certain amount data
		Match string
		// Sort the results
		Sorts map[string]string
		// Defines a limit of results to be fetched
		Limit uint64
		// Relationships defines an slice of relationship definitions
		Relationships []*RelationshipOpt
	}

	// RelationshipOpt represents the relationships options
	RelationshipOpt struct {
		Table           string
		ForeignKey      string
		ReferencedTable string
		ReferencedKey   string
	}

	// ConnOpts are the options to create a connection
	ConnOpts struct {
		DSN             string
		Timeout         time.Duration
		MaxConnLifetime time.Duration
		MaxConns        int
		MaxIdleConns    int
	}
)

// Connect acts as fectory method that returns a reader from a DSN
func Connect(opts ConnOpts) (reader Reader, err error) {
	drivers.Range(func(key, value interface{}) bool {
		driver, ok := value.(Driver)
		if !ok || !driver.IsSupported(opts.DSN) {
			return true
		}

		reader, err = driver.NewConnection(opts)
		return false
	})

	return
}
