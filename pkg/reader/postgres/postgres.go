package postgres

import (
	"database/sql"
	"strings"

	"github.com/usoban/klepto/pkg/reader"
	_ "github.com/lib/pq"
)

type driver struct{}

// IsSupported checks if the postgres driver is supported.
func (m *driver) IsSupported(dsn string) bool {
	return strings.HasPrefix(strings.ToLower(dsn), "postgres://")
}

// NewConnection takes the connection options and returns a new Reader.
func (m *driver) NewConnection(opts reader.ConnOpts) (reader.Reader, error) {
	conn, err := sql.Open("postgres", opts.DSN)
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(opts.MaxConns)
	conn.SetMaxIdleConns(opts.MaxIdleConns)
	conn.SetConnMaxLifetime(opts.MaxConnLifetime)

	dumper, err := NewPgDump(opts.DSN)
	if err != nil {
		return nil, err
	}

	return NewStorage(conn, dumper, opts.Timeout), nil
}

func init() {
	reader.Register("postgres", &driver{})
}
