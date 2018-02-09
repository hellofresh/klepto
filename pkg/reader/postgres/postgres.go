package postgres

import (
	"database/sql"
	"strings"

	"github.com/hellofresh/klepto/pkg/reader"
	_ "github.com/lib/pq"
)

type driver struct{}

func (m *driver) IsSupported(dsn string) bool {
	return strings.HasPrefix(strings.ToLower(dsn), "postgres://")
}

func (m *driver) NewConnection(opts reader.ConnOpts) (reader.Reader, error) {
	conn, err := sql.Open("postgres", opts.DSN)
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(opts.MaxConns)
	conn.SetMaxIdleConns(opts.MaxIdleConns)
	conn.SetConnMaxLifetime(opts.MaxConnLifetime)

	dump, err := NewPgDump(opts.DSN)
	if err != nil {
		return nil, err
	}

	return NewStorage(conn, dump), nil
}

func init() {
	reader.Register("postgres", &driver{})
}
