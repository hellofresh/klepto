package mysql

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"

	"github.com/hellofresh/klepto/pkg/reader"
)

type driver struct{}

// IsSupported checks if the given dsn connection string is supported.
func (m *driver) IsSupported(dsn string) bool {
	if dsn == "" {
		return false
	}

	_, err := mysql.ParseDSN(dsn)
	return err == nil
}

// NewConnection creates a new mysql connection and retrieves a new mysql reader.
func (m *driver) NewConnection(opts reader.ConnOpts) (reader.Reader, error) {
	conn, err := sql.Open("mysql", opts.DSN)
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(opts.MaxConns)
	conn.SetMaxIdleConns(opts.MaxIdleConns)
	conn.SetConnMaxLifetime(opts.MaxConnLifetime)

	return NewStorage(conn, opts.Timeout), nil
}

func init() {
	reader.Register("mysql", &driver{})
}
