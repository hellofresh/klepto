package postgres

import (
	"database/sql"
	"strings"

	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
)

type driver struct{}

func (m *driver) IsSupported(dsn string) bool {
	return strings.HasPrefix(strings.ToLower(dsn), "postgres://")
}

func (m *driver) NewConnection(opts dumper.ConnOpts, rdr reader.Reader) (dumper.Dumper, error) {
	conn, err := sql.Open("postgres", opts.DSN)
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(opts.MaxConns)
	conn.SetMaxIdleConns(opts.MaxIdleConns)
	conn.SetConnMaxLifetime(opts.MaxConnLifetime)

	return NewDumper(conn, rdr), nil
}

func init() {
	dumper.Register("postgres", &driver{})
}
