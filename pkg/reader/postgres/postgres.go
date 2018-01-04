package postgres

import (
	"database/sql"

	parser "github.com/hellofresh/klepto/pkg/dsn"
	"github.com/hellofresh/klepto/pkg/reader"
	_ "github.com/lib/pq"
)

type driver struct{}

func (m *driver) IsSupported(dsn string) (bool, error) {
	d, err := parser.Parse(dsn)
	if err != nil {
		return false, err
	}
	return d.Type == "postgres", nil
}

func (m *driver) NewConnection(dsn string) (reader.Reader, error) {
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	dump, err := NewPgDump(dsn)
	if err != nil {
		return nil, err
	}

	return NewStorage(conn, dump), nil
}

func init() {
	reader.Register("postgres", &driver{})
}
