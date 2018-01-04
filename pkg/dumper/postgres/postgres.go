package postgres

import (
	"database/sql"

	parser "github.com/hellofresh/klepto/pkg/dsn"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
)

type driver struct{}

func (m *driver) IsSupported(dsn string) (bool, error) {
	d, err := parser.Parse(dsn)
	if err != nil {
		return false, err
	}
	return d.Type == "postgres", nil
}

func (m *driver) NewConnection(dsn string, rdr reader.Reader) (dumper.Dumper, error) {
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return NewDumper(conn, rdr), nil
}

func init() {
	dumper.Register("postgres", &driver{})
}
