package mysql

import (
	"database/sql"

	parser "github.com/hellofresh/klepto/pkg/dsn"
	"github.com/hellofresh/klepto/pkg/reader"
)

type driver struct{}

func (m *driver) IsSupported(dsn string) (bool, error) {
	d, err := parser.Parse(dsn)
	if err != nil {
		return false, err
	}
	return d.Type == "mysql", nil

}

func (m *driver) NewConnection(dsn string) (reader.Reader, error) {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	return NewStorage(conn), nil
}

func init() {
	reader.Register("mysql", &driver{})
}
