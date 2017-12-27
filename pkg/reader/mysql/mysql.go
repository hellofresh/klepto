package mysql

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
	"github.com/hellofresh/klepto/pkg/reader"
)

type driver struct{}

func (m *driver) IsSupported(dsn string) bool {
	_, err := mysql.ParseDSN(dsn)
	return err == nil
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
