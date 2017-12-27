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

func (m *driver) NewConnection(dsn string) (reader.Reader, error) {
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return NewStorage(conn), nil
}

func init() {
	reader.Register("postgres", &driver{})
}
