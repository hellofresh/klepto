package postgres

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/hellofresh/klepto/pkg/reader"
	_ "github.com/lib/pq"
)

type driver struct{}

func (m *driver) IsSupported(dsn string) bool {
	return strings.HasPrefix(strings.ToLower(dsn), "postgres://")
}

func (m *driver) NewConnection(dsn string) (reader.Reader, error) {
	_, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return nil, errors.New("TODO")
}

func init() {
	reader.Register("postgres", &driver{})
}
