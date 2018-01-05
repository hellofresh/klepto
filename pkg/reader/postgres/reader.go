package postgres

import (
	"database/sql"
	"strconv"

	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/hellofresh/klepto/pkg/reader/generic"
	log "github.com/sirupsen/logrus"
)

// Storage ...
type storage struct {
	PgDump

	connection *sql.DB
}

// NewStorage ...
func NewStorage(conn *sql.DB, dumper PgDump) reader.Reader {
	return generic.NewSqlReader(
		conn,
		&storage{
			PgDump:     dumper,
			connection: conn,
		},
	)
}

// GetTables gets a list of all tables in the database
func (s *storage) GetTables() ([]string, error) {
	log.Info("Fetching table list")
	rows, err := s.connection.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}

		tables = append(tables, tableName)
	}

	log.WithField("tables", tables).Debug("Fetched table list")

	return tables, nil
}

// GetColumns returns the columns in the specified database table
func (s *storage) GetColumns(table string) ([]string, error) {
	log.WithField("table", table).Info("Fetching table columns")
	rows, err := s.connection.Query(
		"SELECT column_name FROM information_schema.columns WHERE table_catalog=current_database() AND table_name=$1",
		table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}

		columns = append(columns, column)
	}

	return columns, nil
}

func (s *storage) QuoteIdentifier(name string) string {
	return strconv.Quote(name)
}
