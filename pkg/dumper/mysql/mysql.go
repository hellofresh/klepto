package mysql

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
	"github.com/usoban/klepto/pkg/dumper"
	"github.com/usoban/klepto/pkg/reader"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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

// NewConnection creates a new mysql connection and retrieves a new mysql dumper.
func (m *driver) NewConnection(opts dumper.ConnOpts, rdr reader.Reader) (dumper.Dumper, error) {
	dsnCfg, err := mysql.ParseDSN(opts.DSN)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse mysql dsn")
	}

	if !dsnCfg.MultiStatements {
		log.WithField("help", "https://github.com/go-sql-driver/mysql#multistatements").
			Warning("MYSQL dumper forcing multistatements!")
		dsnCfg.MultiStatements = true
	}

	conn, err := sql.Open("mysql", dsnCfg.FormatDSN())
	if err != nil {
		return nil, errors.Wrap(err, "failed to open mysql connection")
	}

	conn.SetMaxOpenConns(opts.MaxConns)
	conn.SetMaxIdleConns(opts.MaxIdleConns)
	conn.SetConnMaxLifetime(opts.MaxConnLifetime)

	return NewDumper(conn, rdr), nil
}

func init() {
	dumper.Register("mysql", &driver{})
}
