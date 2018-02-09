package mysql

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
	log "github.com/sirupsen/logrus"
)

type driver struct{}

func (m *driver) IsSupported(dsn string) bool {
	if dsn == "" {
		return false
	}

	_, err := mysql.ParseDSN(dsn)
	return err == nil
}

func (m *driver) NewConnection(opts dumper.ConnectionOpts, rdr reader.Reader) (dumper.Dumper, error) {
	dsnCfg, err := mysql.ParseDSN(opts.DSN)
	if err != nil {
		return nil, err
	}
	if !dsnCfg.MultiStatements {
		log.WithField("help", "https://github.com/go-sql-driver/mysql#multistatements").
			Warning("MYSQL dumper forcing multistatements!")
		dsnCfg.MultiStatements = true
	}

	conn, err := sql.Open("mysql", dsnCfg.FormatDSN())
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(opts.MaxConnections)
	conn.SetMaxIdleConns(opts.MaxIdleConnections)
	conn.SetConnMaxLifetime(opts.MaxConnLifetime)

	return NewDumper(conn, rdr), nil
}

func init() {
	dumper.Register("mysql", &driver{})
}
