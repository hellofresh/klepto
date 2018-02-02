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

func (m *driver) NewConnection(dsn string, rdr reader.Reader) (dumper.Dumper, error) {
	dsnCfg, err := mysql.ParseDSN(dsn)
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

	// avoid error=io: read/write on closed pipe
	conn.SetMaxIdleConns(0)

	return NewDumper(conn, rdr), nil
}

func init() {
	dumper.Register("mysql", &driver{})
}
