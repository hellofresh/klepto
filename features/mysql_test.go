package features

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/dumper"
	_ "github.com/hellofresh/klepto/pkg/dumper/mysql"
	"github.com/hellofresh/klepto/pkg/reader"
	_ "github.com/hellofresh/klepto/pkg/reader/mysql"
	"github.com/stretchr/testify/suite"
)

type MysqlTestSuite struct {
	suite.Suite
	rootDSN        string
	rootConnection *sql.DB
	databases      []string
	timeout        time.Duration
}

func TestMysqlTestSuite(t *testing.T) {
	s := &MysqlTestSuite{timeout: time.Second * 3}
	suite.Run(t, s)
}

func (s *MysqlTestSuite) TestExample() {
	readDSN := s.createDatabase("simple")
	dumpDSN := s.createDatabase("simple_dump")

	s.loadFixture(readDSN, "mysql_simple.sql")

	rdr, err := reader.Connect(reader.ConnOpts{DSN: readDSN, Timeout: s.timeout})
	s.Require().NoError(err, "Unable to create reader")
	defer func() {
		err := rdr.Close()
		s.Assert().NoError(err)
	}()

	dmp, err := dumper.NewDumper(dumper.ConnOpts{DSN: dumpDSN}, rdr)
	s.Require().NoError(err, "Unable to create dumper")
	defer func() {
		err := dmp.Close()
		s.Assert().NoError(err)
	}()

	done := make(chan struct{})
	defer close(done)
	s.Require().NoError(dmp.Dump(done, config.Tables{}, 4), "Failed to dump")

	<-done

	s.assertDatabaseAreTheSame(readDSN, dumpDSN)
}

func (s *MysqlTestSuite) SetupSuite() {
	rootDSN, ok := os.LookupEnv("TEST_MYSQL")
	if !ok {
		s.T().Skip("TEST_MYSQL env is not defined")
	}

	rootCfg, err := mysql.ParseDSN(rootDSN)
	s.Require().NoError(err, "TEST_MYSQL failed to parse")
	rootCfg.MultiStatements = true

	s.rootDSN = rootCfg.FormatDSN()
	s.rootConnection, err = sql.Open("mysql", rootDSN)
	s.Require().NoError(err, "Failed to connect to mysql")
	s.Require().NoError(s.rootConnection.Ping(), "Failed to ping mysql")
}

func (s *MysqlTestSuite) TearDownSuite() {
	for _, db := range s.databases {
		s.dropDatabase(db)
	}

	err := s.rootConnection.Close()
	s.Assert().NoError(err)
}

func (s *MysqlTestSuite) createDatabase(name string) string {
	s.databases = append(s.databases, name)

	s.dropDatabase(name)

	_, err := s.rootConnection.Exec(fmt.Sprintf("CREATE DATABASE %s", name))
	s.Require().NoError(err, "Unable to create db")

	dbURL, _ := mysql.ParseDSN(s.rootDSN)
	dbURL.DBName = name

	return dbURL.FormatDSN()
}

func (s *MysqlTestSuite) dropDatabase(name string) {
	_, err := s.rootConnection.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", name))
	s.NoError(err, "Unable to drop db")
}

func (s *MysqlTestSuite) loadFixture(dsn string, file string) {
	data, err := ioutil.ReadFile(path.Join("../fixtures/", file))
	s.Require().NoError(err, "Unable to load fixture file")

	conn, err := sql.Open("mysql", dsn)
	defer func() {
		err := conn.Close()
		s.Assert().NoError(err)
	}()
	s.Require().NoError(err, "Unable to open db connection to load fixture")

	_, err = conn.Exec(string(data))
	s.Require().NoError(err, "Unable to execute fixture")
}

func (s *MysqlTestSuite) assertDatabaseAreTheSame(expectedDSN string, dumpDSN string) {
	sourceConn, err := sql.Open("mysql", expectedDSN)
	s.Require().NoError(err, "Unable to connect to source db")
	defer func() {
		err := sourceConn.Close()
		s.Assert().NoError(err)
	}()

	targetConn, err := sql.Open("mysql", dumpDSN)
	s.Require().NoError(err, "Unable to connect to target db")
	defer func() {
		err := targetConn.Close()
		s.Assert().NoError(err)
	}()

	tables := s.fetchTableRowCount(sourceConn)
	s.Require().Equal(tables, s.fetchTableRowCount(targetConn))

	for _, table := range tables {
		s.compareTable(sourceConn, targetConn, table.name, table.columnCount)
	}
}

func (s *MysqlTestSuite) fetchTableRowCount(db *sql.DB) []tableInfo {
	tableRows, err := db.Query(
		`SELECT
		  t.TABLE_NAME AS name,
		  SUM(t.TABLE_ROWS) AS count,
		  COUNT(c.COLUMN_NAME) AS columnCount
		FROM information_schema.TABLES AS t
		  LEFT JOIN information_schema.COLUMNS AS c ON
			c.TABLE_SCHEMA = t.TABLE_SCHEMA AND
			c.TABLE_NAME = t.TABLE_NAME
		WHERE t.TABLE_SCHEMA = DATABASE()
		GROUP BY t.TABLE_NAME`,
	)
	s.Require().NoError(err, "Unable to fetch table info")
	defer tableRows.Close()

	var tables []tableInfo
	for tableRows.Next() {
		table := tableInfo{}

		s.Require().NoError(
			tableRows.Scan(&table.name, &table.count, &table.columnCount),
			"Unable to fetch table info row",
		)

		tables = append(tables, table)
	}

	return tables
}

func (s *MysqlTestSuite) compareTable(source *sql.DB, target *sql.DB, table string, columnCount int) {
	assert := s.Require()
	query := fmt.Sprintf("SELECT * FROM %s", table)

	expectedRows, err := source.Query(query)
	assert.NoError(err, "Unable to query source table")
	defer func() {
		err := expectedRows.Close()
		s.Assert().NoError(err)
	}()

	rows, err := target.Query(query)
	assert.NoError(err, "Unable to query target table")
	defer func() {
		err := rows.Close()
		s.Assert().NoError(err)
	}()

	for expectedRows.Next() {
		assert.True(rows.Next(), "target row mismatch")

		expectedFields := make([]interface{}, columnCount)
		targetFields := make([]interface{}, columnCount)
		for i := 0; i < columnCount; i++ {
			var sourceValue interface{}
			expectedFields[i] = &sourceValue

			var targetValue interface{}
			targetFields[i] = &targetValue
		}

		assert.NoError(expectedRows.Scan(expectedFields...), "failed to fetch expected rows")
		assert.NoError(rows.Scan(targetFields...), "failed to fetch target rows")

		assert.Equal(expectedFields, targetFields)
	}
}
