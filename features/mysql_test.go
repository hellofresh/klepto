package features

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

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

	databases []string
}

func TestMysqlTestSuite(t *testing.T) {
	suite.Run(t, new(MysqlTestSuite))
}

func (s *MysqlTestSuite) TestExample() {
	readDSN := s.createDatabase("simple")
	dumpDSN := s.createDatabase("simple_dump")

	s.loadFixture(readDSN, "mysql_simple.sql")

	rdr, err := reader.Connect(readDSN)
	s.Require().NoError(err, "Unable to create reader")
	defer rdr.Close()

	dmp, err := dumper.NewDumper(dumpDSN, rdr)
	s.Require().NoError(err, "Unable to create dumper")
	defer dmp.Close()

	done := make(chan struct{})
	s.Require().NoError(dmp.Dump(done, config.Tables{}), "Failed to dump")

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

	s.rootConnection.Close()
}

func (s *MysqlTestSuite) createDatabase(name string) string {
	s.databases = append(s.databases, name)

	s.dropDatabase(name)

	_, err := s.rootConnection.Exec(fmt.Sprintf("CREATE DATABASE %s", name))
	s.Require().NoError(err, "Unable to create db")

	dbUrl, _ := mysql.ParseDSN(s.rootDSN)
	dbUrl.DBName = name

	_, err = s.rootConnection.Exec(fmt.Sprintf("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%'", name, dbUrl.User))
	s.Require().NoError(err, "Unable to grant db permissions")

	return dbUrl.FormatDSN()
}

func (s *MysqlTestSuite) dropDatabase(name string) {
	_, err := s.rootConnection.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", name))
	s.NoError(err, "Unable to drop db")
}

func (s *MysqlTestSuite) loadFixture(dsn string, file string) {
	data, err := ioutil.ReadFile(path.Join("../fixtures/", file))
	s.Require().NoError(err, "Unable to load fixture file")

	conn, err := sql.Open("mysql", dsn)
	defer conn.Close()
	s.Require().NoError(err, "Unable to open db connection to load fixture")

	_, err = conn.Exec(string(data))
	s.Require().NoError(err, "Unable to execute fixture")
}

func (s *MysqlTestSuite) assertDatabaseAreTheSame(expectedDSN string, dumpDSN string) {
	sourceConn, err := sql.Open("mysql", expectedDSN)
	s.Require().NoError(err, "Unable to connect to source db")
	defer sourceConn.Close()

	targetConn, err := sql.Open("mysql", dumpDSN)
	s.Require().NoError(err, "Unable to connect to target db")
	defer targetConn.Close()

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
		  t.TABLE_ROWS AS count,
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

	tables := []tableInfo{}
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
	defer expectedRows.Close()

	rows, err := target.Query(query)
	assert.NoError(err, "Unable to query target table")
	defer rows.Close()

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
