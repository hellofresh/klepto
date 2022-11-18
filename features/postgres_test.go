package features

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/hellofresh/klepto/pkg/anonymiser"
	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/dumper"
	_ "github.com/hellofresh/klepto/pkg/dumper/postgres"
	"github.com/hellofresh/klepto/pkg/reader"
	_ "github.com/hellofresh/klepto/pkg/reader/postgres"
)

type PostgresTestSuite struct {
	suite.Suite
	rootDSN        string
	rootConnection *sql.DB
	databases      []string
	timeout        time.Duration
}

type tableInfo struct {
	name        string
	count       uint64
	columnCount int
}

func TestPostgresTestSuite(t *testing.T) {
	s := &PostgresTestSuite{timeout: time.Second * 3}
	suite.Run(t, s)
}

func (s *PostgresTestSuite) TestExample() {
	readDSN := s.createDatabase("pg_simple")
	dumpDSN := s.createDatabase("pg_simple_dump")

	s.loadFixture(readDSN, "pg_simple.sql")

	rdr, err := reader.Connect(reader.ConnOpts{DSN: readDSN, Timeout: s.timeout})
	s.Require().NoError(err, "Unable to create reader")
	defer rdr.Close()

	dmp, err := dumper.NewDumper(dumper.ConnOpts{DSN: dumpDSN}, rdr)
	s.Require().NoError(err, "Unable to create dumper")
	defer dmp.Close()

	done := make(chan struct{})
	defer close(done)
	s.Require().NoError(dmp.Dump(done, config.Tables{}, 4, false), "Failed to dump")

	<-done

	s.assertDatabaseAreTheSame(readDSN, dumpDSN)
}

func (s *PostgresTestSuite) TestMultipleSubsetsExample() {
	readDSN := s.createDatabase("pg_subsets")
	dumpDSN := s.createDatabase("pg_subsets_dump")

	tables, err := config.LoadFromFile(path.Join("../fixtures/", ".klepto_subsets.toml"))
	s.Require().NoError(err, "Unable to load configuration")
	s.loadFixture(readDSN, "pg_subsets.sql")

	rdr, err := reader.Connect(reader.ConnOpts{DSN: readDSN, Timeout: s.timeout})
	s.Require().NoError(err, "Unable to create reader")
	defer rdr.Close()

	rdr = anonymiser.NewAnonymiser(rdr)

	dmp, err := dumper.NewDumper(dumper.ConnOpts{DSN: dumpDSN}, rdr)
	s.Require().NoError(err, "Unable to create dumper")
	defer dmp.Close()

	done := make(chan struct{})
	defer close(done)
	s.Require().NoError(dmp.Dump(done, tables, 4, false), "Failed to dump")

	<-done

	targetConn, err := sql.Open("postgres", dumpDSN)
	s.Require().NoError(err, "Unable to connect to target db")
	defer targetConn.Close()

	targetTables := s.fetchTableRowCount(targetConn)
	s.Assert().Equal([]tableInfo{{name: "users", count: 5, columnCount: 4}}, targetTables)
}

func (s *PostgresTestSuite) SetupSuite() {
	rootDSN, ok := os.LookupEnv("TEST_POSTGRES")
	if !ok {
		s.T().Skip("TEST_POSTGRES env is not defined")
	}

	_, err := url.Parse(rootDSN)
	s.Require().NoError(err, "TEST_POSTGRES failed to parse")

	s.rootDSN = rootDSN
	s.rootConnection, err = sql.Open("postgres", rootDSN)
	s.Require().NoError(err, "Failed to connect to postgres")
	s.Require().NoError(s.rootConnection.Ping(), "Failed to ping postgres")
}

func (s *PostgresTestSuite) TearDownSuite() {
	for _, db := range s.databases {
		s.dropDatabase(db)
	}

	s.rootConnection.Close()
}

func (s *PostgresTestSuite) createDatabase(name string) string {
	s.databases = append(s.databases, name)

	s.dropDatabase(name)

	_, err := s.rootConnection.Exec(fmt.Sprintf("CREATE DATABASE %s", name))
	s.Require().NoError(err, "Unable to create db")

	dbURL, _ := url.Parse(s.rootDSN)
	dbURL.Path = name
	return dbURL.String()
}

func (s *PostgresTestSuite) dropDatabase(name string) {
	_, err := s.rootConnection.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", name))
	s.NoError(err, "Unable to drop db")
}

func (s *PostgresTestSuite) loadFixture(dsn string, file string) {
	data, err := os.ReadFile(path.Join("../fixtures/", file))
	s.Require().NoError(err, "Unable to load fixture file")

	conn, err := sql.Open("postgres", dsn)
	s.Require().NoError(err, "Unable to open db connection to load fixture")
	defer conn.Close()

	_, err = conn.Exec(string(data))
	s.Require().NoError(err, "Unable to execute fixture")
}

func (s *PostgresTestSuite) assertDatabaseAreTheSame(expectedDSN string, dumpDSN string) {
	sourceConn, err := sql.Open("postgres", expectedDSN)
	s.Require().NoError(err, "Unable to connect to source db")
	defer sourceConn.Close()

	targetConn, err := sql.Open("postgres", dumpDSN)
	s.Require().NoError(err, "Unable to connect to target db")
	defer targetConn.Close()

	tables := s.fetchTableRowCount(sourceConn)
	s.Require().Equal(tables, s.fetchTableRowCount(targetConn))

	for _, table := range tables {
		s.compareTable(sourceConn, targetConn, table.name, table.columnCount)
	}
}

func (s *PostgresTestSuite) fetchTableRowCount(db *sql.DB) []tableInfo {
	_, err := db.Exec("ANALYSE")
	s.Require().NoError(err, "Unable to analyse to source db")

	tableRows, err := db.Query(
		`SELECT
		  pg_class.relname   AS name,
		  pg_class.reltuples AS count,
		  pg_class.relnatts  AS columnCount
		FROM
		  pg_class
		  LEFT JOIN pg_namespace ON (pg_namespace.oid = pg_class.relnamespace)
		WHERE
		  pg_namespace.nspname NOT IN ('pg_catalog', 'information_schema') AND
		  pg_class.relkind='r'
		ORDER BY pg_class.relname, pg_class.reltuples`,
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

func (s *PostgresTestSuite) compareTable(source *sql.DB, target *sql.DB, table string, columnCount int) {
	assert := s.Require()
	query := fmt.Sprintf("SELECT * FROM %s", strconv.Quote(table))

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
