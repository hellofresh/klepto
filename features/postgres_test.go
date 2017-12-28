package features

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"testing"

	"github.com/hellofresh/klepto/pkg/dumper"
	_ "github.com/hellofresh/klepto/pkg/dumper/postgres"
	"github.com/hellofresh/klepto/pkg/reader"
	_ "github.com/hellofresh/klepto/pkg/reader/postgres"
	"github.com/stretchr/testify/suite"
)

type PostgresTestSuite struct {
	suite.Suite

	rootDSN        string
	rootConnection *sql.DB

	databases []string
}

func TestPostgresTestSuite(t *testing.T) {
	suite.Run(t, new(PostgresTestSuite))
}

func (s *PostgresTestSuite) TestExample() {
	readDSN := s.createDatabase("pg_simple")
	dumpDSN := s.createDatabase("pg_simple_dump")

	s.loadFixture(readDSN, "pg_simple.sql")

	rdr, err := reader.Connect(readDSN)
	s.Require().NoError(err, "Unable to create reader")
	defer rdr.Close()

	dmp, err := dumper.NewDumper(dumpDSN, rdr)
	s.Require().NoError(err, "Unable to create dumper")
	defer dmp.Close()

	done := make(chan struct{})
	s.Require().NoError(dmp.Dump(done), "Failed to dump")

	<-done

	// TODO check table content
}

func (s *PostgresTestSuite) SetupSuite() {
	rootDSN, ok := os.LookupEnv("TEST_POSTGRES")
	if !ok {
		s.T().Skip("TEST_POSTGRES env is not defined")
	}

	rootUrl, err := url.Parse(rootDSN)
	s.Require().NoError(err, "TEST_POSTGRES failed to parse")
	s.Require().Empty(rootUrl.Path, "TEST_POSTGRES contains a dbname/path this is not supported")

	s.rootDSN = rootDSN
	s.rootConnection, err = sql.Open("postgres", rootDSN)
	s.Require().NoError(err, "Failed to connect to postgres")
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

	dbUrl, _ := url.Parse(s.rootDSN)
	dbUrl.Path = name
	return dbUrl.String()
}

func (s *PostgresTestSuite) dropDatabase(name string) {
	_, err := s.rootConnection.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", name))
	s.NoError(err, "Unable to drop db")
}

func (s *PostgresTestSuite) loadFixture(dsn string, file string) {
	data, err := ioutil.ReadFile(path.Join("fixture/", file))
	s.Require().NoError(err, "Unable to load fixture file")

	conn, err := sql.Open("postgres", dsn)
	defer conn.Close()
	s.Require().NoError(err, "Unable to open db connection to load fixture")

	_, err = conn.Exec(string(data))
	s.Require().NoError(err, "Unable to execute fixture")
}
