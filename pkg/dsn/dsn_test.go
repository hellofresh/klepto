package dsn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var tests = []struct {
	Dsn        string
	Type       string
	Username   string
	Password   string
	Protocol   string
	Address    string
	Host       string
	Port       string
	DataSource string
	Params     map[string]string
}{
	{Dsn: "os://unix(stdout)/?format=csv", Type: "os", Username: "", Password: "", Protocol: "unix", Address: "stdout", Host: "", Port: "", DataSource: "", Params: map[string]string{"format": "csv"}},
	{Dsn: "os://stdout/?format=csv", Type: "os", Username: "", Password: "", Protocol: "", Address: "stdout",
		Host: "", Port: "", DataSource: "", Params: map[string]string{"format": "csv"}},
	{Dsn: "file://path(/some/path/to)/file.csv", Type: "file", Username: "", Password: "", Protocol: "path", Address: "/some/path/to", Host: "", Port: "", DataSource: "file.csv", Params: map[string]string{}},
	{Dsn: "file:///some/path/to/file.csv", Type: "file", Username: "", Password: "", Protocol: "", Address: "/some/path/to", Host: "", Port: "", DataSource: "file.csv", Params: map[string]string{}},
	{Dsn: "file://path(/some/path/to)/file.csv?format=csv", Type: "file", Username: "", Password: "", Protocol: "path", Address: "/some/path/to", Host: "", Port: "", DataSource: "file.csv", Params: map[string]string{"format": "csv"}},
	{Dsn: "file:///some/path/to/file.csv?format=csv", Type: "file", Username: "", Password: "", Protocol: "", Address: "/some/path/to", Host: "", Port: "", DataSource: "file.csv", Params: map[string]string{"format": "csv"}},
	{Dsn: "postgres://bruce:mypass@tcp(localhost:5432)/testdb", Type: "postgres", Username: "bruce", Password: "mypass", Protocol: "tcp", Address: "localhost:5432", Host: "localhost", Port: "5432", DataSource: "testdb", Params: map[string]string{}},
	{Dsn: "postgres://bruce:mypass@localhost:5432/testdb", Type: "postgres", Username: "bruce", Password: "mypass", Protocol: "", Address: "localhost:5432", Host: "localhost", Port: "5432", DataSource: "testdb", Params: map[string]string{}},
	{Dsn: "mysql://bruce:mypass@tcp(localhost:5432)/testdb", Type: "mysql", Username: "bruce", Password: "mypass", Protocol: "tcp", Address: "localhost:5432", Host: "localhost", Port: "5432", DataSource: "testdb", Params: map[string]string{}},
	{Dsn: "mysql://bruce:mypass@localhost:5432/testdb", Type: "mysql", Username: "bruce", Password: "mypass", Protocol: "", Address: "localhost:5432", Host: "localhost", Port: "5432", DataSource: "testdb", Params: map[string]string{}},
}

var hErrors = map[error]bool{
	ErrEmptyDsn:   true,
	ErrInvalidDsn: true,
}

func TestParse(t *testing.T) {
	for _, test := range tests {
		uri, err := Parse(test.Dsn)
		// Check that we parse without any unhandled errors
		if !hErrors[err] && err != nil {
			t.Error(
				"Encountered error when parsing:",
				err,
			)
		}
		// Check that all outputs are correct for non-empty dsns
		if test.Dsn != "" {
			assert.Equal(t, test.Type, uri.Type, "they should be equal")
			assert.Equal(t, test.Username, uri.Username, "they should be equal")
			assert.Equal(t, test.Password, uri.Password, "they should be equal")
			assert.Equal(t, test.Protocol, uri.Protocol, "they should be equal")
			assert.Equal(t, test.Address, uri.Address, "they should be equal")
			assert.Equal(t, test.Host, uri.Host, "they should be equal")
			assert.Equal(t, test.Port, uri.Port, "they should be equal")
			assert.Equal(t, test.DataSource, uri.DataSource, "they should be equal")
			assert.Equal(t, test.Params, uri.Params, "they should be equal")
		}
	}
}

var errorTests = []struct {
	dsn string
	err error
}{
	{"", ErrEmptyDsn},
	{"i_am_not_a_dsn", ErrInvalidDsn},
	{"/", ErrInvalidDsn},
}

func TestParseErrors(t *testing.T) {
	for _, test := range errorTests {
		_, err := Parse(test.dsn)

		// Check that we throw an error for empty dsns
		if test.dsn == "" {
			assert.Equal(t, ErrEmptyDsn, err, "we expect ErrEmptyDsn to be returned")
		} else { // Check that we throw error for invalid dsns
			t.Logf("what is the test: %#v", test.dsn)
			assert.Equal(t, ErrInvalidDsn, err, "we expect ErrInvalidDsn to be returned")
		}
	}
}
