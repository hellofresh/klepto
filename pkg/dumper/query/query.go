package query

import (
	"os"

	parser "github.com/hellofresh/klepto/pkg/dsn"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
)

type driver struct{}

func (m *driver) IsSupported(dsn string) (bool, error) {
	_, err := parser.Parse(dsn)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (m *driver) NewConnection(dsn string, rdr reader.Reader) (dumper.Dumper, error) {
	return NewDumper(os.Stdout, rdr), nil
}

func init() {
	dumper.Register("query", &driver{})
}
