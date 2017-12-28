package query

import (
	"os"

	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
)

type driver struct{}

func (m *driver) IsSupported(dsn string) bool {
	return true
}

func (m *driver) NewConnection(dsn string, rdr reader.Reader) (dumper.Dumper, error) {
	return NewDumper(os.Stdout, rdr), nil
}

func init() {
	dumper.Register("query", &driver{})
}
