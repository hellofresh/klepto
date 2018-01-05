package query

import (
	parser "github.com/hellofresh/klepto/pkg/dsn"
	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
)

type driver struct{}

func (m *driver) IsSupported(dsn string) bool {
	d, err := parser.Parse(dsn)
	if err != nil {
		return false
	}
	return d.Type == "file" || d.Type == "os"
}

func (m *driver) NewConnection(dsn string, rdr reader.Reader) (dumper.Dumper, error) {
	writer, err := getOutputWriter(dsn)
	if err != nil {
		return nil, err
	}
	return NewDumper(writer, rdr), nil
}

func init() {
	dumper.Register("query", &driver{})
}
