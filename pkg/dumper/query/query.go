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
	return d.Type == "os"
}

func (m *driver) NewConnection(opts dumper.ConnectionOpts, rdr reader.Reader) (dumper.Dumper, error) {
	writer, err := getOutputWriter(opts.DSN)
	if err != nil {
		return nil, err
	}
	return NewDumper(writer, rdr), nil
}

func init() {
	dumper.Register("query", &driver{})
}
