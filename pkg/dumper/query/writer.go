package query

import (
	"fmt"
	"io"
	"os"

	parser "github.com/hellofresh/klepto/pkg/dsn"
)

func getOsWriter(address string) io.Writer {
	if address == "stderr" {
		return os.Stderr
	}
	if address == "stdout" {
		return os.Stdout
	}
	return nil
}

// TODO: Implement writer interface for file.
func getOutputWriter(dsn string) (io.Writer, error) {
	config, err := parser.Parse(dsn)
	if err != nil {
		return nil, err
	}
	switch config.Type {
	case "os":
		return getOsWriter(config.Address), nil
	default:
		return nil, fmt.Errorf("Unknown output writer type: %v", config.Type)
	}
}
