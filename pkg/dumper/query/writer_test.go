package query

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var tests = []struct {
	Dsn    string
	Writer io.Writer
}{
	{Dsn: "os://stdout/", Writer: os.Stdout},
	{Dsn: "os://stderr/", Writer: os.Stderr},
}

func TestWriter(t *testing.T) {
	for _, test := range tests {
		w, err := getOutputWriter(test.Dsn)
		if err != nil {
			t.Error("Encountered error when getting output writer",
				err)
		}
		if test.Dsn != "" {
			assert.Equal(t, test.Writer, w)
		}
	}
}
