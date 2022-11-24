package replacer

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
)

func TestWhenColumnIsReplaced(t *testing.T) {
	replacer := NewReplacer(
		&mockReader{},
		config.Tables{
			{
				Name: "test",
				Replace: []*config.Replace{
					{Column: "column_test", Before: "something.somewhere.com", After: "something.somewhere.dev"},
				},
			},
		},
	)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := replacer.ReadTable("test", rowChan, reader.ReadTableOpt{})
	require.NoError(t, err)

	select {
	case row := <-rowChan:
		assert.Equal(t, "something.somewhere.dev/a676f8f6-b1bb-4b47-9517-f7effa9badec", row["column_test"])
	case <-time.After(time.Second):
		assert.FailNow(t, "Failing due to timeout")
	}
}

type mockReader struct{}

func (m *mockReader) GetTables() ([]string, error)        { return []string{"table_test"}, nil }
func (m *mockReader) GetStructure() (string, error)       { return "", nil }
func (m *mockReader) GetColumns(string) ([]string, error) { return []string{"column_test"}, nil }
func (m *mockReader) GetPreamble() (string, error)        { return "", nil }
func (m *mockReader) Close() error                        { return nil }
func (m *mockReader) FormatColumn(tbl string, col string) string {
	return fmt.Sprintf("%s.%s", strconv.Quote(tbl), strconv.Quote(col))
}
func (m *mockReader) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	row := make(database.Row)
	row["column_test"] = "something.somewhere.com/a676f8f6-b1bb-4b47-9517-f7effa9badec"
	rowChan <- row
	return nil
}
