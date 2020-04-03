package anonymiser

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T, reader.ReadTableOpt, config.Tables)
		opts     reader.ReadTableOpt
		config   config.Tables
	}{
		{
			scenario: "when anonymiser is not initialized",
			function: testWhenAnonymiserIsNotInitialized,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test"}},
		},
		{
			scenario: "when table is not set in the config",
			function: testWhenTableIsNotSetInConfig,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test"}},
		},
		{
			scenario: "when column is anonymised",
			function: testWhenColumnIsAnonymised,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test", Anonymise: map[string]string{"column_test": "FirstName"}}},
		},
		{
			scenario: "when column is anonymised with literal",
			function: testWhenColumnIsAnonymisedWithLiteral,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test", Anonymise: map[string]string{"column_test": "literal:Hello"}}},
		},
		{
			scenario: "when column anonymiser in invalid",
			function: testWhenColumnAnonymiserIsInvalid,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test", Anonymise: map[string]string{"column_test": "Hello"}}},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t, test.opts, test.config)
		})
	}
}

func testWhenAnonymiserIsNotInitialized(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row, 1)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, opts)
	require.NoError(t, err)
}

func testWhenTableIsNotSetInConfig(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row, 1)
	defer close(rowChan)

	err := anonymiser.ReadTable("other_table", rowChan, opts)
	require.NoError(t, err)
}

func testWhenColumnIsAnonymised(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, opts)
	require.NoError(t, err)

	for {
		row := <-rowChan
		assert.NotEqual(t, "to_be_anonimised", row["column_test"])
		break
	}
}

func testWhenColumnIsAnonymisedWithLiteral(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, opts)
	require.NoError(t, err)

	for {
		row := <-rowChan
		assert.Equal(t, "Hello", row["column_test"])
		break
	}
}

func testWhenColumnAnonymiserIsInvalid(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, opts)
	require.NoError(t, err)

	for {
		row := <-rowChan
		assert.Equal(t, "Invalid anonymiser: Hello", row["column_test"])
		break
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
	row["column_test"] = "to_be_anonimised"
	rowChan <- row
	return nil
}
