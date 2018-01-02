package anonymiser

import (
	"testing"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/reader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockReader struct{}

func (m *mockReader) GetTables() ([]string, error)        { return []string{"table_test"}, nil }
func (m *mockReader) GetStructure() (string, error)       { return "", nil }
func (m *mockReader) GetColumns(string) ([]string, error) { return []string{"column_test"}, nil }
func (m *mockReader) GetPreamble() (string, error)        { return "", nil }
func (m *mockReader) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	row := make(database.Row)
	row["column_test"] = "to_be_anonimised"

	rowChan <- row

	return nil
}

func TestReadTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		config   config.Tables
		function func(*testing.T, config.Tables)
	}{
		{
			scenario: "when anonymiser is not initialized",
			function: testWhenAnonymiserIsNotInitialized,
			config:   config.Tables{{Name: "test"}},
		},
		{
			scenario: "when table is not set in the config",
			function: testWhenTableIsNotSetInConfig,
			config:   config.Tables{{Name: "test"}},
		},
		{
			scenario: "when column is anonymised",
			function: testWhenColumnIsAnonymised,
			config:   config.Tables{{Name: "test", Anonymise: map[string]string{"column_test": "FirstName"}}},
		},
		{
			scenario: "when column is anonymised with literal",
			function: testWhenColumnIsAnonymisedWithLiteral,
			config:   config.Tables{{Name: "test", Anonymise: map[string]string{"column_test": "literal:Hello"}}},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t, test.config)
		})
	}
}

func testWhenAnonymiserIsNotInitialized(t *testing.T, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row, 1)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, reader.ReadTableOpt{})
	require.NoError(t, err)
}

func testWhenTableIsNotSetInConfig(t *testing.T, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row, 1)
	defer close(rowChan)

	err := anonymiser.ReadTable("other_table", rowChan, reader.ReadTableOpt{})
	require.NoError(t, err)
}

func testWhenColumnIsAnonymised(t *testing.T, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, reader.ReadTableOpt{})
	require.NoError(t, err)

	for {
		row := <-rowChan
		assert.NotEqual(t, "to_be_anonimised", row["column_test"])
		break
	}
}

func testWhenColumnIsAnonymisedWithLiteral(t *testing.T, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, reader.ReadTableOpt{})
	require.NoError(t, err)

	for {
		row := <-rowChan

		assert.Equal(t, "Hello", row["column_test"])
		break
	}
}
