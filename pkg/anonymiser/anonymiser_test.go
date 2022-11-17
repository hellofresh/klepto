package anonymiser

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hellofresh/klepto/pkg/config"
	"github.com/hellofresh/klepto/pkg/database"
)

const waitTimeout = time.Second

func TestReadTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T, config.Table)
		table    config.Table
	}{
		{
			scenario: "when anonymiser is not initialized",
			function: testWhenAnonymiserIsNotInitialized,
			table:    config.Table{Name: "test"},
		},
		{
			scenario: "when table is not set in the config",
			function: testWhenTableIsNotSetInConfig,
			table:    config.Table{Name: "test"},
		},
		{
			scenario: "when column is anonymised",
			function: testWhenColumnIsAnonymised,
			table:    config.Table{Name: "test", Anonymise: map[string]string{"column_test": "FirstName"}},
		},
		{
			scenario: "when column is anonymised with literal",
			function: testWhenColumnIsAnonymisedWithLiteral,
			table:    config.Table{Name: "test", Anonymise: map[string]string{"column_test": "literal:Hello"}},
		},
		{
			scenario: "when column is anonymised with float value",
			function: testWhenColumnIsAnonymisedWithFloatValue,
			table:    config.Table{Name: "test", Anonymise: map[string]string{"column_test": "Latitude"}},
		},
		{
			scenario: "when column anonymiser in invalid",
			function: testWhenColumnAnonymiserIsInvalid,
			table:    config.Table{Name: "test", Anonymise: map[string]string{"column_test": "Hello"}},
		},
		{
			scenario: "when column anonymiser require args",
			function: testWhenColumnAnonymiserRequireArgs,
			table:    config.Table{Name: "test", Anonymise: map[string]string{"column_test": "DigitsN:20"}},
		},
		{
			scenario: "when column anonymiser require multiple args",
			function: testWhenColumnAnonymiserRequireMultipleArgs,
			table:    config.Table{Name: "test", Anonymise: map[string]string{"column_test": "Year:2020:2021"}},
		},
		{
			scenario: "when column anonymiser require args but no values are passed",
			function: testWhenColumnAnonymiserRequireArgsNoValues,
			table:    config.Table{Name: "test", Anonymise: map[string]string{"column_test": "CreditCardNum"}},
		},
		{
			scenario: "when column anonymiser require args but the value passed is invalid",
			function: testWhenColumnAnonymiserRequireArgsInvalidValues,
			table:    config.Table{Name: "test", Anonymise: map[string]string{"column_test1": "CharactersN:invalid", "column_test2": "Password:1:2:yes"}},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t, test.table)
		})
	}
}

func testWhenAnonymiserIsNotInitialized(t *testing.T, table config.Table) {
	anonymiser := NewAnonymiser(&mockReader{})

	rowChan := make(chan database.Row, 1)
	defer close(rowChan)

	err := anonymiser.ReadTable(config.Table{Name: "test"}, rowChan)
	require.NoError(t, err)
}

func testWhenTableIsNotSetInConfig(t *testing.T, table config.Table) {
	anonymiser := NewAnonymiser(&mockReader{})

	rowChan := make(chan database.Row, 1)
	defer close(rowChan)

	err := anonymiser.ReadTable(config.Table{Name: "other_table"}, rowChan)
	require.NoError(t, err)
}

func testWhenColumnIsAnonymised(t *testing.T, table config.Table) {
	anonymiser := NewAnonymiser(&mockReader{})

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable(table, rowChan)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.NotEqual(t, "to_be_anonimised", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnIsAnonymisedWithLiteral(t *testing.T, table config.Table) {
	anonymiser := NewAnonymiser(&mockReader{})

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable(table, rowChan)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.Equal(t, "Hello", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnIsAnonymisedWithFloatValue(t *testing.T, table config.Table) {
	anonymiser := NewAnonymiser(&mockReader{})

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable(table, rowChan)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.NotEqual(t, "<float32 Value>", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnAnonymiserIsInvalid(t *testing.T, table config.Table) {
	anonymiser := NewAnonymiser(&mockReader{})

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable(table, rowChan)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.Equal(t, "Invalid anonymiser: Hello", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnAnonymiserRequireArgs(t *testing.T, table config.Table) {
	anonymiser := NewAnonymiser(&mockReader{})

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable(table, rowChan)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.NotEqual(t, "to_be_anonimised", row["column_test"])
		assert.Len(t, row["column_test"], 20)
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnAnonymiserRequireMultipleArgs(t *testing.T, table config.Table) {
	anonymiser := NewAnonymiser(&mockReader{})

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable(table, rowChan)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.NotEqual(t, "to_be_anonimised", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnAnonymiserRequireArgsNoValues(t *testing.T, table config.Table) {
	anonymiser := NewAnonymiser(&mockReader{})

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable(table, rowChan)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.NotEqual(t, "to_be_anonimised", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnAnonymiserRequireArgsInvalidValues(t *testing.T, table config.Table) {
	anonymiser := NewAnonymiser(&mockReader{})

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable(table, rowChan)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.NotEqual(t, "to_be_anonimised", row["column_test1"])
		assert.NotEqual(t, "to_be_anonimised", row["column_test2"])
	case <-timeoutChan:
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
func (m *mockReader) ReadTable(table config.Table, rowChan chan<- database.Row) error {
	row := make(database.Row)
	row["column_test"] = "to_be_anonimised"
	rowChan <- row
	return nil
}
