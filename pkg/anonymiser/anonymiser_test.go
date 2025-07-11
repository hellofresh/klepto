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
	"github.com/hellofresh/klepto/pkg/reader"
)

const waitTimeout = time.Second

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
			scenario: "when column is anonymised with float value",
			function: testWhenColumnIsAnonymisedWithFloatValue,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test", Anonymise: map[string]string{"column_test": "Latitude"}}},
		},
		{
			scenario: "when column anonymiser in invalid",
			function: testWhenColumnAnonymiserIsInvalid,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test", Anonymise: map[string]string{"column_test": "Hello"}}},
		},
		{
			scenario: "when column anonymiser require args",
			function: testWhenColumnAnonymiserRequireArgs,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test", Anonymise: map[string]string{"column_test": "DigitsN:20"}}},
		},
		{
			scenario: "when column anonymiser require multiple args",
			function: testWhenColumnAnonymiserRequireMultipleArgs,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test", Anonymise: map[string]string{"column_test": "Year:2020:2021"}}},
		},
		{
			scenario: "when column anonymiser require args but no values are passed",
			function: testWhenColumnAnonymiserRequireArgsNoValues,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test", Anonymise: map[string]string{"column_test": "CreditCardNum"}}},
		},
		{
			scenario: "when column anonymiser require args but the value passed is invalid",
			function: testWhenColumnAnonymiserRequireArgsInvalidValues,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test", Anonymise: map[string]string{"column_test1": "CharactersN:invalid", "column_test2": "Password:1:2:yes"}}},
		},
		{
			scenario: "when column is omitted from data",
			function: testWhenColumnIsOmittedFromData,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test", Omit: []string{"column_to_omit"}}},
		},
		{
			scenario: "when column is both omitted from data and anonymised",
			function: testWhenColumnIsBothOmittedFromDataAndAnonymised,
			opts:     reader.ReadTableOpt{},
			config:   config.Tables{{Name: "test", Omit: []string{"column_to_omit"}, Anonymise: map[string]string{"column_test": "FirstName"}}},
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

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.NotEqual(t, "to_be_anonimised", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnIsAnonymisedWithLiteral(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, opts)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.Equal(t, "Hello", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnIsAnonymisedWithFloatValue(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, opts)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.NotEqual(t, "<float32 Value>", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnAnonymiserIsInvalid(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, opts)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.Equal(t, "Invalid anonymiser: Hello", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnAnonymiserRequireArgs(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, opts)
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

func testWhenColumnAnonymiserRequireMultipleArgs(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, opts)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.NotEqual(t, "to_be_anonimised", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnAnonymiserRequireArgsNoValues(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, opts)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.NotEqual(t, "to_be_anonimised", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnAnonymiserRequireArgsInvalidValues(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	rowChan := make(chan database.Row)
	defer close(rowChan)

	err := anonymiser.ReadTable("test", rowChan, opts)
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

func testWhenColumnIsOmittedFromData(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	// Test GetColumns filters out omitted columns
	columns, err := anonymiser.GetColumns("test")
	require.NoError(t, err)
	assert.Equal(t, []string{"column_test"}, columns)
	assert.NotContains(t, columns, "column_to_omit")

	// Test ReadTable removes omitted columns from row data
	rowChan := make(chan database.Row)
	defer close(rowChan)

	err = anonymiser.ReadTable("test", rowChan, opts)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.Contains(t, row, "column_test")
		assert.NotContains(t, row, "column_to_omit")
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func testWhenColumnIsBothOmittedFromDataAndAnonymised(t *testing.T, opts reader.ReadTableOpt, tables config.Tables) {
	anonymiser := NewAnonymiser(&mockReader{}, tables)

	// Test GetColumns filters out omitted columns (for data transfer)
	columns, err := anonymiser.GetColumns("test")
	require.NoError(t, err)
	assert.Equal(t, []string{"column_test"}, columns)
	assert.NotContains(t, columns, "column_to_omit")

	// Test ReadTable removes omitted columns and anonymises remaining columns
	rowChan := make(chan database.Row)
	defer close(rowChan)

	err = anonymiser.ReadTable("test", rowChan, opts)
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.Contains(t, row, "column_test")
		assert.NotContains(t, row, "column_to_omit")
		assert.NotEqual(t, "to_be_anonimised", row["column_test"])
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

func TestGetColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tables   config.Tables
		expected []string
	}{
		{
			name:     "when no omit config",
			tables:   config.Tables{{Name: "test"}},
			expected: []string{"column_test", "column_to_omit"},
		},
		{
			name:     "when table not found",
			tables:   config.Tables{{Name: "other"}},
			expected: []string{"column_test", "column_to_omit"},
		},
		{
			name:     "when one column is omitted",
			tables:   config.Tables{{Name: "test", Omit: []string{"column_to_omit"}}},
			expected: []string{"column_test"},
		},
		{
			name:     "when multiple columns are omitted",
			tables:   config.Tables{{Name: "test", Omit: []string{"column_to_omit", "column_test"}}},
			expected: []string{},
		},
		{
			name:     "when non-existent column is omitted",
			tables:   config.Tables{{Name: "test", Omit: []string{"non_existent"}}},
			expected: []string{"column_test", "column_to_omit"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			anonymiser := NewAnonymiser(&mockReader{}, test.tables)
			columns, err := anonymiser.GetColumns("test")
			require.NoError(t, err)
			assert.Equal(t, test.expected, columns)
		})
	}
}

func TestStructureVsDataBehavior(t *testing.T) {
	// This test verifies that the Omit configuration only affects data transfer and not
	// the database structure.
	//
	// Structure should include all columns, data transfer should omit the specified columns

	tables := config.Tables{{Name: "test", Omit: []string{"column_to_omit"}}}
	anonymiser := NewAnonymiser(&mockReaderWithStructure{}, tables)

	// GetStructure returns all columns
	structure, err := anonymiser.GetStructure()
	require.NoError(t, err)
	assert.Contains(t, structure, "column_test")
	assert.Contains(t, structure, "column_to_omit")

	// GetColumns filters out omitted columns
	columns, err := anonymiser.GetColumns("test")
	require.NoError(t, err)
	assert.Equal(t, []string{"column_test"}, columns)
	assert.NotContains(t, columns, "column_to_omit")

	// ReadTable removes omitted columns from row data
	rowChan := make(chan database.Row)
	defer close(rowChan)

	err = anonymiser.ReadTable("test", rowChan, reader.ReadTableOpt{})
	require.NoError(t, err)

	timeoutChan := time.After(waitTimeout)
	select {
	case row := <-rowChan:
		assert.Contains(t, row, "column_test")
		assert.NotContains(t, row, "column_to_omit")
	case <-timeoutChan:
		assert.FailNow(t, "Failing due to timeout")
	}
}

type mockReader struct{}

func (m *mockReader) GetTables() ([]string, error)  { return []string{"table_test"}, nil }
func (m *mockReader) GetStructure() (string, error) { return "", nil }
func (m *mockReader) GetColumns(string) ([]string, error) {
	return []string{"column_test", "column_to_omit"}, nil
}
func (m *mockReader) GetPreamble() (string, error) { return "", nil }
func (m *mockReader) Close() error                 { return nil }
func (m *mockReader) FormatColumn(tbl string, col string) string {
	return fmt.Sprintf("%s.%s", strconv.Quote(tbl), strconv.Quote(col))
}
func (m *mockReader) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	row := make(database.Row)
	row["column_test"] = "to_be_anonimised"
	row["column_to_omit"] = "sensitive_data"
	rowChan <- row
	return nil
}

type mockReaderWithStructure struct{}

func (m *mockReaderWithStructure) GetTables() ([]string, error) { return []string{"test"}, nil }
func (m *mockReaderWithStructure) GetStructure() (string, error) {
	return "CREATE TABLE test (column_test VARCHAR(255), column_to_omit VARCHAR(255));", nil
}
func (m *mockReaderWithStructure) GetColumns(string) ([]string, error) {
	return []string{"column_test", "column_to_omit"}, nil
}
func (m *mockReaderWithStructure) Close() error { return nil }
func (m *mockReaderWithStructure) FormatColumn(tbl string, col string) string {
	return fmt.Sprintf("%s.%s", strconv.Quote(tbl), strconv.Quote(col))
}
func (m *mockReaderWithStructure) ReadTable(tableName string, rowChan chan<- database.Row, opts reader.ReadTableOpt) error {
	row := make(database.Row)
	row["column_test"] = "to_be_anonimised"
	row["column_to_omit"] = "sensitive_data"
	rowChan <- row
	return nil
}
