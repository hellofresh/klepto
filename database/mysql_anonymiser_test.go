package database_test

import (
	"strings"
	"testing"

	"github.com/hellofresh/klepto/database"
	"github.com/hellofresh/klepto/utils"
)

var v interface{}

type fieldsTestPair struct {
	columns []string
	fields  []interface{}
}

var fieldsTests = []fieldsTestPair{
	{[]string{"sid", "name", "password"}, []interface{}{&v, &v, &v}},
	{[]string{"sid", "name", "password"}, []interface{}{3, "FirstName", 1234}},
}

func TestShouldNotAnonymise(t *testing.T) {
	db := new(database.MySQLAnonymiser)
	for _, pair := range fieldsTests {
		fields := db.ShouldNotAnonymise(pair.columns)
		if len(fields) != len(pair.fields) {
			t.Error(
				"For", pair.columns,
				"expected", len(pair.fields),
				"got", len(fields),
			)
		}
	}
}

type seedsTestPair struct {
	column     string
	value, typ interface{}
	cell       *database.Cell
}

var seedTests = []seedsTestPair{
	{"somecolumn", "randompassword", "string", &database.Cell{Column: "somecolumn", Value: "randompassword", Type: "string"}},
	{"somecolumn", 1234, "int", &database.Cell{Column: "somecolumn", Value: 1234, Type: "int"}},
}

func TestKeepsSeedValueUnchanged(t *testing.T) {
	db := new(database.MySQLAnonymiser)

	for _, pair := range seedTests {
		cell, _ := db.KeepsSeedValueUnchanged(pair.column, pair.value, pair.typ)
		// Check that cell types match
		if cell.Type != pair.cell.Type {
			t.Error(
				"For", pair.typ,
				"expected", pair.cell.Type,
				"got", cell.Type,
			)
		}
		// Check that cell values match
		if cell.Value != pair.cell.Value {
			t.Error(
				"For", pair.value,
				"expected", pair.cell.Value,
				"got", cell.Value,
			)
		}
		// Check that cell columns match
		if cell.Column != pair.cell.Column {
			t.Error(
				"For", pair.column,
				"expected", pair.cell.Column,
				"got", cell.Column,
			)
		}
	}
}

type anonCellsTestPair struct {
	column, replacement string
	cell                *database.Cell
}

var anonCellsTests = []anonCellsTestPair{
	{"aColumn", "EmailAddress", &database.Cell{Column: "somecolumn", Type: "string", Value: "fake@address.com"}},
	{"aColumn", "EmailAdd", &database.Cell{Column: "somecolumn", Type: "string", Value: "fake@address.com"}},
	{"sColumn", "literal:4321", &database.Cell{Column: "somecolumn", Type: "string", Value: 4321}},
}

func TestAnonymiseCell(t *testing.T) {
	db := new(database.MySQLAnonymiser)

	for _, pair := range anonCellsTests {
		// Trim will remove the prefix first. If the replacement is a literal, then the replacement
		// and the literal will have the same value.
		literal := strings.TrimPrefix(pair.replacement, database.LiteralPrefix)
		cell, _ := db.AnonymiseCell(pair.column, pair.replacement)

		// Check that literals are not anonymised.
		if len(literal) != len(pair.replacement) && cell.Value != literal {
			t.Error(
				"For", pair.column,
				"expected", literal,
				"got", cell.Value,
			)
		}
		// Check that non-literals are anonymised. We expect that the replacement is generated
		// by one of the faker functions
		fakers := make([]string, 0, len(utils.Functions))
		for k := range utils.Functions {
			fakers = append(fakers, k)
		}

		if foundFaker, err := db.FindsFaker(pair.replacement, fakers); pair.replacement != "" && len(literal) == len(pair.replacement) && !foundFaker {
			// We expect an error if faker function is incorrect.
			if err == nil {
				t.Error(
					"For", pair.column,
					"expected to use a faker from utils.Functions to generate fake data, ",
					"found this faker instead, ", pair.replacement,
				)
			}

		}
	}

}
