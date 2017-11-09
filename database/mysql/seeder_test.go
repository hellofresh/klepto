package mysql

import (
	"testing"

	"github.com/hellofresh/klepto/database"
)

type seedsTestPair struct {
	column     string
	value, typ interface{}
	cell       *database.Cell
}

var seedTests = []seedsTestPair{
	{"somecolumn", "randompassword", "string", &database.Cell{Column: "somecolumn", Value: "randompassword", Type: "string"}},
	{"somecolumn", 1234, "int", &database.Cell{Column: "somecolumn", Value: 1234, Type: "int"}},
}

func TestKeepSeedValueUnchanged(t *testing.T) {

	for _, pair := range seedTests {
		cell, _ := KeepSeedValueUnchanged(pair.column, pair.value, pair.typ)
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
