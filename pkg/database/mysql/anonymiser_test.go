package mysql

import (
	"strings"
	"testing"

	"github.com/hellofresh/klepto/pkg/database"
	"github.com/hellofresh/klepto/pkg/utils"
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

func TestDoNotAnonymise(t *testing.T) {
	for _, pair := range fieldsTests {
		fields := doNotAnonymise(pair.columns)
		if len(fields) != len(pair.fields) {
			t.Error(
				"For", pair.columns,
				"expected", len(pair.fields),
				"got", len(fields),
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

func TestAnonymise(t *testing.T) {

	for _, pair := range anonCellsTests {
		// Trim will remove the prefix first. If the replacement is a literal, then the replacement
		// and the literal will have the same value.
		literal := strings.TrimPrefix(pair.replacement, literalPrefix)
		cell, _ := anonymise(pair.column, pair.replacement)

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

		if foundFaker, err := findFaker(pair.replacement, fakers); pair.replacement != "" && len(literal) == len(pair.replacement) && !foundFaker {
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

// type anonymiseRowsTestPair struct {
// 	table   string
// 	rowChan chan []*database.Cell
// 	endChan chan bool
// 	err     error
// }

// var rowChan chan []*database.Cell
// var endChan chan bool

// var dumpTableTests = []anonymiseRowsTestPair{
// 	{"users", rowChan, endChan, nil},
// }

// func bufferer(buf *bytes.Buffer, rowChan chan []*database.Cell, done chan bool, wg *sync.WaitGroup) {
// 	for {
// 		select {
// 		case cells, more := <-rowChan:
// 			if !more {
// 				done <- true
// 				return
// 			}

// 			len := len(cells)
// 			for i, c := range cells {
// 				if i == 0 {
// 					buf.WriteString("\n(")
// 				}

// 				if c.Type == "string" {
// 					buf.WriteString(fmt.Sprintf("\"%s\"", c.Value))
// 				} else {
// 					buf.WriteString(fmt.Sprintf("%s", c.Value))
// 				}

// 				if i == len-1 {
// 					buf.WriteString("),")
// 				} else {
// 					buf.WriteString(", ")
// 				}
// 			}
// 		case <-done:
// 			wg.Done()
// 			return
// 		}
// 	}
// }

// func TestAnonymiseRows(t *testing.T) {
// 	fromDSN := "root:@tcp(localhost:3307)/fromDb"
// 	inputConn, _ := database.Connect(fromDSN)
// 	// TODO: Pass mock connection here. do not connect to the actual database.
// 	a := NewMySQLAnonymiser(inputConn)
// 	var wg sync.WaitGroup
// 	out := make(chan []*database.Cell, 1000)
// 	done := make(chan bool)
// 	dumper := NewMySQLDumper(inputConn)

// 	wg.Add(len(dumpTableTests))
// 	for _, pair := range dumpTableTests {
// 		// Check that AnonymiseRows runs sucessfully
// 		columns, err := dumper.GetColumns(pair.table)
// 		if err != nil {
// 			t.Error("Could not get columns")
// 		}

// 		buf := bytes.NewBufferString(fmt.Sprintf("\nINSERT INTO `%s` (%s) VALUES", pair.table, strings.Join(columns, ", ")))
// 		go bufferer(buf, out, done, &wg)

// 		if err := a.AnonymiseRows(pair.table, pair.rowChan, pair.endChan); err != nil {
// 			t.Error(
// 				"For", pair.table,
// 				"expected dump to complete successfully. ",
// 				"got an error instead",
// 			)
// 		}
// 	}
// 	close(out)
// 	wg.Wait()
// }
