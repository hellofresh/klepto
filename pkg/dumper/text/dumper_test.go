package text

// storeStub's methods implements database.Reader() interface
type storeStub struct{}

func (st *storeStub) getTables() (tables []string, err error) {
	return
}

func (st *storeStub) getStructure() (structure string, err error) {
	return
}

func (st *storeStub) getColumns() (columns []string, err error) {
	return
}

func (st *storeStub) getPreamble() (preamble string, err error) {
	return
}

type dumperTestPair struct {
	structure string
	err       error
}

var dumpTests = []dumperTestPair{
	{"some structure", nil},
	{"some other structure", nil},
}

// func TestDumpStructure(t *testing.T) {
// 	var st storeStub
// 	var structure string
// 	var err error
// 	for _, pair := range dumpTests {
// 		preamble, _ := st.getPreamble()
// 		tables, _ := st.getTables()
// 		var tableStructure string
// 		for _, table := range tables {
// 			tableStructure, err = st.getTableStructure(table)
// 		}
// 		structure = fmt.Sprintf("%s\n%s;\n\n", preamble, tableStructure)
// 		// Check that no error is returned
// 		if structure != pair.structure {
// 			t.Error(
// 				"For", pair.structure,
// 				"expected", pair.err,
// 				"got", err,
// 			)
// 		}
// 	}
// }
