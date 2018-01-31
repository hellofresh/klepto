package database

type (
	Table struct {
		Name string
		Row  Row
	}

	Row map[string]interface{}
)

func NewTable(name string) Table {
	return Table{
		Name: name,
		Row:  make(map[string]interface{}),
	}
}
