package mysql

import (
	"fmt"
	"reflect"

	"github.com/hellofresh/klepto/database"
)

// Seeder describes values (seeds) that we do not change.
type Seeder struct {
}

// Keep leaves primary key or any other non-anonymous fields unchanged.
func Keep(column string, value, typ interface{}) (*database.Cell, error) {
	kind := fmt.Sprintf("%s", reflect.TypeOf(value).Kind())
	cell := &database.Cell{Column: column, Value: value, Type: kind}
	if cell.Type != "" {
		return cell, nil
	}
	return nil, fmt.Errorf("couldn't keep cell value unchanged for column: %v", column)
}
