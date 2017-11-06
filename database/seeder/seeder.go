package seeder

import (
	"fmt"
	"reflect"

	"github.com/hellofresh/klepto/database"
)

// Seeder describes values (seeds) that we do not change.
type Seeder struct {
}

// KeepSeedValueUnchanged keeps primary key or any other non-anonymous fields unchanged.
func KeepSeedValueUnchanged(column string, value, typ interface{}) (*database.Cell, error) {
	kind := fmt.Sprintf("%s", reflect.TypeOf(value).Kind())
	cell := &database.Cell{Column: column, Value: value, Type: kind}
	if cell.Type != "" {
		return cell, nil
	}
	return nil, fmt.Errorf("couldn't keep cell value unchanged for column: %v", column)
}
