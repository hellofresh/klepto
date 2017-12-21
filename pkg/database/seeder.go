package database

import (
	"fmt"
	"reflect"
)

// Seeder describes values (seeds) that we do not change.
type Seeder struct {
}

// KeepSeedValueUnchanged leaves primary key or any other non-anonymous fields unchanged.
func KeepSeedValueUnchanged(column string, value, typ interface{}) (*Cell, error) {
	kind := fmt.Sprintf("%s", reflect.TypeOf(value).Kind())
	cell := &Cell{Column: column, Value: value, Type: kind}
	if cell.Type != "" {
		return cell, nil
	}
	return nil, fmt.Errorf("couldn't keep cell value unchanged for column: %v", column)
}
