package mysql

import (
	"fmt"

	"github.com/hellofresh/klepto/database"
)

// Seeder describes values (seeds) that we do not change.
type Seeder struct {
}

// KeepSeedValueUnchanged leaves primary key or any other non-anonymous fields unchanged.
func KeepSeedValueUnchanged(column string, value interface{}, typ string) (*database.Cell, error) {
	cell := &database.Cell{Column: column, Value: value, Type: typ}
	if cell.Type != "" {
		return cell, nil
	}
	return nil, fmt.Errorf("couldn't keep cell value unchanged for column: %v", column)
}
