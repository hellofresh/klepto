package config

import "errors"

type (
	// Spec represents the global app configuration.
	Spec struct {
		Matchers
		Tables
	}

	// Matchers are variables to store filter data,
	// you can declare a filter once and reuse it among tables.
	Matchers map[string]string

	// Tables are an array of table definitions.
	Tables []*Table

	// Table represents a klepto table definition.
	Table struct {
		// Name is the table name.
		Name string
		// IgnoreData if set to true, it will dump the table structure without importing data.
		IgnoreData bool
		// Filter represents the way you want to filter the results.
		Filter
		// Anonymise anonymise columns.
		Anonymise map[string]string
		// Relationship is an collection of relationship definitions.
		Relationships []*Relationship
	}

	// Filter represents the way you want to filter the results.
	Filter struct {
		// Match is a condition field to dump only certain amount data.
		Match string
		// Limit defines a limit of results to be fetched.
		Limit uint64
		// Sorts is the sort condition for the table.
		Sorts map[string]string
	}

	// Relationship represents the relationship between the table and referenced table.
	Relationship struct {
		// Table is the table name.
		Table string
		// ForeignKey is the table name foreign key.
		ForeignKey string
		// ReferencedTable is the referenced table name.
		ReferencedTable string
		// ReferencedKey is the referenced table primary key name.
		ReferencedKey string
	}
)

// FindByName find a table by its name.
func (t Tables) FindByName(name string) (*Table, error) {
	for _, table := range t {
		if table.Name == name {
			return table, nil
		}
	}

	return nil, errors.New("table not found")
}
