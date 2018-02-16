package config

import "errors"

type (
	// Spec represents the global app configuration
	Spec struct {
		Tables Tables
	}
	// Tables are an array of table
	Tables []*Table

	// Table represents a klepto table definition
	Table struct {
		Name          string
		Filter        Filter
		Anonymise     map[string]string
		Relationships []*Relationship
	}

	// Filter represents the way you want to filter the results
	Filter struct {
		Match string
		Limit uint64
		Sorts map[string]string
	}

	// Relationship represents a relationship definition
	Relationship struct {
		ReferencedTable string
		ReferencedKey   string
		ForeignKey      string
	}
)

// FindByName filter a table by its name
func (t Tables) FindByName(name string) (*Table, error) {
	for _, table := range t {
		if table.Name == name {
			return table, nil
		}
	}

	return nil, errors.New("table not found")
}
