package config

type (
	// Spec represents the global app configuration
	Spec struct {
		Tables []*Table
	}

	// Table represents a klepto table definition
	Table struct {
		Name          string
		Filter        *Filter
		Anonymise     map[string]string
		Relationships []*Relationship
	}

	// Filter represents the way you want to filter the results
	Filter struct {
		Limit int
		Sorts map[string]string
	}

	// Relationship represents a relationship definition
	Relationship struct {
		ReferencedTable string
		ReferencedKey   string
		ForeignKey      string
	}
)
