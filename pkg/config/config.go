package config

import (
	"os"
	"strings"

	wErrors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Config-related defaults
const (
	DefaultConfigFileName = ".klepto.toml"
)

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
		Filter Filter
		// Anonymise anonymises columns.
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
func (t Tables) FindByName(name string) *Table {
	for _, table := range t {
		if table.Name == name {
			return table
		}
	}

	return nil
}

// LoadSpecFromFile loads klepto spec from file
func LoadSpecFromFile(configPath string) (Tables, error) {
	if configPath != "" {
		// Use config file from the flag.
		log.Debugf("Reading config from %s ...", configPath)
		viper.SetConfigFile(configPath)
	} else {
		log.Debugf("Reading config from %s ...", DefaultConfigFileName)

		cwd, err := os.Getwd()
		if err != nil {
			return nil, wErrors.Wrap(err, "can't find current working directory")
		}

		viper.SetConfigName(".klepto")
		viper.AddConfigPath(cwd)
		viper.AddConfigPath(".")
	}

	err := viper.ReadInConfig()
	if err != nil {
		return nil, wErrors.Wrap(err, "could not read configurations")
	}

	cfgSpec := new(Spec)
	err = viper.Unmarshal(cfgSpec)
	if err != nil {
		return nil, wErrors.Wrap(err, "could not unmarshal config file")
	}

	// replace matchers aliases in tables with matchers expressions
	for i, t := range cfgSpec.Tables {
		if t.Filter.Match == "" {
			continue
		}

		if m, ok := cfgSpec.Matchers[t.Filter.Match]; ok {
			cfgSpec.Tables[i].Filter.Match = m
		}

		// matcher keys can be lower-cased by the parser - check this case as well
		if m, ok := cfgSpec.Matchers[strings.ToLower(t.Filter.Match)]; ok {
			cfgSpec.Tables[i].Filter.Match = m
		}
	}

	return cfgSpec.Tables, nil
}
