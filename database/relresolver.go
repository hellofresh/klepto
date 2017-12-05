package database

import (
	"errors"
	"fmt"

	"github.com/spf13/viper"
)

// RelationshipResolver ...
type RelationshipResolver struct {
	g ConfigReader
}

// ConfigReader ...
type ConfigReader struct {
	v *viper.Viper
}

// NewConfigReader returns an initialised instance of ConfigReader
func NewConfigReader(v *viper.Viper) *ConfigReader {
	return &ConfigReader{
		v,
	}
}

// ReadPrimaryRecord ...
func (g *ConfigReader) ReadPrimaryRecord() (pRecordType string, err error) {
	c := g.v.Sub("primary_record_type")
	pRecordType = c.AllKeys()[0] // TODO: In the exciting future when
	// users can configure multiple record_types, get all keys, not just the first one.
	if pRecordType == "" {
		return "", errors.New("warning: primary_record_type not set in config")
	}
	return pRecordType, nil
}

// ReadPrimaryRecordLimit returns configured number of records to return
func (g *ConfigReader) ReadPrimaryRecordLimit() (limit string, err error) {
	pRecord, err := g.ReadPrimaryRecord()
	if err != nil {
		return "", err
	}
	c := g.v.GetString(fmt.Sprintf("primary_record_type.%s", pRecord))
	return c, nil
}

// Read all relationships
func (g *ConfigReader) readRelationships() (map[string]string, error) {
	c := g.v.Sub("relationships")
	children := c.AllKeys()
	rels := make(map[string]string, len(children))
	for _, ch := range children {
		rels[ch] = c.GetString(ch)
	}

	return rels, nil

}
