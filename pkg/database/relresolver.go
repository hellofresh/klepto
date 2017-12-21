package database

import (
	"errors"

	"github.com/spf13/viper"
)

// ConfigReader ...
type ConfigReader struct {
	v *viper.Viper
}

func (g *ConfigReader) readPrimaryRecord() (pRecordType string, err error) {
	c := g.v.Sub("primary_record_type")
	pRecordType = c.AllKeys()[0] // TODO: In the exciting future when
	// users can configure multiple record_types, get all keys, not just the first one.
	if pRecordType == "" {
		return "", errors.New("warning: primary_record_type not set in config")
	}
	return pRecordType, nil
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
