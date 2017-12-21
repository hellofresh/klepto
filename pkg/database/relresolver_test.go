package database

import (
	"bytes"
	"testing"

	"github.com/spf13/viper"
)

type primaryRecord struct{}

// Pass and read some config
func createConfig(data string) (cReader ConfigReader, err error) {
	v := viper.New()
	v.SetConfigType("toml")
	var config = []byte(data)
	f := ConfigReader{
		v: v,
	}
	err = f.v.ReadConfig(bytes.NewBuffer(config))
	if err != nil {
		return ConfigReader{}, err
	}
	return f, nil
}

func TestReadPrimaryRecord(t *testing.T) {
	f, err := createConfig(`
[primary_record_type]
"users" = 1
		`)
	if err != nil {
		t.Errorf("Failed creating a test config: %s", err)
	}
	// Set expectation
	expectedRecordType := "users"

	// Check expectation
	recordType, err := f.readPrimaryRecord()

	// Check that readPrimaryRecord() reads the given configuration
	if err != nil {
		t.Fatalf("Expected no error. Got %s", err)
	}
	if expectedRecordType != recordType {
		t.Fatalf("Expected %s. Got %s", expectedRecordType, recordType)
	}
}

func TestReadRelationships(t *testing.T) {
	// given a config of relationships,
	f, err := createConfig(`
[relationships]
"page_views.login_id" = "logins.id"
"logins.user_id" = "users.id"
		`)
	if err != nil {
		t.Fatalf("Failed creating a test config: %s", err)
	}
	// Set expections
	expectedRels := map[string]string{
		"logins.user_id":      "users.id",
		"page_views.login_id": "logins.id",
	}

	// read configured relationships
	relationships, err := f.readRelationships()

	if err != nil {
		t.Fatalf("Expected no error. Got %s", err)
	}

	// Check that all relationships are read correctly
	for k, expectation := range expectedRels {
		if expectation != relationships[k] {
			t.Logf("relationships: %v", relationships)
			t.Fatalf("Expected %s. Got %s", expectation, relationships[k])
		}
	}

}
