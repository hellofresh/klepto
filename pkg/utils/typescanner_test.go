package utils_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hellofresh/klepto/pkg/utils"
)

var ts = &utils.TypeScanner{}

func TestScan(t *testing.T) {
	values := make(map[string]interface{})
	values["string"] = "Example"
	values["bool"] = true
	values["float"] = 1.23

	for expected, val := range values {
		err := scan(val, expected)
		if err != nil {
			t.Error(err)
		}
	}
}

func scan(val interface{}, expected string) error {
	ts.Scan(val)
	if ts.Detected != expected {
		return errors.New(fmt.Sprintf("%s detected as %s, should be %s", val, ts.Detected, expected))
	}
	return nil
}
