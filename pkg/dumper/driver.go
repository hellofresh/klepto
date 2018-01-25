package dumper

import (
	"sort"
	"sync"

	log "github.com/sirupsen/logrus"
)

var drivers sync.Map

// Register makes a database driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, driver Driver) {
	if driver == nil {
		log.Fatal("dumper: Register driver is nil")
	}
	if _, dup := drivers.Load(name); dup {
		log.Fatalf("dumper: Register called twice for driver %s", name)
	}
	drivers.Store(name, driver)
}

// Drivers returns a sorted list of the names of the registered drivers.
func Drivers() []string {
	var list []string
	drivers.Range(func(key, value interface{}) bool {
		name, ok := key.(string)
		if ok {
			list = append(list, name)
		}
		return true
	})
	sort.Strings(list)
	return list
}
