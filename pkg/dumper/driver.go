package dumper

import (
	"sort"
	"sync"
)

var drivers sync.Map

// Register makes a database driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, driver Driver) {
	if driver == nil {
		panic("dumper: Register driver is nil")
	}
	if _, dup := drivers.Load(name); dup {
		panic("dumper: Register called twice for driver " + name)
	}
	drivers.Store(name, driver)
}

// Drivers returns a sorted list of the names of the registered drivers.
func Drivers() []string {
	var list []string
	drivers.Range(func(key, value interface{}) bool {
		name, _ := key.(string)
		list = append(list, name)
		return true
	})
	sort.Strings(list)
	return list
}
