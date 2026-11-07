package power

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// Factory builds a Driver from its raw configuration.
type Factory func(config json.RawMessage) (Driver, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register makes a driver available under name. It panics on duplicate names,
// so it is meant to be called from a driver package's init function.
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := factories[name]; dup {
		panic(fmt.Sprintf("power: driver %q registered twice", name))
	}
	factories[name] = f
}

// New builds the driver registered under name.
func New(name string, config json.RawMessage) (Driver, error) {
	mu.RLock()
	f, ok := factories[name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("power: unknown driver %q (available: %v)", name, Drivers())
	}
	d, err := f(config)
	if err != nil {
		return nil, fmt.Errorf("power: configure driver %q: %w", name, err)
	}
	return d, nil
}

// Drivers returns the names of all registered drivers.
func Drivers() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(factories))
	for n := range factories {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
