package bgpfinder

import (
	"fmt"
	"sync"
)

// BaseFinder provides a common implementation for caching collectors.
// Other finders can embed this to reduce boilerplate.
type BaseFinder struct {
	mu            sync.RWMutex
	collectors    []Collector
	collectorsErr error
	project       Project
	
	// getCollectorsFunc is the project-specific logic to fetch collectors
	getCollectorsFunc func() ([]Collector, error)
}

func (f *BaseFinder) Init(project Project, getCollectorsFunc func() ([]Collector, error)) {
	f.project = project
	f.getCollectorsFunc = getCollectorsFunc
}

func (f *BaseFinder) Projects() ([]Project, error) {
	return []Project{f.project}, nil
}

func (f *BaseFinder) Project(name string) (Project, error) {
	if name == "" || name == f.project.Name {
		return f.project, nil
	}
	return Project{}, nil
}

func (f *BaseFinder) Collectors(project string) ([]Collector, error) {
	if project != "" && project != f.project.Name {
		return nil, nil
	}
	if err := f.ensureCollectors(); err != nil {
		return nil, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.collectors, f.collectorsErr
}

func (f *BaseFinder) Collector(name string) (Collector, error) {
	if err := f.ensureCollectors(); err != nil {
		return Collector{}, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, c := range f.collectors {
		if c.Name == name {
			return c, nil
		}
	}
	return Collector{}, fmt.Errorf("collector not found: %s", name)
}

func (f *BaseFinder) ensureCollectors() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.collectors != nil || f.collectorsErr != nil {
		return f.collectorsErr
	}
	c, err := f.getCollectorsFunc()
	f.collectors = c
	f.collectorsErr = err
	return err
}
