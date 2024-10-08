package bgpfinder

// Global finder instance that includes all the built-in finder
// implementations (RV and RIS for now).
//
// If you have a custom (private) finder, you can either register it
// with this finder instance, or use it directly.
var DefaultFinder = mustInitDefaultFinder()

func mustInitDefaultFinder() Finder {
	f, err := NewMultiFinder(
		NewRouteViewsFinder(),
		NewRISFinder(),
	)
	if err != nil {
		panic(err)
	}
	return f
}

func Projects() ([]Project, error) {
	return DefaultFinder.Projects()
}

func GetProject(name string) (Project, error) {
	return DefaultFinder.Project(name)
}

func Collectors(project string) ([]Collector, error) {
	return DefaultFinder.Collectors(project)
}

func GetCollector(name string) (Collector, error) {
	return DefaultFinder.Collector(name)
}

func Find(query Query) ([]BGPDump, error) {
	return DefaultFinder.Find(query)
}
