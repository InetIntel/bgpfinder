package bgpfinder

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/alistairking/bgpfinder/internal/scraper"
)

type rvDumpType struct {
	DumpType DumpType
	Duration time.Duration
	URL      string
	Regexp   *regexp.Regexp
}

const (
	ROUTEVIEWS           = "routeviews"
	RouteviewsArchiveUrl = "https://archive.routeviews.org/"
)

var (
	RouteviewsProject = Project{Name: ROUTEVIEWS}

	ROUTEVIEWS_DUMP_TYPES = map[DumpType]rvDumpType{
		DumpTypeRib: {
			DumpType: DumpTypeRib,
			Duration: time.Hour, // ish
			URL:      "RIBS",
			Regexp:   regexp.MustCompile(`^rib\.(\d{8}\.\d{4})\.bz2$`),
		},
		DumpTypeUpdates: {
			DumpType: DumpTypeUpdates,
			Duration: time.Minute * 15,
			URL:      "UPDATES",
			Regexp:   regexp.MustCompile(`^updates\.(\d{8}\.\d{4})\.bz2$`),
		},
	}
)

// RouteViewsFinder implements the Finder interface
// TODO: refactor a this common caching-finder code out so that RIS and PCH can use it
type RouteViewsFinder struct {
	// Cache of collectors
	mu            *sync.RWMutex
	collectors    []Collector
	collectorsErr error // set if collectors is nil, nil otherwise
}

func NewRouteViewsFinder() *RouteViewsFinder {
	f := &RouteViewsFinder{
		mu: &sync.RWMutex{},
	}

	// TODO: turn this into a goroutine that periodically
	// refreshes collector list (and handles transient failures)?
	c, err := f.getCollectors()
	f.collectors = c
	f.collectorsErr = err

	return f
}

// Projects Retrieves a list of supported projects
func (f *RouteViewsFinder) Projects() ([]Project, error) {
	return []Project{RouteviewsProject}, nil
}

// Project Retrieves a specific project by name
func (f *RouteViewsFinder) Project(name string) (Project, error) {
	if name == "" || name == ROUTEVIEWS {
		return RouteviewsProject, nil
	}
	return Project{}, nil
}

// Collectors gets a list of collectors for a given project
func (f *RouteViewsFinder) Collectors(project string) ([]Collector, error) {
	if project != "" && project != ROUTEVIEWS {
		return nil, nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.collectors, f.collectorsErr
}

// Collector Gets a specific collector by name
func (f *RouteViewsFinder) Collector(name string) (Collector, error) {
	if f.collectorsErr != nil {
		return Collector{}, f.collectorsErr
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	// TODO: add a map to avoid the linear search
	for _, c := range f.collectors {
		if c.Name == name {
			return c, nil
		}
	}
	// not found
	return Collector{}, fmt.Errorf("collector not found: %+v", name)
}

// getCollectors fetches all collectors from RouteviewsArchiveUrl
func (f *RouteViewsFinder) getCollectors() ([]Collector, error) {
	// If we could find a Go rsync client (not a wrapper) we could just do
	// `rsync archive.routeviews.org::` and do some light parsing on the
	// output.
	links, err := scraper.ScrapeLinks(RouteviewsArchiveUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to get collector list: %v", err)
	}

	var collectors []Collector
	for _, link := range links {
		if !strings.HasSuffix(link, "/bgpdata") {
			continue
		}
		link = strings.TrimSuffix(link, "/bgpdata")
		link = strings.TrimPrefix(link, "/")

		// Handle the only special case for now. This is needed because collector.Name is used in other places
		if link == "" {
			link = "route-views2"
		}

		collectors = append(collectors, Collector{
			Project: RouteviewsProject,
			Name:    link,
		})
	}
	return collectors, nil
}

// getCollectorURL constructs the collector URL from collector name
func (f *RouteViewsFinder) getCollectorURL(collector Collector) string {
	// usually a collector's url is https://archive.routeviews.org/<collector.Name>bgpdata/
	// but for route-views2, the url is https://archive.routeviews.org/bgpdata/
	CollectorNameOverride := map[string]string{
		"route-views2": "",
	}

	if override, exists := CollectorNameOverride[collector.Name]; exists {
		return RouteviewsArchiveUrl + override + "bgpdata/"
	}

	return RouteviewsArchiveUrl + collector.Name + "/bgpdata/"
}

// Find BGP dumps matching the specified query
func (f *RouteViewsFinder) Find(query Query) ([]BGPDump, error) {
	var results []BGPDump
	var allowedPrefixes []string

	if query.DumpType == DumpTypeRib || query.DumpType == DumpTypeAny {
		allowedPrefixes = append(allowedPrefixes, "rib.")
	}
	if query.DumpType == DumpTypeUpdates || query.DumpType == DumpTypeAny {
		allowedPrefixes = append(allowedPrefixes, "updates.")
	}

	for _, collector := range query.Collectors {
		// baseURL: https://archive.routeviews.org/<collector_name>/bgpdata/
		baseURL := f.getCollectorURL(collector)

		// monthDirs: YYYY.MM/
		monthDirs, err := scraper.ScrapeLinks(baseURL)
		if err != nil {
			return nil, fmt.Errorf("failed to get month list from %s: %v", baseURL, err)
		}

		for _, monthDir := range monthDirs {
			date, err := time.Parse("2006.01", strings.TrimSuffix(monthDir, "/"))
			if err != nil {
				// Skip directories that don't match the expected format
				continue
			}

			if monthInRange(date, query) {
				for _, prefix := range allowedPrefixes {
					finalDir := baseURL + monthDir
					if prefix == "rib." {
						finalDir += "RIBS/"
					} else {
						finalDir += "UPDATES/"
					}

					dumps, err := f.scrapeFilesFromDir(finalDir, prefix, collector, query)
					if err != nil {
						fmt.Printf("Warning: failed to process %s: %v\n", finalDir, err)
						continue
					}
					results = append(results, dumps...)
				}
			}
		}
	}
	return results, nil
}

func (f *RouteViewsFinder) scrapeFilesFromDir(dir string, prefix string, collector Collector, query Query) ([]BGPDump, error) {
	fmt.Println("Scraping ", dir)
	var results []BGPDump

	files, err := scraper.ScrapeLinks(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to get file list from %s: %v", dir, err)
	}

	for _, file := range files {
		if !strings.HasPrefix(file, prefix) {
			continue
		}

		// file: updates.20150801.0000.bz2
		parts := strings.Split(strings.TrimSuffix(file, ".bz2"), ".")
		if len(parts) != 3 {
			continue
		}

		// Parse both date and time parts in UTC
		timestamp, err := time.Parse("20060102.1504", parts[1]+"."+parts[2])
		if err != nil {
			continue
		}

		if dateInRange(timestamp, query) {
			results = append(results, BGPDump{
				URL:       dir + file,
				Collector: collector,
				Duration:  getDurationFromPrefix(prefix),
				DumpType:  getDumpTypeFromPrefix(prefix),
				Timestamp: timestamp.Unix(),
			})
		}
	}
	return results, nil
}
