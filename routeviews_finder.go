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
		return RouteviewsArchiveUrl + override + "/bgpdata/"
	}

	return RouteviewsArchiveUrl + collector.Name + "/bgpdata/"
}

func (f *RouteViewsFinder) monthURL(coll Collector, month time.Time) string {
	return f.getCollectorURL(coll) + month.Format("2006.01") + "/"
}

func (f *RouteViewsFinder) dumpTypeURL(coll Collector, month time.Time, rvdt string) string {
	return f.monthURL(coll, month) + rvdt + "/"
}

// Find BGP dumps matching the specified query
func (f *RouteViewsFinder) Find(query Query) ([]BGPDump, error) {
	// Use all collectors if none are specified
	if len(query.Collectors) == 0 {
		var err error
		query.Collectors, err = f.Collectors("")
		if err != nil {
			return nil, err
		}
	}

	var results []BGPDump
	for _, collector := range query.Collectors {
		BGPDump, err := f.findFiles(collector, query)
		if err != nil {
			// TODO: probably don't need to give up the whole search
			return nil, err
		}

		results = append(results, BGPDump...)
	}
	return results, nil
}

func (f *RouteViewsFinder) findFiles(coll Collector, query Query) ([]BGPDump, error) {
	// RV archive is organized by YYYY.MM, so we first iterate
	// over the months in our query range (there has to be at
	// least one)
	//
	// But first, let's figure out our dump type(s)
	rvdts, err := f.getDumpType(query.DumpType)
	if err != nil {
		return nil, err
	}

	res := []BGPDump{}
	cur := query.From
	for cur.Before(query.Until) {
		for _, rvdt := range rvdts {
			dtUrl := f.dumpTypeURL(coll, cur, rvdt.URL)
			baseF := BGPDump{
				URL:       dtUrl,
				Collector: coll,
				Duration:  rvdt.Duration,
				DumpType:  rvdt.DumpType,
			}
			if dtRes, err := f.findFilesForURL(res, baseF, rvdt, query); err != nil {
				return nil, err
			} else {
				res = dtRes
			}
		}
		cur = cur.AddDate(0, 1, 0)
	}
	return res, nil
}

func (f *RouteViewsFinder) findFilesForURL(res []BGPDump, baseFile BGPDump, rvdt rvDumpType, query Query) ([]BGPDump, error) {
	// Here we have something like:
	// url=http://archive.routeviews.org/route-views3/bgpdata/2020.09/RIBS/
	// now we need to grab the files there and figure out which
	// ones actually match our query.

	links, err := scraper.ScrapeLinks(baseFile.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to get file list from %s: %v",
			baseFile.URL, err)
	}

	for _, fname := range links {
		m := rvdt.Regexp.FindStringSubmatch(fname)
		if m == nil {
			continue
		}
		// TODO: validate m[0] (dump type)
		// build a time from YYYYMMdd.HHMM
		ft, err := time.Parse("20060102.1504", m[1])
		if err != nil {
			// TODO: we need a way to bubble up this kind of "error"
			continue
		}
		if ft.Equal(query.From) ||
			(ft.After(query.From) && ft.Before(query.Until)) {
			f := baseFile
			f.URL = f.URL + fname
			res = append(res, f)
		}
	}
	return res, nil
}

// getDumpType determines the BGP dump types that should be searched for based on the input DumpType.
// If the input is DumpTypeAny, it returns all available dump types (RIB and UPDATES) for RouteViews.
// If a specific DumpType is provided, it returns a slice containing that dump type, or an error if
// the dump type is invalid.
func (f *RouteViewsFinder) getDumpType(dt DumpType) ([]rvDumpType, error) {
	if dt == DumpTypeAny {
		all := []rvDumpType{}
		for _, rvdt := range ROUTEVIEWS_DUMP_TYPES {
			all = append(all, rvdt)
		}
		return all, nil
	}
	rvt, ok := ROUTEVIEWS_DUMP_TYPES[dt]
	if !ok {
		return nil, fmt.Errorf("invalid RouteViews dump type: %v", dt)
	}
	return []rvDumpType{rvt}, nil
}
