package bgpfinder

import (
	"fmt"
	"os"
	"regexp"
	"strings"
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
	RVRibDuration    = DumpDuration(time.Minute * 2)
	RVUpdateDuration = DumpDuration(time.Minute * 15)
	RVRibPeriod      = DumpDuration(time.Hour * 2)
	RVUpdatePeriod   = DumpDuration(time.Minute * 15)
)

var (
	ROUTEVIEWS_DUMP_TYPES = map[DumpType]rvDumpType{
		DumpTypeRibs: {
			DumpType: DumpTypeRibs,
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

func getRouteviewsArchiveUrl() string {
	if url := os.Getenv("ROUTEVIEWS_ARCHIVE_URL"); url != "" {
		if !strings.HasSuffix(url, "/") {
			url += "/"
		}
		return url
	}
	return "https://archive.routeviews.org/"
}

// RouteViewsFinder implements the Finder interface
type RouteViewsFinder struct {
	BaseFinder
}

func NewRouteViewsFinder() *RouteViewsFinder {
	f := &RouteViewsFinder{}
	f.BaseFinder.Init(RouteviewsProject, f.getCollectors)
	return f
}

func (f *RouteViewsFinder) GetCollectorNameAliases(project string) (map[string]string, error) {
	if project != "" && project != ProjectRouteViews {
		return nil, nil
	}
	return map[string]string{
		"route-views2.saopaulo": "ix-br2.gru",
		"route-views.saopaulo":  "ix-br.gru",
		"route-views.amsix":  "locix.fra",
	}, nil
}

func (f *RouteViewsFinder) Collector(name string) (Collector, error) {
	return f.BaseFinder.Collector(name)
}

// getCollectors fetches all collectors from RouteviewsArchiveUrl
func (f *RouteViewsFinder) getCollectors() ([]Collector, error) {
	// If we could find a Go rsync client (not a wrapper) we could just do
	// `rsync archive.routeviews.org::` and do some light parsing on the
	// output.
	links, err := scraper.ScrapeLinks(getRouteviewsArchiveUrl())
	if err != nil {
		return nil, fmt.Errorf("failed to get collector list: %v", err)
	}

	collectorNameOverrides, err := f.GetCollectorNameAliases(ProjectRouteViews)
	if err != nil {
		return nil, fmt.Errorf("failed to get collector aliases: %v", err)
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
		// handle known renaming instances
		for collectorName, _ := range collectorNameOverrides {
			if link == collectorName {
				continue
			}
		}

		collectors = append(collectors, Collector{
			Project: ProjectRouteViews,
			Name:    link,
		})
	}
	return collectors, nil
}

// getCollectorURL constructs the collector URL from collector name
func (f *RouteViewsFinder) getCollectorURL(collector Collector) string {
	// Get the collectors that have aliases that should be used for the URL
	collectorNameOverrides, _ := f.GetCollectorNameAliases(ProjectRouteViews)

	// usually a collector's url is https://archive.routeviews.org/<collector.Name>bgpdata/
	// but for route-views2, the url is https://archive.routeviews.org/bgpdata/
	collectorNameOverrides["route-views2"] = ""

	if override, exists := collectorNameOverrides[collector.Name]; exists {
		return getRouteviewsArchiveUrl() + override + "/bgpdata/"
	}

	return getRouteviewsArchiveUrl() + collector.Name + "/bgpdata/"
}

// Find BGP dumps matching the specified query
func (f *RouteViewsFinder) Find(query Query) ([]BGPDump, error) {
	var results []BGPDump
	var allowedPrefixes []string

	if query.DumpType == DumpTypeRibs || query.DumpType == DumpTypeAny {
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
			fmt.Printf("Warning: failed to get month list from %s: %v\n", baseURL, err)
			continue
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
				Duration:  f.getDurationFromPrefix(prefix),
				DumpType:  f.getDumpTypeFromPrefix(prefix),
				Timestamp: timestamp.Unix(),
			})
		}
	}
	return results, nil
}

func (f *RouteViewsFinder) getDumpTypeFromPrefix(prefix string) DumpType {
	switch prefix {
	case "rib.":
		return DumpTypeRibs
	case "updates.":
		return DumpTypeUpdates
	default:
		return DumpTypeAny
	}
}

func (f *RouteViewsFinder) getPeriodFromPrefix(prefix string) DumpDuration {
	switch prefix {
	case "rib.":
		return RVRibPeriod
	case "updates.":
		return RVUpdatePeriod
	default:
		return 0
	}
}

func (f *RouteViewsFinder) getDurationFromPrefix(prefix string) DumpDuration {
	switch prefix {
	case "rib.":
		return RVRibDuration
	case "updates.":
		return RVUpdateDuration
	default:
		return 0
	}
}
