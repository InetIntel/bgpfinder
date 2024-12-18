package bgpfinder

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/alistairking/bgpfinder/internal/scraper"
)

const (
	RIS = "ris"
	// RISCollectorsUrl : it's tempting, but we can't use
	// https://www.ris.ripe.net/peerlist/ because it only lists
	// currently-active collectors.
	RISCollectorsUrl = "https://ris.ripe.net/docs/route-collectors/"
)

var (
	RisProject    = Project{Name: RIS}
	risRRCPattern = regexp.MustCompile(`(rrc\d\d)`)
)

type RISFinder struct {
	// Cache of collectors
	mu            *sync.RWMutex
	collectors    []Collector
	collectorsErr error // set if collectors is nil, nil otherwise
}

func NewRISFinder() *RISFinder {
	f := &RISFinder{
		mu: &sync.RWMutex{},
	}

	// TODO: turn this into a goroutine that periodically
	// refreshes collector list (and handles transient failures)?
	c, err := f.getCollectors()
	f.collectors = c
	f.collectorsErr = err

	return f
}

func (f *RISFinder) Projects() ([]Project, error) {
	return []Project{RisProject}, nil
}

func (f *RISFinder) Project(name string) (Project, error) {
	if name == "" || name == RIS {
		return RisProject, nil
	}
	return Project{}, nil
}

func (f *RISFinder) Collectors(project string) ([]Collector, error) {
	if project != "" && project != RIS {
		return nil, nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.collectors, f.collectorsErr
}

func (f *RISFinder) Collector(name string) (Collector, error) {
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
	return Collector{}, nil
}

// Find the BGP data corresponding to the query
// The naming scheme for BGP data is as follows:
// https://data.ris.ripe.net/rrcXX/YYYY.MM/TYPE.YYYYMMDD.HHmm.gz
func (f *RISFinder) Find(query Query) ([]BGPDump, error) {
	var results []BGPDump
	var allowedPrefixes []string

	if query.DumpType == DumpTypeRib || query.DumpType == DumpTypeAny {
		allowedPrefixes = append(allowedPrefixes, "bview.")
	}
	if query.DumpType == DumpTypeUpdates || query.DumpType == DumpTypeAny {
		allowedPrefixes = append(allowedPrefixes, "updates.")
	}

	for _, collector := range query.Collectors {
		// baseURL: https://data.ris.ripe.net/rrcXX
		baseURL := "https://data.ris.ripe.net/" + collector.Name

		monthDirs, err := scraper.ScrapeLinks(baseURL)
		if err != nil {
			return nil, fmt.Errorf("failed to scrape %s : %v", baseURL, err)
		}

		// monthDir: YYYY.MM
		for _, monthDir := range monthDirs {
			date, err := time.Parse("2006.01", strings.TrimSuffix(monthDir, "/"))
			if err != nil {
				// some links such as logs/, latest/ do not conform to the format and can be safely ignored
				continue
			}

			if monthInRange(date, query) {
				finalDir := baseURL + "/" + monthDir
				dumps, err := f.scrapeFilesFromDir(finalDir, allowedPrefixes, collector, query)
				if err != nil {
					fmt.Printf("Warning: failed to process %s: %v\n", finalDir, err)
					continue
				}
				results = append(results, dumps...)

			}
		}
	}
	return results, nil
}

// scrapeFilesFromDir
func (f *RISFinder) scrapeFilesFromDir(dir string, allowedPrefixes []string, collector Collector, query Query) ([]BGPDump, error) {
	fmt.Println("Scraping ", dir)
	var results []BGPDump

	files, err := scraper.ScrapeLinks(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to scrape %s: %v", dir, err)
	}

	// file: TYPE.YYYYMMDD.HHmm.gz
	for _, file := range files {
		for _, prefix := range allowedPrefixes {
			if strings.HasPrefix(file, prefix) {
				parts := strings.Split(file, ".")
				fileDateStr := parts[1] // "20060101"
				fileTimeStr := parts[2] // "1504"

				// Parse both date and time parts in UTC
				timestamp, err := time.Parse("20060102.1504", fileDateStr+"."+fileTimeStr)
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
		}
	}
	return results, nil
}

// getCollectors fetches ALL Ris collectors
func (f *RISFinder) getCollectors() ([]Collector, error) {
	links, err := scraper.ScrapeLinks(RISCollectorsUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to get collector list: %v", err)
	}

	var collectors []Collector
	for _, link := range links {
		m := risRRCPattern.FindStringSubmatch(link)
		if len(m) != 2 {
			continue
		}

		collectors = append(collectors, Collector{
			Project: RisProject,
			Name:    m[1],
		})
	}
	return collectors, nil
}
