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
	RISCollectorsUrl  = "https://ris.ripe.net/docs/route-collectors/"
	RISUpdateDuration = time.Hour
	RISRibDuration    = time.Minute * 15
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
		baseUrl := "https://data.ris.ripe.net/" + collector.Name
		fmt.Println("Scraping ", baseUrl)
		dateDirs, err := scraper.ScrapeLinks(baseUrl)
		if err != nil {
			return nil, fmt.Errorf("failed to scrape %s : %v", baseUrl, err)
		}

		// dateDir: YYYY.MM
		for _, dateDir := range dateDirs {
			date, err := time.Parse("2006.01", strings.TrimSuffix(dateDir, "/"))
			if err != nil {
				// some dateDir such as logs/, latest/ do not conform to the format and can be safely ignored
				continue
			}

			if dateInRange(date, query) {
				dateDirLink := baseUrl + "/" + dateDir
				dateDirResults, err := f.scrapeFilesFromDateDir(dateDirLink, allowedPrefixes, collector, query)
				if err != nil {
					fmt.Printf("Warning: failed to process %s: %v\n", dateDirLink, err)
					continue
				}
				results = append(results, dateDirResults...)

			}
		}
	}
	return results, nil
}

// scrapeFilesFromDateDir
func (f *RISFinder) scrapeFilesFromDateDir(dateDirLink string, allowedPrefixes []string, collector Collector, query Query) ([]BGPDump, error) {
	var results []BGPDump

	fmt.Println("Scraping ", dateDirLink)
	files, err := scraper.ScrapeLinks(dateDirLink)
	if err != nil {
		return nil, fmt.Errorf("failed to scrape %s: %v", dateDirLink, err)
	}

	// file: TYPE.YYYYMMDD.HHmm.gz
	for _, file := range files {
		for _, prefix := range allowedPrefixes {
			if strings.HasPrefix(file, prefix) {
				parts := strings.Split(file, ".")
				fileType := parts[0]
				fileDateStr := parts[1] // "20060101"
				fileDate, err := time.Parse("20060102", fileDateStr)
				if err != nil {
					fmt.Printf("Error parsing date %s from file %s: %v\n", fileDateStr, file, err)
					continue
				}
				if dateInRange(fileDate, query) {
					if fileType == "updates" {
						results = append(results, BGPDump{
							URL:       dateDirLink + file,
							Collector: collector,
							Duration:  RISUpdateDuration,
							DumpType:  DumpTypeUpdates,
						})
					}
					if fileType == "bview" || fileType == "view" {
						results = append(results, BGPDump{
							URL:       dateDirLink + file,
							Collector: collector,
							Duration:  RISRibDuration,
							DumpType:  DumpTypeRib,
						})
					}
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
		// fmt.Printf("Debug: link:%s, m:%s\n", link, m)
		collectors = append(collectors, Collector{
			Project: RisProject,
			Name:    m[1],
		})
	}
	return collectors, nil
}

func dateInRange(date time.Time, query Query) bool {
	return (date.Equal(query.From) || date.After(query.From)) && date.Before(query.Until)
}
