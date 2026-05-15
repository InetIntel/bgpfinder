package bgpfinder

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/alistairking/bgpfinder/internal/scraper"
)

const (
	RISRibDuration    = DumpDuration(time.Minute * 2)
	RISUpdateDuration = DumpDuration(time.Minute * 5)
	RISRibPeriod      = DumpDuration(time.Hour * 8)
	RISUpdatePeriod   = DumpDuration(time.Minute * 5)
)

var (
	risRRCPattern = regexp.MustCompile(`(rrc\d\d)`)
)

func getRisCollectorsUrl() string {
	if url := os.Getenv("RIS_COLLECTORS_URL"); url != "" {
		return url
	}
	return "https://ris.ripe.net/docs/route-collectors/"
}

func getRisDataUrl() string {
	if url := os.Getenv("RIS_DATA_URL"); url != "" {
		if !strings.HasSuffix(url, "/") {
			url += "/"
		}
		return url
	}
	return "https://data.ris.ripe.net/"
}

type RISFinder struct {
	BaseFinder
}

func NewRISFinder() *RISFinder {
	f := &RISFinder{}
	f.BaseFinder.Init(RisProject, f.getCollectors)
	return f
}

func (f *RISFinder) GetCollectorNameAliases(project string) (map[string]string, error) {
	if project != "" && project != ProjectRIS {
		return nil, nil
	}
	return map[string]string{}, nil
}

func (f *RISFinder) Collector(name string) (Collector, error) {
	return f.BaseFinder.Collector(name)
}

// Find the BGP data corresponding to the query
// The naming scheme for BGP data is as follows:
// https://data.ris.ripe.net/rrcXX/YYYY.MM/TYPE.YYYYMMDD.HHmm.gz
func (f *RISFinder) Find(query Query) ([]BGPDump, error) {
	var results []BGPDump
	var allowedPrefixes []string

	if query.DumpType == DumpTypeRibs || query.DumpType == DumpTypeAny {
		allowedPrefixes = append(allowedPrefixes, "bview.")
	}
	if query.DumpType == DumpTypeUpdates || query.DumpType == DumpTypeAny {
		allowedPrefixes = append(allowedPrefixes, "updates.")
	}

	for _, collector := range query.Collectors {
		// baseURL: e.g. https://data.ris.ripe.net/rrcXX
		baseURL := getRisDataUrl() + collector.Name

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
	var types []string
	for _, p := range allowedPrefixes {
		if p == "bview." {
			types = append(types, "RIBs")
		} else if p == "updates." {
			types = append(types, "Updates")
		}
	}
	fmt.Printf("Scraping %s from %s\n", strings.Join(types, " and "), dir)
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
						Duration:  f.getDurationFromPrefix(prefix),
						DumpType:  f.getDumpTypeFromPrefix(prefix),
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
	links, err := scraper.ScrapeLinks(getRisCollectorsUrl())
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
			Project: "ris",
			Name:    m[1],
		})
	}
	return collectors, nil
}

func (f *RISFinder) getDumpTypeFromPrefix(prefix string) DumpType {
	switch prefix {
	case "bview.":
		return DumpTypeRibs
	case "updates.":
		return DumpTypeUpdates
	default:
		return DumpTypeAny
	}
}

func (f *RISFinder) getPeriodFromPrefix(prefix string) DumpDuration {
	switch prefix {
	case "bview.":
		return RISRibPeriod
	case "updates.":
		return RISUpdatePeriod
	default:
		return 0
	}
}

func (f *RISFinder) getDurationFromPrefix(prefix string) DumpDuration {
	switch prefix {
	case "bview.":
		return RISRibDuration
	case "updates.":
		return RISUpdateDuration
	default:
		return 0
	}
}
