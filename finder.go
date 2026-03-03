package bgpfinder

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	// TargetLimit is the preferred maximum number of results to return.
	// Can be overridden by BGPFINDER_TARGET_LIMIT env var.
	TargetLimit = 500

	// HardLimit is the absolute maximum number of results to return,
	// even when extending to include all results with the same timestamp.
	// Can be overridden by BGPFINDER_HARD_LIMIT env var.
	HardLimit = 1000

	// MaxScanWindow is the maximum time range to scan in a single database query.
	// Queries exceeding this will be windowed for efficiency.
	// Can be overridden by BGPFINDER_MAX_SCAN_WINDOW (in days) env var.
	MaxScanWindow = 30 * 24 * time.Hour
)

func init() {
	if tl := os.Getenv("BGPFINDER_TARGET_LIMIT"); tl != "" {
		if val, err := strconv.Atoi(tl); err == nil {
			TargetLimit = val
		}
	}
	if hl := os.Getenv("BGPFINDER_HARD_LIMIT"); hl != "" {
		if val, err := strconv.Atoi(hl); err == nil {
			HardLimit = val
		}
	}
	if mw := os.Getenv("BGPFINDER_MAX_SCAN_WINDOW"); mw != "" {
		if val, err := strconv.Atoi(mw); err == nil {
			MaxScanWindow = time.Duration(val) * 24 * time.Hour
		}
	}
}

// Finder Just a sketch of what the base Finder interface might look like.  Everything
// gets built on top of (or under, I guess) this.
type Finder interface {
	// Projects gets the list of projects supported by this finder
	Projects() ([]Project, error)

	// Project gets a specific project
	Project(name string) (Project, error)

	// Collectors gets the list of collectors supported by the given project. All
	// projects if unset.
	Collectors(project string) ([]Collector, error)

	// Collector gets a specific collector by name
	Collector(name string) (Collector, error)

	// Find all the BGP data URLs that match the given query
	Find(query Query) ([]BGPDump, error)

	// Get any collector names that are no longer used, as well as
	// their new name (in the case of a replacement)
	GetCollectorNameAliases(project string) (map[string]string, error)
}

func (d BGPDump) MarshalJSON() ([]byte, error) {
	custom := map[string]interface{}{
		"url":         d.URL,
		"format":      "mrt",  // TODO temporarily hardcoding, may need to fix
		"transport":   "file", // TODO temporarily hardcoding, may need to fix
		"project":     d.Project,
		"collector":   d.Collector.Name,
		"type":        d.DumpType,
		"initialTime": d.Timestamp,
		"duration":    d.Duration,
		"attr":        []string{},
	}
	return json.Marshal(custom)
}

type Project struct {
	Name string `json:"name"`
}

type Collector struct {
	// Project name the collector belongs to
	Project string `json:"project"`

	// Name of the collector
	Name string `json:"name"`
}

func (c Collector) String() string {
	return fmt.Sprintf("%s:%s", c.Project, c.Name)
}

func (c Collector) AsCSV() string {
	return strings.Join([]string{
		c.Project,
		c.Name,
	}, ",")
}

// TODO: add BGPStream backwards compat names.

//go:generate enumer -type=DumpType -json -text -linecomment
type DumpType uint8

const (
	DumpTypeAny     DumpType = 0 // any
	DumpTypeRibs    DumpType = 1 // ribs
	DumpTypeUpdates DumpType = 2 // updates
)

// TODO: think about how this should work -- just keep it simple! no complex query structures
// TODO: add Validate method (e.g., From is before Until, IsADumpType, etc.)
type Query struct {
	// Collectors to search for. All collectors if unset/empty
	Collectors []Collector

	// Query window start time (inclusive)
	From time.Time

	// Query window end time (exclusive)
	Until time.Time

	// Dump type to search for. Any type if unset
	DumpType DumpType

	// Projects to search for. All projects if empty or unset
	Projects []string

	// Min initial time
	MinInitialTime *time.Time

	// Data added since
	DataAddedSince *time.Time

	// Debug type to use in response
	// Debug Debug

	ResponseTime time.Time
}

func (q Query) MarshalJSON() ([]byte, error) {
	custom := make(map[string]interface{})
	if !(q.From.IsZero() && q.Until.IsZero()) {
		custom["intervals"] = []string{strconv.FormatInt(q.From.Unix(), 10) + "," + strconv.FormatInt(q.Until.Unix(), 10)}
	}
	custom["human"] = false
	custom["projects"] = q.Projects
	if len(q.Projects) == 1 {
		custom["project"] = q.Projects[0]
	} else {
		custom["project"] = nil
	}
	collectorNames := make([]string, len(q.Collectors))
	for i, c := range q.Collectors {
		collectorNames[i] = c.Name
	}
	custom["collectors"] = collectorNames
	if len(q.Collectors) == 1 {
		custom["collector"] = q.Collectors[0].Name
	} else {
		custom["collector"] = nil
	}

	custom["types"] = []string{q.DumpType.String()}
	if q.DumpType != DumpTypeAny {
		custom["type"] = q.DumpType.String()
	} else {
		custom["type"] = nil
	}
	return json.Marshal(custom)
}

type DumpDuration time.Duration

func (d DumpDuration) MarshalJSON() ([]byte, error) {
	seconds := time.Duration(d).Seconds()
	return json.Marshal(seconds)
}

// BGPDump represents a single BGP file found by a Finder.
type BGPDump struct {
	// URL of the file
	URL string `json:"url"`

	// Collector that collected this file
	Collector Collector `json:"collector"`

	// Nominal dump duration
	Duration DumpDuration `json:"duration"`

	// Type of dump (RIB or Updates)
	DumpType DumpType `json:"type"`

	// Timestamp of when this dump was created (seconds since epoch)
	Timestamp int64 `json:"timestamp"`

	Project string `json:"project"`
}

// monthInRange checks if any part of the month overlaps with the query range
func monthInRange(date time.Time, query Query) bool {
	monthStart := date
	monthEnd := date.AddDate(0, 1, 0)
	start := query.From

	if query.MinInitialTime != nil {
		start = *query.MinInitialTime
	}

	if query.Until.Unix() != 0 {
		return monthEnd.After(start) && monthStart.Before(query.Until)
	}
	return monthEnd.After(start)
}

// dateInRange checks if a specific timestamp falls within the query range
func dateInRange(date time.Time, query Query) bool {
	unixTime := date.Unix()
	startTime := query.From.Unix()
	if query.MinInitialTime != nil {
		startTime = query.MinInitialTime.Unix()
	}

	if query.Until.Unix() != 0 {
		return unixTime >= startTime && unixTime < query.Until.Unix()
	}
	return unixTime >= startTime
}

// ApplyResultCap applies the pagination capping logic to a list of BGP dumps.
func ApplyResultCap(results []BGPDump) []BGPDump {
	if len(results) <= TargetLimit {
		return results
	}

	// Ensure results are sorted by timestamp and then dump type
	sort.Slice(results, func(i, j int) bool {
		if results[i].Timestamp == results[j].Timestamp {
			return results[i].DumpType < results[j].DumpType
		}
		return results[i].Timestamp < results[j].Timestamp
	})

	limitTs := results[TargetLimit].Timestamp

	if results[0].Timestamp == limitTs {
		// The first timestamp already crosses/hits the target limit.
		// Include all results with this timestamp, up to HardLimit.
		i := 0
		for i < len(results) && results[i].Timestamp == limitTs && i < HardLimit {
			i++
		}
		return results[:i]
	}

	// Otherwise, exclude all results that have the timestamp that crosses the limit.
	j := TargetLimit
	for j > 0 && results[j-1].Timestamp == limitTs {
		j--
	}
	return results[:j]
}
