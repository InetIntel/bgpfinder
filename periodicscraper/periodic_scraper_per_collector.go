package periodicscraper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/alistairking/bgpfinder"
	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/jackc/pgx/v5/pgxpool"
)

func PeriodicScraper(ctx context.Context,
	logger *logging.Logger,
	retryMultInterval int64,
	prevRuntimes []time.Time,
	collectors []bgpfinder.Collector,
	db *pgxpool.Pool,
	finder bgpfinder.Finder,
	isRibsData bool,
	expectedLatest time.Time) error {

	var wg sync.WaitGroup

	for i := 0; i < len(collectors); i++ {
		j := i // capture loop variable properly
		wg.Add(1)
		go func() {
			defer wg.Done()
			latestTime, err := ScrapeCollector(ctx, logger, retryMultInterval, prevRuntimes[j], collectors[j], db, finder, isRibsData, expectedLatest)
			if err != nil {
				logger.Error().Err(err).Str("collector", collectors[j].Name).Msg("Failed to scrape collector")
				return
			}
			
			// Update this collector immediately
			if err := bgpfinder.UpsertCollectors(ctx, logger, db, []bgpfinder.Collector{collectors[j]}, getDumpTypeFromBool(isRibsData), latestTime); err != nil {
				logger.Error().Err(err).Str("collector", collectors[j].Name).Msg("Failed to update collector metadata")
			}
		}()
	}

	wg.Wait()

	return nil
}

// PeriodicScraper starts a goroutine that scraps the collectors for data.
// startTime defines the start time from which we collect the for.
// retryMultInterval defines the interval for exponential retry
// finder defines the finder.
// isRibsData tells us if it is a Ribs data we want to collect or updates data.
func ScrapeCollector(ctx context.Context,
	logger *logging.Logger,
	retryMultInterval int64,
	prevRuntime time.Time,
	collector bgpfinder.Collector,
	db *pgxpool.Pool,
	finder bgpfinder.Finder,
	isRibsData bool,
	expectedLatest time.Time) (time.Time, error) { // Changed return type to (time.Time, error)
	
	typeStr := "Updates"
	if isRibsData {
		typeStr = "RIBs"
	}

	allowedRetries := 4

	dumps, err := getDumps(ctx, logger, db, finder, prevRuntime, collector, isRibsData, expectedLatest, retryMultInterval, int64(allowedRetries))

	if dumps == nil && err != nil {
		logger.Error().Err(err).Str("type", typeStr).Msg("Failed to update collectors data for collector: " + collector.Name)
		return time.Time{}, err // Updated return
	}

	if err := bgpfinder.UpsertBGPDumps(ctx, logger, db, dumps); err != nil {
		logger.Error().Err(err).Str("collector", collector.Name).Str("type", typeStr).Msg("Failed to upsert dumps")
		return time.Time{}, err
	}

	// Calculate the latest timestamp found in this scrape
	latestDataTime := prevRuntime
	for _, d := range dumps {
		t := time.Unix(d.Timestamp, 0)
		if t.After(latestDataTime) {
			latestDataTime = t
		}
	}


	logger.Info().
		Str("collector", collector.Name).
		Str("type", typeStr).
		Time("latest_timestamp", latestDataTime).
		Msg("Scraping completed successfully")
	return latestDataTime, nil
}

func getDumps(ctx context.Context,
	logger *logging.Logger,
	db *pgxpool.Pool,
	finder bgpfinder.Finder,
	prevRunTimeEnd time.Time,
	collector bgpfinder.Collector,
	isRibsData bool,
	expectedLatest time.Time,
	retryInterval int64,
	allowedRetries int64) ([]bgpfinder.BGPDump, error) {

	var retry string

	logger.Info().Str("collector", collector.Name).Msg("Starting to scrape collector data")

	dumpType := getDumpTypeFromBool(isRibsData)
	untilNextDay := time.Now().AddDate(0, 0, 1)

	queryFrom := prevRunTimeEnd
	if queryFrom.After(time.Unix(0, 0)) {
		queryFrom = queryFrom.Add(-48 * time.Hour)
	}

	query := bgpfinder.Query{
		Collectors: []bgpfinder.Collector{collector},
		DumpType:   dumpType,
		Intervals: []bgpfinder.Interval{
			{
				From:  queryFrom,
				Until: untilNextDay, // Until tomorrow (to ensure we get today's data)
			},
		},
	}

	dumps, err := finder.Find(query)
	retry = "no"

	mostRecentDump := int64(0)
	for _, dump := range dumps {
		if dump.Timestamp > mostRecentDump {
			mostRecentDump = dump.Timestamp
		}
	}

	if err == nil && len(dumps) == 0 {
		err = fmt.Errorf("didn't recieve enough records for collector %s", collector.Name)
	}

	latest := time.Unix(mostRecentDump, 0)
	if latest.Before(expectedLatest) {
		if expectedLatest.Sub(latest) > (24 * time.Hour) {
			logger.Info().Msgf("collector (%s) appears to be out of date (latest: %s, expected: %s). Skipping retry\n", collector.Name, latest, expectedLatest)
			retry = "no"
			if len(dumps) > 0 {
				err = nil
			}
		} else {
			//err = fmt.Errorf("most recent expected not available (collector: %s got: %s, expected: %s)", collector.Name, latest, expectedLatest)

			// Shane: I don't think we should be treating this as a fatal error -- we can retry, but if we never get the file in time then that shouldn't prevent us from considering the scrape a "success". Especially if we did actually scrape some files, just not the most recent one we were expecting! 
			if err := bgpfinder.UpsertBGPDumps(ctx, logger, db, dumps); err != nil {
				logger.Error().Err(err).Str("collector", collector.Name).Msg("Failed to upsert dumps")
			} else {
				prevRunTimeEnd = latest
			}
			retry = "yes"
		}
	}

	if (retry != "no" && (err != nil || retry == "yes") && allowedRetries > 0) {
		if err == nil {
			logger.Info().Str("collector", collector.Name).Msgf("Still waiting on expected file for %s", expectedLatest)
		}
		logger.Info().Str("collector", collector.Name).Int("retries left", int(allowedRetries)).Msg("Will retry scraping after sleeping.")
		time.Sleep(time.Duration(retryInterval) * time.Second)
		return getDumps(ctx, logger, db, finder, prevRunTimeEnd, collector, isRibsData, expectedLatest, 2*retryInterval, allowedRetries-1)
	}

	logger.Info().Str("collector", collector.Name).Int("dumps_found", len(dumps)).Msg("Found BGP dumps for collector")

	return dumps, nil
}
