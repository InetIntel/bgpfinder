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
	isRibsData bool) error {

	var successfullyWrittenCollectors []bgpfinder.Collector
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < len(collectors); i++ {
		j := i // capture loop variable properly
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := ScrapeCollector(ctx, logger, retryMultInterval, prevRuntimes[j], collectors[j], db, finder, isRibsData); err != nil {
				logger.Error().Err(err).Str("collector", collectors[j].Name).Msg("Failed to upsert dumps")
				return
			}
			mu.Lock()
			successfullyWrittenCollectors = append(successfullyWrittenCollectors, collectors[j])
			mu.Unlock()
		}()
	}

	wg.Wait()

	return bgpfinder.UpsertCollectors(ctx, logger, db, successfullyWrittenCollectors, getTimestampForDumpType(isRibsData))
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
	isRibsData bool) error {

	allowedRetries := 4

	dumps, err := getDumps(ctx, logger, db, finder, prevRuntime, collector, isRibsData, retryMultInterval, int64(allowedRetries))

	if err != nil {
		logger.Error().Err(err).Msg("Failed to update collectors data on initial run")
		return err
	}

	if err := bgpfinder.UpsertBGPDumps(ctx, logger, db, dumps); err != nil {
		logger.Error().Err(err).Str("collector", collector.Name).Msg("Failed to upsert dumps")
		return err
	}

	logger.Info().Msg("Scraping completed successfully")
	return nil
}

func getDumps(ctx context.Context,
	logger *logging.Logger,
	db *pgxpool.Pool,
	finder bgpfinder.Finder,
	prevRunTimeEnd time.Time,
	collector bgpfinder.Collector,
	isRibsData bool,
	retryInterval int64,
	allowedRetries int64) ([]bgpfinder.BGPDump, error) {

	logger.Info().Str("collector", collector.Name).Msg("Starting to scrape collector data")

	dumpType := getDumpTypeFromBool(isRibsData)

	query := bgpfinder.Query{
		Collectors: []bgpfinder.Collector{collector},
		DumpType:   dumpType,
		From:       prevRunTimeEnd,              // Start from prevRuntime
		Until:      time.Now().AddDate(0, 0, 1), // Until tomorrow (to ensure we get today's data)
	}

	dumps, err := finder.Find(query)

	if err == nil && len(dumps) == 0 {
		err = fmt.Errorf("didn't recieve enough records for collector %s", collector.Name)
	}

	if err != nil {
		logger.Error().Err(err).Str("collector", collector.Name).Msg("Finder.Find failed")
		if allowedRetries == 0 {
			return nil, err
		}
		logger.Info().Str("collector", collector.Name).Int("retries left", int(allowedRetries)).Msg("Will retry scraping collectors after sleeping.")
		time.Sleep(time.Duration(retryInterval))
		return getDumps(ctx, logger, db, finder, prevRunTimeEnd, collector, isRibsData, 2*retryInterval, allowedRetries-1)
	}

	logger.Info().Str("collector", collector.Name).Int("dumps_found", len(dumps)).Msg("Found BGP dumps for collector")

	return dumps, nil
}
