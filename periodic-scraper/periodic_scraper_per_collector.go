package periodicscraper

import (
	"context"
	"fmt"
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

	for i := 0; i < len(collectors); i++ {
		if err := ScrapeCollector(ctx, logger, retryMultInterval, prevRuntimes[i], collectors[i], db, finder, isRibsData); err != nil {
			logger.Error().Err(err).Str("collector", collectors[i].Name).Msg("Failed to upsert dumps")
			continue
		}
		successfullyWrittenCollectors = append(successfullyWrittenCollectors, collectors[i])
	}

	return bgpfinder.UpsertCollectors(ctx, logger, db, successfullyWrittenCollectors)
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
	retryInterval := retryMultInterval

	for i := 0; i < allowedRetries; i++ {
		// Run once immediately before waiting for the ticker
		logger.Info().Msg("Starting period run on collectors data")
		err := UpsertBgpDumpsForCollector(ctx, logger, db, finder, prevRuntime, collector, isRibsData)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to update collectors data on initial run")
			if i == allowedRetries-1 {
				return err
			}
		} else {
			logger.Info().Msg("Scraping completed successfully")
			return nil
		}
		time.Sleep(time.Duration(retryInterval))
		retryInterval = retryMultInterval * retryInterval
	}
	return nil
}

func UpsertBgpDumpsForCollector(ctx context.Context,
	logger *logging.Logger,
	db *pgxpool.Pool,
	finder bgpfinder.Finder,
	prevRunTimeEnd time.Time,
	collector bgpfinder.Collector,
	isRibsData bool) error {

	logger.Info().Str("collector", collector.Name).Msg("Starting to scrape collector data")

	var dumpType bgpfinder.DumpType

	if isRibsData {
		dumpType = bgpfinder.DumpTypeRib
	} else {
		dumpType = bgpfinder.DumpTypeUpdates
	}

	query := bgpfinder.Query{
		Collectors: []bgpfinder.Collector{collector},
		DumpType:   dumpType,
		From:       prevRunTimeEnd,              // Start from prevRuntime
		Until:      time.Now().AddDate(0, 0, 1), // Until tomorrow (to ensure we get today's data)
	}

	dumps, err := finder.Find(query)
	if err != nil {
		logger.Error().Err(err).Str("collector", collector.Name).Msg("Finder.Find failed")
		return err
	}

	if len(dumps) == 0 {
		return fmt.Errorf("didn't recieve enough records for collector %s", collector.Name)
	}

	logger.Info().Str("collector", collector.Name).Int("dumps_found", len(dumps)).Msg("Found BGP dumps for collector")

	if err := bgpfinder.UpsertBGPDumps(ctx, logger, db, dumps); err != nil {
		logger.Error().Err(err).Str("collector", collector.Name).Msg("Failed to upsert dumps")
		return err
	}

	return nil
}
