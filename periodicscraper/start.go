package periodicscraper

import (
	"context"
	"sync"
	"time"

	"github.com/alistairking/bgpfinder"
	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Start(logger *logging.Logger, envFile *string) {
	db := setupDB(logger, envFile)
	defer db.Close()
	ctx, stop := setupContext()
	defer stop()

	logger.Info().Msg("Starting runn")

	var wg sync.WaitGroup

	projectTuples := getProjectTuples()

	for _, projectTuple := range projectTuples {
		// Start each scraping task in its own goroutine
		tuple := projectTuple
		wg.Add(1)
		go func() {
			defer wg.Done()
			startScraping(ctx, logger, db, tuple)
		}()
	}

	// Wait for all scraping tasks to complete
	wg.Wait()

	<-ctx.Done() // Wait for context cancellation

	logger.Info().Msg("Exiting Main()")
}

func startScraping(ctx context.Context,
	logger *logging.Logger,
	db *pgxpool.Pool,
	projectTuple ProjectTuple) {
	wait(projectTuple.interval, logger)
	for {
		if ctx.Err() != nil {
			return
		}
		startTime := time.Now()
		driver(ctx, logger, db, projectTuple.project, projectTuple.isRibs)
		elapsedTime := time.Since(startTime)
		logger.Info().Msgf("Scraping runtime for project %s and isribs %t is %v", projectTuple.project, projectTuple.isRibs, elapsedTime)
		wait(projectTuple.interval, logger) // or <-tick.C
	}
}

func wait(intervalSeconds int64, logger *logging.Logger) {
	waitTill := nextDivisibleTimestamp(intervalSeconds)
	waitUntilTimestamp(waitTill)
	logger.Info().Msgf("Reached target time: %v", waitTill)
}

func nextDivisibleTimestamp(intervalSeconds int64) time.Time {
	now := time.Now()
	interval := time.Duration(intervalSeconds) * time.Second
	remainder := now.Unix() % int64(interval.Seconds()) // This is the modulo time of now with the interval frequency
	if remainder == 0 {
		return now
	}
	return now.Add(time.Duration(int64(interval.Seconds())-remainder+epsilonTime) * time.Second)
}

func waitUntilTimestamp(targetTime time.Time) {
	duration := time.Until(targetTime)

	if duration <= 0 {
		return
	}

	time.Sleep(duration)
}

func driver(ctx context.Context, logger *logging.Logger, db *pgxpool.Pool, project string, isRibs bool) {
	logger.Info().Msgf("Starting periodic collectors data for %s isribs: %t", project, isRibs)
	collectors, prevRuntimes, err := getCollectorsAndPrevRuntime(ctx, logger, db, project, isRibs)
	if err != nil {
		logger.Error().Err(err).Msgf("Failed to run db to collect data for %s isribs: %t data for collectors", project, isRibs)
	} else {
		logger.Info().Msgf("Run of db on %s isribs: %t completed successfully", project, isRibs)
	}
	var finder bgpfinder.Finder
	if project == RIS {
		finder = bgpfinder.NewRISFinder()
	} else {
		finder = bgpfinder.NewRouteViewsFinder()
	}
	err = PeriodicScraper(ctx, logger, getRetryInterval(project, isRibs), prevRuntimes, collectors, db, finder, isRibs)
	if err != nil {
		logger.Error().Err(err).Msgf("Failed to run periodic scraper %s isribs: %t data for collectors", project, isRibs)
	} else {
		logger.Info().Msgf("Run of periodic scraper %s isribs: %t completed successfully", project, isRibs)
	}
}
