package bgpfinder

import (
	"context"
	"fmt"
	"time"

	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StartPeriodicScraping starts a goroutine that periodically calls UpdateCollectorsData.
// interval defines how often to update the database with fresh data.
func StartPeriodicScraping(ctx context.Context, logger *logging.Logger, interval time.Duration, db *pgxpool.Pool, finder Finder) {
	ticker := time.NewTicker(interval)
	go func() {
		// Run once immediately before waiting for the ticker
		logger.Info().Msg("Starting initial collectors data update")
		err := UpdateCollectorsData(ctx, logger, db, finder)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to update collectors data on initial run")
		} else {
			logger.Info().Msg("Initial scraping completed successfully")
		}

		for {
			select {
			case <-ticker.C:
				logger.Info().Msg("Starting periodic collectors data update")
				err := UpdateCollectorsData(ctx, logger, db, finder)
				if err != nil {
					logger.Error().Err(err).Msg("Failed to update collectors data")
				} else {
					logger.Info().Msg("Periodic update completed successfully")
				}
			case <-ctx.Done():
				logger.Info().Msg("Stopping periodic scraping due to context cancellation")
				ticker.Stop()
				return
			}
		}
	}()
}

// UpdateCollectorsData fetches projects and their collectors, then finds BGP dumps and upserts them into the DB.
func UpdateCollectorsData(ctx context.Context, logger *logging.Logger, db *pgxpool.Pool, finder Finder) error {
	projects, err := finder.Projects()
	if err != nil {
		return fmt.Errorf("failed to get projects: %w", err)
	}

	for _, project := range projects {
		if err := scrapeProject(ctx, logger, db, finder, project); err != nil {
			logger.Error().Err(err).Str("project", project.Name).Msg("Failed to scrape project")
			continue
		}
	}
	return nil
}

func scrapeProject(ctx context.Context, logger *logging.Logger, db *pgxpool.Pool, finder Finder, project Project) error {
	collectors, err := finder.Collectors(project.Name)
	if err != nil {
		return fmt.Errorf("failed to get collectors for project %s: %w", project.Name, err)
	}

	logger.Info().
		Str("project", project.Name).
		Int("collector_count", len(collectors)).
		Msg("Found collectors for project")

	if err := UpsertCollectors(ctx, logger, db, collectors, DumpTypeAny, time.Now()); err != nil {
		return fmt.Errorf("failed to upsert collectors for project %s: %w", project.Name, err)
	}

	for _, collector := range collectors {
		if err := scrapeCollector(ctx, logger, db, finder, collector); err != nil {
			logger.Error().Err(err).Str("collector", collector.Name).Msg("Failed to scrape collector")
			continue
		}
	}
	return nil
}

func scrapeCollector(ctx context.Context, logger *logging.Logger, db *pgxpool.Pool, finder Finder, collector Collector) error {
	logger.Info().Str("collector", collector.Name).Msg("Starting to scrape collector data")

	// Use a sensible default interval for periodic scraping (e.g., last 24 hours)
	// But for the very first scrape, we might want more.
	// For now, sticking to the existing "all time" logic but cleaned up.
	query := Query{
		Collectors: []Collector{collector},
		DumpType:   DumpTypeAny,
		Intervals: []Interval{{
			From:  time.Unix(0, 0),
			Until: time.Now().Add(time.Hour * 24),
		}},
	}

	dumps, err := finder.Find(query)
	if err != nil {
		return fmt.Errorf("finder.Find failed for collector %s: %w", collector.Name, err)
	}

	logger.Info().
		Str("collector", collector.Name).
		Int("dumps_found", len(dumps)).
		Msg("Found BGP dumps for collector")

	if err := UpsertBGPDumps(ctx, logger, db, dumps); err != nil {
		return fmt.Errorf("failed to upsert dumps for collector %s: %w", collector.Name, err)
	}

	return nil
}
