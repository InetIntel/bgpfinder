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
		collectors, err := finder.Collectors(project.Name)
		if err != nil {
			logger.Error().Err(err).Str("project", project.Name).Msg("Failed to get collectors")
			continue
		}

		logger.Info().
			Str("project", project.Name).
			Int("collector_count", len(collectors)).
			Msg("Found collectors for project")

		if err := UpsertCollectors(ctx, logger, db, collectors); err != nil {
			logger.Error().Err(err).Str("project", project.Name).Msg("Failed to upsert collectors")
			continue
		}

		// For each collector, find BGP dumps
		for _, collector := range collectors {
			logger.Info().Str("collector", collector.Name).Msg("Starting to scrape collector data")

			query := Query{
				Collectors: []Collector{collector},
				DumpType:   DumpTypeAny,
				From:       time.Unix(0, 0), // Start from Unix epoch (1970-01-01)
				Until:      time.Now().AddDate(0, 0, 1), // Until tomorrow (to ensure we get today's data)
			}

			dumps, err := finder.Find(query)
			if err != nil {
				logger.Error().
					Err(err).
					Str("collector", collector.Name).
					Msg("Finder.Find failed")
				continue
			}

			logger.Info().
				Str("collector", collector.Name).
				Int("dumps_found", len(dumps)).
				Msg("Found BGP dumps for collector")

			if err := UpsertBGPDumps(ctx, logger, db, dumps); err != nil {
				logger.Error().
					Err(err).
					Str("collector", collector.Name).
					Msg("Failed to upsert dumps")
				continue
			}
		}
	}
	return nil
}
