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

	if err := UpsertCollectors(ctx, logger, db, collectors, DumpTypeAny, time.Unix(0, 0)); err != nil {
		return fmt.Errorf("failed to upsert collectors for project %s: %w", project.Name, err)
	}

	return nil
}
