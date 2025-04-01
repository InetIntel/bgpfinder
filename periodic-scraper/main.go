package periodicscraper

import (
	"context"
	"time"

	"github.com/alistairking/bgpfinder"
	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := setupContext()
	logger := setupLogger()
	db := setupDB(logger)

	tickerRipeRisRibs := time.NewTicker(time.Duration(risRibsInterval))
	tickerRipeRisUpdates := time.NewTicker(time.Duration(risUpdatesInterval))
	tickerRipeRouteViewsRibs := time.NewTicker(time.Duration(routeviewRibsInterval))
	tickerRipeRouteViewsUpdates := time.NewTicker(time.Duration(routeviewUpdatesInterval))

	go func() {
		for {
			select {
			case <-tickerRipeRisRibs.C:
				driver(ctx, logger, db, RIS, true)
			case <-tickerRipeRisUpdates.C:
				driver(ctx, logger, db, RIS, false)
			case <-tickerRipeRouteViewsRibs.C:
				driver(ctx, logger, db, ROUTEVIEWS, true)
			case <-tickerRipeRouteViewsUpdates.C:
				driver(ctx, logger, db, ROUTEVIEWS, false)
			case <-ctx.Done():
				logger.Info().Msg("Stopping periodic scraping due to context cancellation")
				tickerRipeRisRibs.Stop()
				tickerRipeRisUpdates.Stop()
				tickerRipeRouteViewsRibs.Stop()
				tickerRipeRouteViewsUpdates.Stop()
				return
			}
		}
	}()
}

func driver(ctx context.Context, logger *logging.Logger, db *pgxpool.Pool, collector string, isRibs bool) {
	logger.Info().Msgf("Starting periodic collectors data for %s isribs: %t", collector, isRibs)
	collectors, prevRuntimes, err := runDb(ctx, logger, db, collector)
	if err != nil {
		logger.Error().Err(err).Msgf("Failed to run db %s isribs: %t data for collectors", collector, isRibs)
	} else {
		logger.Info().Msgf("Run of db %s isribs: %t completed successfully", collector, isRibs)
	}
	err = PeriodicScraper(ctx, logger, getRetryInterval(collector, isRibs), prevRuntimes, collectors, db, bgpfinder.NewRouteViewsFinder(), isRibs)
	if err != nil {
		logger.Error().Err(err).Msgf("Failed to run periodic scraper %s isribs: %t data for collectors", collector, isRibs)
	} else {
		logger.Info().Msgf("Run of periodic scraper %s isribs: %t completed successfully", collector, isRibs)
	}
}
