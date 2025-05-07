package periodicscraper

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alistairking/bgpfinder"
	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const (
	RIS                      = "ris"
	ROUTEVIEWS               = "routeviews"
	routeviewRibsInterval    = (2*60 + 0) * 60
	routeviewUpdatesInterval = (0*60 + 15) * 60
	risRibsInterval          = (8*60 + 0) * 60
	risUpdatesInterval       = (0*60 + 5) * 60
	divVal                   = 128
	epsilonTime              = 30 // buffer time so that the scraper code doesn't immediately kick off at the designated time.
)

type DBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
}

type ProjectTuple struct {
	project  string
	isRibs   bool
	interval int64
}

func getProjectTuples() [4]ProjectTuple {
	return [4]ProjectTuple{
		{RIS, true, risRibsInterval},
		{RIS, false, risUpdatesInterval},
		{ROUTEVIEWS, true, routeviewRibsInterval},
		{ROUTEVIEWS, false, routeviewUpdatesInterval},
	}
}

func loadDBConfig(envFile string) (*DBConfig, error) {
	if err := godotenv.Load(envFile); err != nil {
		return nil, fmt.Errorf("error loading env file: %w", err)
	}

	config := &DBConfig{
		Host:     os.Getenv("POSTGRES_HOST"),
		Port:     os.Getenv("POSTGRES_PORT"),
		User:     os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
		DBName:   os.Getenv("POSTGRES_DB"),
	}

	// Validate required fields
	if config.User == "" || config.Password == "" || config.DBName == "" {
		return nil, fmt.Errorf("missing required database configuration")
	}

	return config, nil
}

func setupDB(logger *logging.Logger, envFile *string) *pgxpool.Pool {
	config, err := loadDBConfig(*envFile)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load database configuration")
	}
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		config.User,
		config.Password,
		config.Host,
		config.Port,
		config.DBName,
	)

	db, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to connect to database")
	}
	logger.Info().Msg("Successfully connected to Database")
	return db
}

func setupContext() (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
	return ctx, stop
}

func getCollectorsAndPrevRuntime(ctx context.Context,
	logger *logging.Logger,
	db *pgxpool.Pool,
	project string,
	isRibs bool) ([]bgpfinder.Collector, []time.Time, error) {

	stmt := `SELECT name,` + getTimestampForDumpType(isRibs) + ` FROM collectors where project_name = $1`

	rows, err := db.Query(ctx, stmt, project)
	if err != nil {
		logger.Error().Err(err).Msg("Query failed")
		return nil, nil, err
	}
	defer rows.Close()

	// Iterate over rows
	var collectors []bgpfinder.Collector
	var timeArray []time.Time

	for rows.Next() {
		var collectorName string
		var collector bgpfinder.Collector
		var lastCompletedCrawlTime time.Time // Use time.Time if it's a timestamp

		err := rows.Scan(&collectorName, &lastCompletedCrawlTime)
		if err != nil {
			logger.Error().Err(err).Msg("Scan failed")
		}
		collector.Project.Name = project
		collector.Name = collectorName

		fmt.Printf("Collector: %s, Last Completed Crawl Time: %s\n", collectorName, lastCompletedCrawlTime)
		collectors = append(collectors, collector)
		timeArray = append(timeArray, lastCompletedCrawlTime)
	}

	// Check for errors after iteration
	if err := rows.Err(); err != nil {
		logger.Error().Err(err).Msg("Row iteration error")
	}

	return collectors, timeArray, nil
}

func getRetryInterval(project string, isRibs bool) int64 {
	switch project {
	case ROUTEVIEWS:
		if isRibs {
			return routeviewRibsInterval / divVal
		} else {
			return routeviewUpdatesInterval / divVal
		}
	case RIS:
		if isRibs {
			return risRibsInterval / divVal
		} else {
			return risUpdatesInterval / divVal
		}
	}
	return 0
}

func getDumpTypeFromBool(isRibs bool) bgpfinder.DumpType {
	var dumpType bgpfinder.DumpType

	if isRibs {
		dumpType = bgpfinder.DumpTypeRib
	} else {
		dumpType = bgpfinder.DumpTypeUpdates
	}

	return dumpType
}

func getTimestampForDumpType(isRibs bool) string {
	var timestampField string

	if isRibs {
		timestampField = `last_completed_crawl_time_ribs`
	} else {
		timestampField = `last_completed_crawl_time_updates`
	}

	return timestampField
}
