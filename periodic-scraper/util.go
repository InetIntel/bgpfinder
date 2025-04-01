package periodicscraper

import (
	"context"
	"flag"
	"fmt"
	"log"
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
	divVal                   = 64
)

type DBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
}

func setupLogger() *logging.Logger {
	loggerConfig := logging.LoggerConfig{
		LogLevel: *flag.String("loglevel", "info", "Log level (debug, info, warn, error)"),
	}
	logger, err := logging.NewLogger(loggerConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	return logger
}

func setupContext() context.Context {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
	defer stop()
	return ctx
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

func setupDB(logger *logging.Logger) *pgxpool.Pool {
	envFile := flag.String("env-file", ".env", "Path to .env file (required if use-db is true)")

	config, err := loadDBConfig(*envFile)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load database configuration")
		return nil
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
		return nil
	}
	defer db.Close()
	logger.Info().Msg("Successfully connected to Database")

	return db
}

func runDb(ctx context.Context,
	logger *logging.Logger,
	db *pgxpool.Pool,
	project string) ([]bgpfinder.Collector, []time.Time, error) {

	stmt := `SELECT name, last_completed_crawl_time FROM collectors where project_name =` + project

	rows, err := db.Query(ctx, stmt)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
		return nil, nil, err
	}
	defer rows.Close()

	// Iterate over rows
	var collectors []bgpfinder.Collector
	var timeArray []time.Time

	for rows.Next() {
		var collector bgpfinder.Collector
		var lastCompletedCrawlTime time.Time // Use time.Time if it's a timestamp

		err := rows.Scan(&collector, &lastCompletedCrawlTime)
		if err != nil {
			log.Fatalf("Scan failed: %v", err)
		}

		fmt.Printf("Collector: %s, Last Completed Crawl Time: %s\n", collector, lastCompletedCrawlTime)
		collectors = append(collectors, collector)
		timeArray = append(timeArray, lastCompletedCrawlTime)
	}

	// Check for errors after iteration
	if err := rows.Err(); err != nil {
		log.Fatalf("Row iteration error: %v", err)
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
