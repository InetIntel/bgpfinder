package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alistairking/bgpfinder"
	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"golang.org/x/sync/errgroup"
)

type DBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
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

func main() {
	portPtr := flag.String("port", "8080", "port to listen on")
	logLevel := flag.String("loglevel", "info", "Log level (debug, info, warn, error)")
	scrapeFreq := flag.Duration("scrape-frequency", 168*time.Hour, "Scraping frequency")
	useDB := flag.Bool("use-db", false, "Enable database functionality")
	envFile := flag.String("env-file", ".env", "Path to .env file (required if use-db is true)")
	flag.Parse()

	loggerConfig := logging.LoggerConfig{
		LogLevel: *logLevel,
	}
	logger, err := logging.NewLogger(loggerConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	var db *pgxpool.Pool
	if *useDB {
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

		db, err = pgxpool.New(context.Background(), connStr)
		if err != nil {
			logger.Fatal().Err(err).Msg("Unable to connect to database")
		}
		defer db.Close()
		logger.Info().Msg("Successfully connected to Database")
	}

	// Set up context to handle signals for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
	defer stop()

	// Start periodic scraping with the configured frequency
	if *useDB {
		bgpfinder.StartPeriodicScraping(ctx, logger, *scrapeFreq, db, bgpfinder.DefaultFinder)
	}

	// Handle HTTP requests
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/meta/projects", projectHandler).Methods("GET")
	router.HandleFunc("/meta/projects/{project}", projectHandler).Methods("GET")
	router.HandleFunc("/meta/collectors", collectorHandler).Methods("GET")
	router.HandleFunc("/meta/collectors/{collector}", collectorHandler).Methods("GET")
	router.HandleFunc("/data", dataHandler(db, logger)).Methods("GET")

	server := &http.Server{
		Addr:    ":" + *portPtr,
		Handler: router,
	}

	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		logger.Error().Err(err).Msgf("Failed to listen on port %s", *portPtr)
	}
	logger.Info().Msgf("Starting server on %s", server.Addr)

	// Use errgroup to manage goroutines
	eg, ctx := errgroup.WithContext(ctx)

	// Start the HTTP server in a goroutine
	eg.Go(func() error {
		if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	// Wait for the context to be canceled and then shut down the server
	eg.Go(func() error {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Minute)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return nil
	})

	// Wait for all goroutines to finish
	if err := eg.Wait(); err != nil {
		logger.Error().Err(err).Msg("HTTP server error")
	} else {
		logger.Info().Msg("HTTP server gracefully stopped")
	}
}

// projectHandler handles /meta/projects and /meta/projects/{project} endpoints
func projectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projectName := vars["project"]

	projects, err := bgpfinder.Projects()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error fetching projects: %v", err), http.StatusInternalServerError)
		return
	}

	if projectName == "" {
		// Return all projects
		jsonResponse(w, projects)
	} else {
		// Return specific project if exists
		for _, project := range projects {
			if project.Name == projectName {
				jsonResponse(w, project)
				return
			}
		}
		http.Error(w, "Project not found", http.StatusNotFound)
	}
}

// collectorHandler handles /meta/collectors and /meta/collectors/{collector} endpoints
func collectorHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collectorName := vars["collector"]

	collectors, err := bgpfinder.Collectors("")
	if err != nil {
		http.Error(w, fmt.Sprintf("Error fetching collectors: %v", err), http.StatusInternalServerError)
		return
	}

	if collectorName == "" {
		// Return all collectors
		jsonResponse(w, collectors)
	} else {
		// Return specific collector if exists
		for _, collector := range collectors {
			if collector.Name == collectorName {
				jsonResponse(w, collector)
				return
			}
		}
		http.Error(w, "Collector not found", http.StatusNotFound)
	}
}

// parseDataRequest parses the HTTP request and builds a bgpfinder.Query object
func parseDataRequest(r *http.Request) (bgpfinder.Query, error) {
	query := bgpfinder.Query{}

	intervalsParams := r.URL.Query()["intervals[]"]
	collectorsParams := r.URL.Query()["collectors[]"]
	typesParams := r.URL.Query()["types[]"]

	// Parse interval
	if len(intervalsParams) == 0 {
		return query, fmt.Errorf("at least one interval is required")
	}

	times := strings.Split(intervalsParams[0], ",")
	if len(times) != 2 {
		return query, fmt.Errorf("invalid interval format. Expected format: start,end")
	}

	startInt, err := strconv.ParseInt(times[0], 10, 64)
	if err != nil {
		return query, fmt.Errorf("invalid start time: %v", err)
	}

	endInt, err := strconv.ParseInt(times[1], 10, 64)
	if err != nil {
		return query, fmt.Errorf("invalid end time: %v", err)
	}

	query.From = time.Unix(startInt, 0)
	query.Until = time.Unix(endInt, 0)

	// Parse collectors
	var collectors []bgpfinder.Collector
	if len(collectorsParams) == 0 {
		// Use all collectors
		collectors, err = bgpfinder.Collectors("")
		if err != nil {
			return query, fmt.Errorf("error fetching collectors: %v", err)
		}
	} else {
		// Use specified collectors
		allCollectors, err := bgpfinder.Collectors("")
		if err != nil {
			return query, fmt.Errorf("error fetching collectors: %v", err)
		}

		collectorMap := make(map[string]bgpfinder.Collector)
		for _, c := range allCollectors {
			collectorMap[c.Name] = c
		}

		for _, name := range collectorsParams {
			if collector, exists := collectorMap[name]; exists {
				collectors = append(collectors, collector)
			} else {
				return query, fmt.Errorf("collector not found: %s", name)
			}
		}
	}
	query.Collectors = collectors

	// Parse types
	if len(typesParams) == 0 {
		query.DumpType = bgpfinder.DumpTypeAny
	} else {
		// Use the first type parameter
		dumpType, err := bgpfinder.DumpTypeString(typesParams[0])
		if err != nil {
			return query, fmt.Errorf("invalid type: %s", typesParams[0])
		}
		query.DumpType = dumpType
	}

	return query, nil
}

// dataHandler handles /data endpoint
func dataHandler(db *pgxpool.Pool, logger *logging.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query, err := parseDataRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Log the parsed query details in UTC
		logger.Info().
			Time("from", query.From.UTC()).
			Time("until", query.Until.UTC()).
			Str("dump_type", query.DumpType.String()).
			Int("collector_count", len(query.Collectors)).
			Msg("Parsed query parameters")

		// Log collector details
		for _, c := range query.Collectors {
			logger.Info().
				Str("collector_name", c.Name).
				Str("project", c.Project.Name).
				Msg("Query collector")
		}

		// Parse "no-cache" flag from query parameters
		noCacheParam := r.URL.Query().Get("no-cache")
		noCache := db == nil || strings.ToLower(noCacheParam) == "true"

		var results []bgpfinder.BGPDump

		if noCache {
			// If "no-cache" is true, fetch data from remote source
			logger.Info().Msg("No-cache flag detected or DB not connected. Fetching data from remote source.")
			results, err = bgpfinder.Find(query)
			if err != nil {
				http.Error(w, fmt.Sprintf("Error finding BGP dumps: %v", err), http.StatusInternalServerError)
				return
			}
		} else {
			// Fetch data from the database
			logger.Info().Msg("Fetching BGP dumps from the database.")
			results, err = bgpfinder.FetchDataFromDB(r.Context(), db, query)
			if err != nil {
				http.Error(w, fmt.Sprintf("Error fetching BGP dumps from DB: %v", err), http.StatusInternalServerError)
				return
			}

			// If no data found in DB, optionally fetch from remote
			if len(results) == 0 {
				logger.Info().Msg("No BGP dumps found in DB. Fetching from remote source.")
				results, err = bgpfinder.Find(query)
				if err != nil {
					http.Error(w, fmt.Sprintf("Error finding BGP dumps: %v", err), http.StatusInternalServerError)
					return
				}

				// Optionally, upsert the fetched data into the DB for future caching
				if len(results) > 0 {
					err = bgpfinder.UpsertBGPDumps(r.Context(), logger, db, results)
					if err != nil {
						logger.Error().Err(err).Msg("Failed to upsert newly fetched BGP dumps into DB")
						// Continue without failing the request
					} else {
						logger.Info().Int("dumps_upserted", len(results)).Msg("Successfully upserted BGP dumps into DB")
					}
				}
			}
		}

		jsonResponse(w, results)
	}
}

// jsonResponse sends a JSON response
func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding JSON: %v", err), http.StatusInternalServerError)
	}
}
