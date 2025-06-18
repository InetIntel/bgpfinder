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
	runScrape := flag.Bool("run-scrape", false, "Run initial scrape for all collectors")
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
	if *useDB && *runScrape {
		bgpfinder.StartPeriodicScraping(ctx, logger, *scrapeFreq, db, bgpfinder.DefaultFinder)
	}

	// // Handle HTTP requests
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/meta/projects", projectHandler(db, logger)).Methods("GET")
	router.HandleFunc("/meta/projects/{project}", projectHandler(db, logger)).Methods("GET")
	router.HandleFunc("/meta/collectors", collectorHandler(db, logger)).Methods("GET")
	router.HandleFunc("/meta/collectors/{collector}", collectorHandler(db, logger)).Methods("GET")
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

type Data struct {
	Resources []bgpfinder.BGPDump `json:"resources"`
}

type DataResponse struct {
	Version string          `json:"version,omitempty"`
	Time    int64           `json:"time,omitempty"`
	Type    string          `json:"type,omitempty"`
	Error   *string         `json:"error"`
	Query   bgpfinder.Query `json:"queryParameters"`
	Data    Data            `json:"data"`
	//QueryParameters QueryParameters `json:"queryParameters"`
}

type DataProjects struct {
	Projects map[string]map[string]map[string]ResponseCollector `json:"projects"`
}

type ProjectsResponse struct {
	Version      string          `json:"version,omitempty"`
	Time         int64           `json:"time,omitempty"`
	Type         string          `json:"type,omitempty"`
	Error        *string         `json:"error"`
	Query        bgpfinder.Query `json:"queryParameters"`
	DataProjects DataProjects    `json:"data"`
	// QueryParameters QueryParameters `json:"queryParameters"`
}

type DataCollectors struct {
	Collectors map[string]ResponseCollector `json:"collectors"`
}

type CollectorsResponse struct {
	Version      string          `json:"version,omitempty"`
	Time         int64           `json:"time,omitempty"`
	Type         string          `json:"type,omitempty"`
	Error        *string         `json:"error"`
	Query        bgpfinder.Query `json:"queryParameters"`
	DataProjects DataCollectors  `json:"data"`
	// QueryParameters QueryParameters `json:"queryParameters"`
}

type DataType struct {
	DumpPeriod     int64  `json:"dumpPeriod"`
	DumpDuration   int64  `json:"dumpDuration"`
	OldestDumpTime string `json:"oldestDumpTime"`
	LatestDumpTime string `json:"latestDumpTime"`
}

type DataTypes struct {
	Ribs    DataType `json:"ribs"`
	Updates DataType `json:"updates"`
}

func (c ResponseCollector) MarshalJSON() ([]byte, error) {
	var ribDuration, ribPeriod, updateDuration, updatePeriod time.Duration
	if c.Project == "ris" {
		ribDuration = time.Duration(bgpfinder.RISRibDuration)
		ribPeriod = time.Duration(bgpfinder.RISRibPeriod)
		updateDuration = time.Duration(bgpfinder.RISUpdateDuration)
		updatePeriod = time.Duration(bgpfinder.RISUpdatePeriod)
	} else if c.Project == "routeviews" {
		ribDuration = time.Duration(bgpfinder.RVRibDuration)
		ribPeriod = time.Duration(bgpfinder.RVRibPeriod)
		updateDuration = time.Duration(bgpfinder.RVUpdateDuration)
		updatePeriod = time.Duration(bgpfinder.RVUpdatePeriod)
	}
	dataTypes := DataTypes{
		Ribs: DataType{
			DumpPeriod:     int64(ribPeriod.Seconds()),
			DumpDuration:   int64(ribDuration.Seconds()),
			OldestDumpTime: c.OldestRibsDump,
			LatestDumpTime: c.LatestRibsDump,
		},
		Updates: DataType{
			DumpPeriod:     int64(updatePeriod.Seconds()),
			DumpDuration:   int64(updateDuration.Seconds()),
			OldestDumpTime: c.OldestUpdatesDump,
			LatestDumpTime: c.LatestUpdatesDump,
		},
	}
	custom := map[string]interface{}{
		"project":   c.Project,
		"dataTypes": dataTypes,
	}
	return json.Marshal(custom)
}

type ResponseCollector struct {
	// Project name the collector belongs to
	Project string `json:"project"`

	// Name of the collector
	Name string `json:"name"`

	OldestRibsDump    string
	OldestUpdatesDump string
	LatestRibsDump    string
	LatestUpdatesDump string
}

// projectHandler handles /meta/projects and /meta/projects/{project} endpoints
func projectHandler(db *pgxpool.Pool, logger *logging.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		projectName := vars["project"]

		oldestLatestDumps, err := bgpfinder.GetCollectorOldestLatest(r.Context(), db)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error checking collector status: %v", err), http.StatusInternalServerError)
			return
		}

		projects, err := bgpfinder.DefaultFinder.Projects()
		if err != nil {
			http.Error(w, fmt.Sprintf("Error fetching projects: %v", err), http.StatusInternalServerError)
			return
		}

		projectsMap := make(map[string]map[string]map[string]ResponseCollector)
		// Find matching projects
		for _, project := range projects {
			if project.Name == projectName || projectName == "" {
				collectors, err := bgpfinder.DefaultFinder.Collectors(project.Name)
				if err == nil {
					if projectsMap[project.Name] == nil {
						projectsMap[project.Name] = map[string]map[string]ResponseCollector{
							"collectors": make(map[string]ResponseCollector),
						}
					}
					for _, collector := range collectors {
						c := ResponseCollector{
							Project:           collector.Project,
							Name:              collector.Name,
							OldestRibsDump:    oldestLatestDumps[collector.Name].OldestRibsDump,
							OldestUpdatesDump: oldestLatestDumps[collector.Name].OldestUpdatesDump,
							LatestRibsDump:    oldestLatestDumps[collector.Name].LatestRibsDump,
							LatestUpdatesDump: oldestLatestDumps[collector.Name].LatestUpdatesDump,
						}
						projectsMap[project.Name]["collectors"][collector.Name] = c
					}
				}
			}
		}

		projectsResponse := ProjectsResponse{
			Query:        bgpfinder.Query{},
			DataProjects: DataProjects{projectsMap},
			Time:         time.Now().Unix(),
			Version:      "2",
			Type:         "data",
			Error:        nil,
		}
		jsonResponse(w, projectsResponse)
	}
}

// collectorHandler handles /meta/collectors and /meta/collectors/{collector} endpoints
func collectorHandler(db *pgxpool.Pool, logger *logging.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		collectorName := vars["collector"]

		collectors, err := bgpfinder.Collectors("")
		if err != nil {
			http.Error(w, fmt.Sprintf("Error fetching collectors: %v", err), http.StatusInternalServerError)
			return
		}

		oldestLatestDumps, err := bgpfinder.GetCollectorOldestLatest(r.Context(), db)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error checking collector status: %v", err), http.StatusInternalServerError)
			return
		}

		collectorsMap := make(map[string]ResponseCollector)

		for _, collector := range collectors {
			if _, ok := oldestLatestDumps[collector.Name]; ok {
				collectorsMap[collector.Name] = ResponseCollector{
					Project:           collector.Project,
					Name:              collector.Name,
					OldestRibsDump:    oldestLatestDumps[collector.Name].OldestRibsDump,
					OldestUpdatesDump: oldestLatestDumps[collector.Name].OldestUpdatesDump,
					LatestRibsDump:    oldestLatestDumps[collector.Name].LatestRibsDump,
					LatestUpdatesDump: oldestLatestDumps[collector.Name].LatestUpdatesDump,
				}
			}
		}

		collectorsResponse := CollectorsResponse{
			Query:        bgpfinder.Query{},
			DataProjects: DataCollectors{collectorsMap},
			Time:         time.Now().Unix(),
			Version:      "2",
			Type:         "data",
			Error:        nil,
		}

		if collectorName == "" {
			// Return all collectors
			jsonResponse(w, collectorsResponse)
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
}

// parseDataRequest parses the HTTP request and builds a bgpfinder.Query object
func parseDataRequest(r *http.Request) (bgpfinder.Query, error) {
	query := bgpfinder.Query{}
	queryParams := r.URL.Query()
	intervalsParams := queryParams["intervals[]"]
	collectorsParams := queryParams["collectors[]"]
	typesParams := queryParams["types[]"]

	collectorParam := queryParams.Get("collector")
	minInitialTime := queryParams.Get("minInitialTime")
	dataAddedSince := queryParams.Get("dataAddedSince")

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

	if minInitialTime != "" {
		minInitialTimeInt, err := strconv.ParseInt(minInitialTime, 10, 64)
		if err != nil {
			return query, fmt.Errorf("invalid minInitialTime: %v", err)
		}
		ts := time.Unix(minInitialTimeInt, 0)
		query.MinInitialTime = &ts
	}

	if dataAddedSince != "" {
		dataAddedSinceInt, err := strconv.ParseInt(dataAddedSince, 10, 64)
		if err != nil {
			return query, fmt.Errorf("invalid dataAddedSince: %v", err)
		}
		ts := time.Unix(dataAddedSinceInt, 0)
		query.DataAddedSince = &ts
	}

	var collectors []bgpfinder.Collector

	if len(collectorsParams) > 0 {
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
	} else if collectorParam != "" {
		allCollectors, err := bgpfinder.Collectors("")
		found := false
		if err != nil {
			return query, fmt.Errorf("error fetching collectors: %v", err)
		}

		for _, c := range allCollectors {
			if collectorParam == c.Name {
				collectors = append(collectors, c)
				found = true
				break
			}
		}
		if !found {
			return query, fmt.Errorf("collector not found: %s", collectorParam)
		}
	} else {
		// Use all collectors
		collectors, err = bgpfinder.Collectors("")
		if err != nil {
			return query, fmt.Errorf("error fetching collectors: %v", err)
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
				Str("project", c.Project).
				Msg("Query collector")
		}

		// Parse "no-cache" flag from query parameters
		noCacheParam := r.URL.Query().Get("no-cache")
		noCache := db == nil || strings.ToLower(noCacheParam) == "true"

		results := []bgpfinder.BGPDump{}

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
		if results == nil {
			results = []bgpfinder.BGPDump{}
		}
		dataResponse := DataResponse{
			Query:   query,
			Data:    Data{results},
			Time:    time.Now().Unix(),
			Version: "2",
			Type:    "data",
			Error:   nil,
		}
		jsonResponse(w, dataResponse)
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
