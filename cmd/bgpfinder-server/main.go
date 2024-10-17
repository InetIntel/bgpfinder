package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/alistairking/bgpfinder/bgpfinder"
	"github.com/gorilla/mux"
)

func main() {
	router := mux.NewRouter().StrictSlash(true)

	router.HandleFunc("/meta/projects", projectHandler).Methods("GET")
	router.HandleFunc("/meta/projects/{project}", projectHandler).Methods("GET")

	router.HandleFunc("/meta/collectors", collectorHandler).Methods("GET")
	router.HandleFunc("/meta/collectors/{collector}", collectorHandler).Methods("GET")

	router.HandleFunc("/data", dataHandler).Methods("GET")

	fmt.Println("Starting server on :8080")
	log.Fatal(http.ListenAndServe(":8080", router))
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
func dataHandler(w http.ResponseWriter, r *http.Request) {
	query, err := parseDataRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	results, err := bgpfinder.Find(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error finding BGP dumps: %v", err), http.StatusInternalServerError)
		return
	}

	jsonResponse(w, results)
}

// jsonResponse sends a JSON response
func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding JSON: %v", err), http.StatusInternalServerError)
	}
}
