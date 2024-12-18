package main

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/alistairking/bgpfinder"
)

func TestParseDataRequest(t *testing.T) {
	// Test parameters
	startTimeStr := "1609459200"
	endTimeStr := "1609545600"
	collectorName := "rrc00"
	dumpTypeStr := "updates"

	// Build the http request with above parameters for /data endpoint
	queryParams := url.Values{}
	queryParams.Add("intervals[]", startTimeStr+","+endTimeStr)
	queryParams.Add("collectors[]", collectorName)
	queryParams.Add("types[]", dumpTypeStr)

	reqURL := &url.URL{
		Path:     "/data",
		RawQuery: queryParams.Encode(),
	}
	req := &http.Request{
		Method: "GET",
		URL:    reqURL,
	}

	// Parse the request
	query, err := parseDataRequest(req)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Verify start and end time
	startTimeInt64, err := strconv.ParseInt(startTimeStr, 10, 64)
	if err != nil {
		t.Fatalf("Invalid start time: %v", err)
	}
	endTimeInt64, err := strconv.ParseInt(endTimeStr, 10, 64)
	if err != nil {
		t.Fatalf("Invalid end time: %v", err)
	}
	expectedFrom := time.Unix(startTimeInt64, 0)
	expectedUntil := time.Unix(endTimeInt64, 0)

	if !query.From.Equal(expectedFrom) {
		t.Errorf("Expected From: %v, got %v", expectedFrom, query.From)
	}

	if !query.Until.Equal(expectedUntil) {
		t.Errorf("Expected Until: %v, got %v", expectedUntil, query.Until)
	}

	// Verify Collectors
	if len(query.Collectors) != 1 || query.Collectors[0].Name != collectorName {
		t.Errorf("Expected Collectors: [%s], got %v", collectorName, query.Collectors)
	}

	// Verify DumpType
	expectedDumpType, err := bgpfinder.DumpTypeString(dumpTypeStr)
	if err != nil {
		t.Fatalf("Invalid dump type: %v", err)
	}
	if query.DumpType != expectedDumpType {
		t.Errorf("Expected DumpType: %v, got %v", expectedDumpType, query.DumpType)
	}
}
