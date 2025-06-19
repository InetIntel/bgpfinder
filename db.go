package bgpfinder

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UpsertCollectors inserts or updates collector records.
func UpsertCollectors(ctx context.Context, logger *logging.Logger, db *pgxpool.Pool, collectors []Collector, dumptType DumpType, timestamp time.Time) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to begin transaction for UpsertCollectors")
		return err
	}
	defer tx.Rollback(ctx)
	// Define the SQL query

	var timestampField string
	var timestampValue string
	var timestampCondition string

	switch dumptType {
	case DumpTypeRibs:
		timestampField = `last_completed_crawl_time_ribs`
		timestampValue = `$4`
		timestampCondition = timestampField + ` = EXCLUDED.` + timestampField
	case DumpTypeUpdates:
		timestampField = `last_completed_crawl_time_updates`
		timestampValue = `$4`
		timestampCondition = timestampField + ` = EXCLUDED.` + timestampField
	case DumpTypeAny:
		timestampField = `last_completed_crawl_time_ribs, last_completed_crawl_time_updates`
		timestampValue = `$4, $5`
		timestampCondition = `last_completed_crawl_time_ribs = EXCLUDED.last_completed_crawl_time_ribs,
			last_completed_crawl_time_updates = EXCLUDED.last_completed_crawl_time_updates`
	}

	stmt := `
		INSERT INTO collectors (name, project_name, cdate, mdate, most_recent_file_timestamp, ` + timestampField + `)
		VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, COALESCE((SELECT max(timestamp) FROM bgp_dumps WHERE collector_name = $3), '1970-01-01 00:00:00'), ` + timestampValue + `)
		ON CONFLICT (name) DO UPDATE
		SET project_name = EXCLUDED.project_name,
			mdate = EXCLUDED.mdate,
			most_recent_file_timestamp = EXCLUDED.most_recent_file_timestamp,` + timestampCondition

	logger.Info().Int("collector_count", len(collectors)).Msg("Upserting collectors into DB")
	for _, c := range collectors {
		logger.Debug().Str("collector", c.Name).Str("project", c.Project).Msg("Executing upsert for collector")
		collectorName := c.Name

		var ct pgconn.CommandTag
		var err error

		if dumptType == DumpTypeAny {
			ct, err = tx.Exec(ctx, stmt, collectorName, c.Project, collectorName, timestamp, timestamp)
		} else {
			ct, err = tx.Exec(ctx, stmt, collectorName, c.Project, collectorName, timestamp)
		}

		if err != nil {
			logger.Error().Err(err).Str("collector", c.Name).Msg("Failed to execute upsert")
			return err
		}
		logger.Debug().Str("collector", c.Name).Str("command_tag", ct.String()).Msg("Executed upsert for collector")
	}

	err = tx.Commit(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to commit transaction for UpsertCollectors")
		return err
	}
	logger.Info().Msg("Transaction committed successfully for UpsertCollectors")

	return nil
}

// UpsertBGPDumps inserts or updates BGP dump records in batches.
func UpsertBGPDumps(ctx context.Context, logger *logging.Logger, db *pgxpool.Pool, dumps []BGPDump) error {
	const batchSize = 10000 // Define an appropriate batch size

	total := len(dumps)
	for start := 0; start < total; start += batchSize {
		end := start + batchSize
		if end > total {
			end = total
		}
		batch := dumps[start:end]
		logger.Info().Int("batch_start", start).Int("batch_end", end).Int("current_batch_size", len(batch)).Msg("Upserting BGP dumps batch into DB")

		tx, err := db.Begin(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to begin transaction for UpsertBGPDumps batch")
			return err
		}

		stmt := `
			INSERT INTO bgp_dumps (collector_name, url, dump_type, duration, timestamp, cdate, mdate)
			VALUES ($1, $2, $3, $4, to_timestamp($5), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT (collector_name, url) DO UPDATE
			SET dump_type = EXCLUDED.dump_type,
				duration = EXCLUDED.duration,
				timestamp = EXCLUDED.timestamp,
				mdate = EXCLUDED.mdate
		`

		for _, d := range batch {
			_, err := tx.Exec(ctx, stmt, d.Collector.Name, d.URL, int16(d.DumpType), time.Duration(d.Duration), d.Timestamp)
			if err != nil {
				logger.Error().Err(err).Str("collector", d.Collector.Name).Str("url", d.URL).Msg("Failed to execute upsert for BGP dump")
				tx.Rollback(ctx)
				return err
			}
		}

		err = tx.Commit(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to commit transaction for UpsertBGPDumps batch")
			return err
		}
		logger.Info().Int("batch_start", start).Int("batch_end", end).Msg("Transaction committed successfully for UpsertBGPDumps batch")
	}

	return nil
}

// FetchDataFromDB retrieves BGP dump data filtered by collector names and dump types.
func FetchDataFromDB(ctx context.Context, db *pgxpool.Pool, query Query) ([]BGPDump, error) {

	var args []interface{}
	paramCounter := 1
	sqlQuery := `
        SELECT url, dump_type, duration, collector_name, EXTRACT(EPOCH FROM timestamp)::bigint
        FROM bgp_dumps
        WHERE collector_name = ANY($1)
        AND timestamp + duration >= to_timestamp($2)
    ` // This ORDER BY may be bad for performance? But putting it there to match bgpstream ordering (which I think this is)

	// Extract collector names from the query
	collectorNames := make([]string, len(query.Collectors))
	for i, c := range query.Collectors {
		collectorNames[i] = c.Name
	}
	args = append(args, collectorNames, query.From.Unix())
	paramCounter = 3

	// Check if Until is 0, if not, add an end range
	if !query.Until.IsZero() {
		sqlQuery += ``
		sqlQuery += fmt.Sprintf(" AND timestamp <= to_timestamp($%d)", paramCounter)
		args = append(args, query.Until.Unix())
		paramCounter++
	}

	// Check if dump type was specified, if so query for just that type of file
	if query.DumpType != DumpTypeAny {
		sqlQuery += fmt.Sprintf(" AND dump_type = $%d", paramCounter)
		paramCounter++
		args = append(args, int16(query.DumpType))
	}

	if query.MinInitialTime != nil {
		//sqlQuery += fmt.Sprintf(" AND timestamp >= to_timestamp($%d) AND timestamp <= to_timestamp($%d)", paramCounter, paramCounter+1)
		//paramCounter += 2
		//args = append(args, query.MinInitialTime.Unix(), query.MinInitialTime.Add(-time.Duration(86400)*time.Second).Unix())
	}

	if query.DataAddedSince != nil {
		// TODO implement this (uncomment if correct, or fix if not)
		//sqlQuery += fmt.Sprintf(" AND cdate >= $%d", paramCounter)
		//paramCounter++
		//args = append(args, query.DataAddedSince)
	}

	sqlQuery += " ORDER BY timestamp ASC, dump_type ASC"

	rows, err := db.Query(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []BGPDump
	for rows.Next() {
		var (
			url           string
			dumpTypeInt   int16
			duration      time.Duration
			collectorName string
			timestamp     int64
		)

		err := rows.Scan(&url, &dumpTypeInt, &duration, &collectorName, &timestamp)
		if err != nil {
			return nil, err
		}

		results = append(results, BGPDump{
			URL:       url,
			DumpType:  DumpType(dumpTypeInt),
			Duration:  DumpDuration(duration),
			Collector: Collector{Name: collectorName},
			Timestamp: timestamp,
		})
	}

	return results, nil
}

type CollectorOldestLatestRecord struct {
	OldestRibsDump    string
	OldestUpdatesDump string
	LatestRibsDump    string
	LatestUpdatesDump string
}

func GetCollectorOldestLatest(ctx context.Context, db *pgxpool.Pool) (map[string]CollectorOldestLatestRecord, error) {
	sqlQuery := `
		WITH collectors AS (
		SELECT distinct (name) FROM collectors
		)
		SELECT
		c.name as collector_name,
		min_ribs.oldest_timestamp_ribs,
		max_ribs.latest_timestamp_ribs,
		min_updates.oldest_timestamp_updates,
		max_updates.latest_timestamp_updates

		FROM collectors c

		LEFT JOIN LATERAL (
		SELECT timestamp AS oldest_timestamp_ribs
		FROM bgp_dumps
		WHERE dump_type = 1 AND collector_name = c.name
		ORDER BY timestamp ASC
		LIMIT 1
		) min_ribs ON TRUE

		LEFT JOIN LATERAL (
		SELECT timestamp AS latest_timestamp_ribs
		FROM bgp_dumps
		WHERE dump_type = 1 AND collector_name = c.name
		ORDER BY timestamp DESC
		LIMIT 1
		) max_ribs ON TRUE

		LEFT JOIN LATERAL (
		SELECT timestamp AS oldest_timestamp_updates
		FROM bgp_dumps
		WHERE dump_type = 2 AND collector_name = c.name
		ORDER BY timestamp ASC
		LIMIT 1
		) min_updates ON TRUE

		LEFT JOIN LATERAL (
		SELECT timestamp AS latest_timestamp_updates
		FROM bgp_dumps
		WHERE dump_type = 2 AND collector_name = c.name
		ORDER BY timestamp DESC
		LIMIT 1
		) max_updates ON TRUE
		;
    `

	rows, err := db.Query(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make(map[string]CollectorOldestLatestRecord)
	for rows.Next() {
		var (
			collector                string
			oldest_timestamp_ribs    time.Time
			latest_timestamp_ribs    time.Time
			oldest_timestamp_updates time.Time
			latest_timestamp_updates time.Time
		)

		err := rows.Scan(&collector, &oldest_timestamp_ribs, &latest_timestamp_ribs, &oldest_timestamp_updates, &latest_timestamp_updates)
		if err != nil {
			return nil, err
		}
		results[collector] = CollectorOldestLatestRecord{
			OldestRibsDump:    strconv.FormatInt(oldest_timestamp_ribs.Unix(), 10),
			LatestRibsDump:    strconv.FormatInt(latest_timestamp_ribs.Unix(), 10),
			OldestUpdatesDump: strconv.FormatInt(oldest_timestamp_updates.Unix(), 10),
			LatestUpdatesDump: strconv.FormatInt(latest_timestamp_updates.Unix(), 10),
		}
	}

	return results, nil
}

func parseInterval(val interface{}) time.Duration {
	if val == nil {
		return 0
	}
	if s, ok := val.(string); ok {
		// Minimal parsing, improve as needed
		if strings.Contains(s, "hour") {
			return time.Hour
		}
		if strings.Contains(s, "minute") {
			return time.Minute
		}
	}
	return 0
}
