package bgpfinder

import (
	"context"
	"strings"
	"time"

	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UpsertCollectors inserts or updates collector records.
func UpsertCollectors(ctx context.Context, logger *logging.Logger, db *pgxpool.Pool, collectors []Collector) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to begin transaction for UpsertCollectors")
		return err
	}
	defer tx.Rollback(ctx)

	// Define the SQL query
	stmt := `
		INSERT INTO collectors (name, project_name, cdate, mdate, last_completed_crawl_time, most_recent_file_timestamp)
		VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, COALESCE((SELECT max(timestamp) FROM bgp_dumps WHERE collector_name = $3), CURRENT_TIMESTAMP))
		ON CONFLICT (name) DO UPDATE
		SET project_name = EXCLUDED.project_name,
			mdate = EXCLUDED.mdate,
			most_recent_file_timestamp = EXCLUDED.most_recent_file_timestamp,
			last_completed_crawl_time = EXCLUDED.last_completed_crawl_time
	`

	logger.Info().Int("collector_count", len(collectors)).Msg("Upserting collectors into DB")
	for _, c := range collectors {
		logger.Debug().Str("collector", c.Name).Str("project", c.Project.Name).Msg("Executing upsert for collector")
		collectorName := c.Project.Name

		ct, err := tx.Exec(ctx, stmt, c.Name, collectorName, collectorName)
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
			_, err := tx.Exec(ctx, stmt, d.Collector.Name, d.URL, int16(d.DumpType), d.Duration, d.Timestamp)
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
	sqlQuery := `
        SELECT url, dump_type, duration, collector_name, EXTRACT(EPOCH FROM timestamp)::bigint
        FROM bgp_dumps
        WHERE collector_name = ANY($1)
        AND timestamp >= to_timestamp($2)
        AND timestamp < to_timestamp($3)
    `
	if query.DumpType != DumpTypeAny {
		sqlQuery += " AND dump_type = $4"
	}

	// Extract collector names from the query
	collectorNames := make([]string, len(query.Collectors))
	for i, c := range query.Collectors {
		collectorNames[i] = c.Name
	}

	var args []interface{}
	args = append(args, collectorNames, query.From.Unix(), query.Until.Unix())
	if query.DumpType != DumpTypeAny {
		args = append(args, int16(query.DumpType))
	}

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
			dur           interface{}
			collectorName string
			timestamp     int64
		)

		err := rows.Scan(&url, &dumpTypeInt, &dur, &collectorName, &timestamp)
		if err != nil {
			return nil, err
		}

		durationVal := parseInterval(dur)
		results = append(results, BGPDump{
			URL:       url,
			DumpType:  DumpType(dumpTypeInt),
			Duration:  durationVal,
			Collector: Collector{Name: collectorName},
			Timestamp: timestamp,
		})
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
