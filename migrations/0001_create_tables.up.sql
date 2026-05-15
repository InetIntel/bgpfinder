CREATE TABLE IF NOT EXISTS collectors (
    name VARCHAR(255) UNIQUE NOT NULL,
    project_name VARCHAR(255) NOT NULL,
    cdate TIMESTAMP NOT NULL DEFAULT NOW(),
    mdate TIMESTAMP NOT NULL DEFAULT NOW(),
    most_recent_file_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    last_completed_crawl_time_ribs TIMESTAMP NOT NULL DEFAULT NOW(),
    last_completed_crawl_time_updates TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bgp_dumps (
    collector_name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL,
    dump_type SMALLINT NOT NULL,
    duration INTERVAL,
    timestamp TIMESTAMP NOT NULL,
    cdate TIMESTAMP NOT NULL DEFAULT NOW(),
    mdate TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_bgp_dump UNIQUE (collector_name, url)
);

CREATE TABLE IF NOT EXISTS collector_aliases (
    project TEXT NOT NULL,
    alias TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    PRIMARY KEY (project, alias)
);

INSERT INTO collector_aliases (project, alias, canonical_name) VALUES
('routeviews', 'route-views2.saopaulo', 'ix-br2.gru'),
('routeviews', 'route-views.saopaulo', 'ix-br.gru'),
('routeviews', 'route-views.amsix', 'locix.fra')
ON CONFLICT DO NOTHING;


CREATE INDEX bgp_dumps_timestamp_idx ON public.bgp_dumps USING btree ("timestamp");

CREATE INDEX bgp_dumps_dump_type_idx ON public.bgp_dumps USING btree (dump_type);

CREATE INDEX bgp_dumps_collector_name_idx ON public.bgp_dumps USING btree (collector_name);

CREATE INDEX idx_bgp_dumps_type_collector_timestamp ON public.bgp_dumps USING btree (dump_type, collector_name, "timestamp");

CREATE INDEX idx_bgp_collector_ts_duration ON public.bgp_dumps USING btree (collector_name, "timestamp", (("timestamp" + duration)));

CREATE INDEX idx_bgp_dumps_distinct_dump_col_ts ON public.bgp_dumps USING btree (dump_type, collector_name, "timestamp" DESC);
