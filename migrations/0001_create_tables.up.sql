CREATE TABLE IF NOT EXISTS collectors (
    collector_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    project_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS bgp_dumps (
    bgp_dump_id SERIAL PRIMARY KEY,
    collector_name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL,
    dump_type SMALLINT NOT NULL,
    duration INTERVAL,
    timestamp TIMESTAMP NOT NULL,
    CONSTRAINT unique_bgp_dump UNIQUE (collector_name, url)
);