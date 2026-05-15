# Deployment Guide: BGPFinder Service

This guide explains how to deploy the BGPFinder stack (API service, periodic scraper, and Postgres database) using Docker Compose.

## Prerequisites
- Docker and Docker Compose installed on the host machine.
- Network access to RouteViews and RIS data archives.

## Step 1: Configure Environment
Copy the example environment file and customize it for your deployment:

```bash
cp example.env .env
```

Open `.env` and configure the following:
- `POSTGRES_PASSWORD`: Set a secure password for the database.
- `API_PORT`: The port you want the service to be accessible on (e.g., `80`).
- `DB_DATA_PATH`: The location on your host where database files will be stored (e.g., `./pgdata`).

## Step 2: Launch the Service
Run the following command to build and start the containers in detached mode:

```bash
docker-compose up -d --build
```

### What happens during startup?
1. **Database Initialization**: On the first run, Postgres will automatically execute all SQL scripts in the `migrations/` directory to create the schema and seed initial data.
2. **Health Checks**: The API and Scraper services will wait until the database is healthy before starting.
3. **Scraping**: The scraper will begin its first run according to the frequency configured in the application.

## Step 3: Verify Deployment
Check the status of the containers:

```bash
docker-compose ps
```

Test the API endpoint (assuming `API_PORT=8080`):

```bash
curl "http://localhost:8080/meta/projects?human=1"
```

## Management Commands

### Viewing Logs
To see logs from all services:
```bash
docker-compose logs -f
```

To see logs from just the scraper:
```bash
docker-compose logs -f scraper
```

### Stopping the Service
```bash
docker-compose down
```

### Updating
To update the service after code changes:
```bash
git pull
docker-compose up -d --build
```
