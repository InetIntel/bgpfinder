# --- Build Stage ---
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build API server
RUN go build -o /bgpfinder-server ./cmd/bgpfinder-server

# Build Scraper
RUN go build -o /scraper ./cmd/periodicscraper

# --- Run Stage ---
FROM alpine:latest

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /bgpfinder-server .
COPY --from=builder /scraper .

# Expose the default internal port
EXPOSE 8080

# Default command (can be overridden by docker-compose)
CMD ["./bgpfinder-server"]
