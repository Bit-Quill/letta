#!/bin/bash
# Run docker-compose with Valkey (default)

export CACHE_IMAGE="valkey/valkey:alpine"
export CACHE_CONTAINER_NAME="valkey"
export CACHE_HEALTHCHECK="['CMD-SHELL', 'valkey-cli ping | grep PONG']"
export CACHE_COMMAND="valkey-server --appendonly yes"
export CACHE_DATA_DIR="valkey"

echo "Starting services with Valkey..."
docker compose -f scripts/docker-compose.yml "$@"
