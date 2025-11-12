#!/bin/bash
# Run docker-compose with Redis

export CACHE_IMAGE="redis:alpine"
export CACHE_CONTAINER_NAME="redis"
export CACHE_HEALTHCHECK="['CMD-SHELL', 'redis-cli ping | grep PONG']"
export CACHE_COMMAND="redis-server --appendonly yes"
export CACHE_DATA_DIR="redis"

echo "Starting services with Redis..."
docker compose -f scripts/docker-compose.yml "$@"
