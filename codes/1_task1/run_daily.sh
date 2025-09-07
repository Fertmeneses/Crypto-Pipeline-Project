#!/bin/bash
set -euo pipefail

# --- ABSOLUTE PATHS (edit these to match your machine) ---
ROOT="YourPath/codes/1_task1"
ENV_FILE="YourPath/.env"            
DATA_DIR="$ROOT/crypto_datafiles"
DOCKER="/usr/bin/docker"                                   # run: `which docker` to confirm
IMAGE="api_request:latest"                                 # make sure this name/tag exists

# Make sure the output folder exists
mkdir -p "$DATA_DIR"

# Use yesterday’s date
DATE_STR="$(date -d 'yesterday' '+%Y-%m-%d')"
USER_IDS="$(id -u):$(id -g)"                      # compute once

for COIN in bitcoin ethereum cardano; do
  "$DOCKER" run --rm \
    --env-file "$ENV_FILE" \
    -u "$USER_IDS" \
    -v "$DATA_DIR:/app/crypto_datafiles" \
    "$IMAGE" "$COIN" "$DATE_STR"
done

