#!/bin/bash

BLUE='\033[0;34m'
NC='\033[0m'

cd ./deploy
echo -e "${BLUE}Shutting down the Docker-Compose stack...${NC}"
docker compose down -v # -v removes volumes, as the 'docker compose up -d' commands creates new volumes every time it's ran