#!/bin/bash

BLUE='\033[0;34m'
NC='\033[0m'

cd ./deploy
echo -e "${BLUE}Starting up the Docker-Compose stack...${NC}"
docker-compose up -d