#!/bin/bash

BLUE='\033[0;34m'
NC='\033[0m'

cd ./deploy
docker network create backend

echo -e "${BLUE}Created 'backend' docker network${NC}"