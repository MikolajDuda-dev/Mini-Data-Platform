#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${RED}======================================================${NC}"
echo -e "${RED}    STOPPING MLOPS PLATFORM                         ${NC}"
echo -e "${RED}======================================================${NC}"

# 1. Stopping containers
echo -e "${YELLOW}[1/2] Stopping Docker services...${NC}"
docker compose down

echo -e "${GREEN} Containers have been stopped.${NC}"
echo ""

# 2. Ask about deleting data 
echo -e "${YELLOW} DO YOU WANT TO DELETE ALL DATA? (Nuclear Option)${NC}"
echo "   (Choose 'y' if you want to clear the database, minio and start from scratch)"
echo "   (Choose 'n' if you just want to turn off the computer and come back later)"
read -p "   Delete volumes? [y/N]: " choice

if [[ "$choice" == "y" || "$choice" == "Y" ]]; then
    echo ""
    echo -e "${RED}[2/2] Deleting all data (Volumes)...${NC}"
    docker compose down -v
    docker compose down --remove-orphans
    echo -e "${GREEN} Everything cleared. Next start will be from scratch.${NC}"
else
    echo ""
    echo -e "${GREEN} Data saved. You can safely close the terminal.${NC}"
fi

echo -e "${RED}======================================================${NC}"