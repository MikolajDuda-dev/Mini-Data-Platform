#!/bin/bash

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}    MINI DATA PLATFORM & MLOPS: AUTO-LAUNCHER       ${NC}"
echo -e "${BLUE}======================================================${NC}"

# 1. Start Databases  
echo -e "${YELLOW}[1/6] Starting Databases (Postgres + MinIO)...${NC}"
docker compose up -d postgres-airflow postgres-db minio 2>/dev/null || docker compose up -d

# 2. WAIT LOOP 
echo -e "${YELLOW}[2/6] Waiting for Postgres readiness...${NC}"
RETRIES=30
while [ $RETRIES -gt 0 ]; do
    if docker exec postgres-airflow pg_isready -U airflow > /dev/null 2>&1; then
        echo -e "${GREEN}    Postgres is ready!${NC}"
        break
    else
        echo "    Database is starting up... ($RETRIES retries left)"
        sleep 2
        RETRIES=$((RETRIES-1))
    fi
done

if [ $RETRIES -eq 0 ]; then
    echo -e "${RED} Error: Postgres did not start in time. Check logs.${NC}"
    exit 1
fi




echo -e "${YELLOW}[2.5/6] Initializing MinIO Buckets...${NC}"
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/bronze local/silver local/gold local/mlflow || true

# 3. Initialize Airflow DB 
echo -e "${YELLOW}[3/6] Initializing Airflow Database...${NC}"
docker compose run --rm airflow-webserver airflow db init

echo "   Creating Admin user..."
docker compose run --rm airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com 2>/dev/null || true

# 4. Start the rest of the services
echo -e "${YELLOW}[4/6] Starting remaining services (Webserver, Scheduler, Spark...)${NC}"
docker compose up -d

echo "Waiting 15 seconds for Airflow Scheduler to fully load..."
sleep 15

# 5. DAG Operations
echo -e "${YELLOW}[5/6] Triggering Pipeline...${NC}"
sleep 10
docker exec airflow-scheduler airflow dags unpause 3_mlops_full_pipeline
docker exec airflow-scheduler airflow dags trigger 3_mlops_full_pipeline
if [ $? -eq 0 ]; then
    echo -e "${GREEN}   DAG triggered successfully!${NC}"
else
    echo -e "${RED}   Failed to trigger DAG. You may need to do it manually in the UI.${NC}"
fi
# 6. Final information
echo -e "${BLUE}======================================================${NC}"
echo -e "${GREEN} SUCCESS! Pipeline has been started in the background.${NC}"
echo -e "${BLUE}======================================================${NC}"
echo ""
echo "What is happening now?"
echo "1. Data generation (Postgres)"
echo "2. ETL Processing (Spark Bronze -> Silver -> Gold)"
echo "3. Data validation (Great Expectations)"
echo "4. Model training and promotion (MLflow)"
echo "5. API Restart (Deployment)"
echo ""
echo " Track progress here: http://localhost:8085 (Login: admin/admin)"
echo ""
echo -e "${YELLOW} In about 2-3 minutes, when DAG in Airflow is green, type:${NC}"
echo "   python test_inference.py"
echo ""