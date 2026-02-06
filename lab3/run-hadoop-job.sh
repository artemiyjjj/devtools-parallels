#!/bin/bash

set -ex

EXEC_TIMESTAMP="$(date +%d/%m/%Y/%H/%M/%S)"

export WORKER_NODES="${1:-1}"
METRIC="${2:-sum}"
INPUT_DIR="${3:-/input}"
OUTPUT_DIR="${4:-/output/${EXEC_TIMESTAMP}}"

VERSION=1.0.0
JAR_NAME="lab3-hadoop-${VERSION}-all.jar"

echo "Build jar"
docker build -t sales-analysis-hadoop:latest .
container_id=$(docker create sales-analysis-hadoop:latest)
mkdir -p ./jars
docker cp "$container_id":/app/build/libs/${JAR_NAME} ./jars/

echo "Starting Hadoop cluster"
docker compose up -d

sleep 5 # Wait until every cluster node is up (TODO health-check)
echo "Starting Hadoop MapReduce job"
docker exec namenode hadoop fs -mkdir -p "${INPUT_DIR}"
# docker exec namenode hadoop fs -mkdir -p "${OUTPUT_DIR}"

# Avoid hadoop fs locking problem
IS_PUT=false
while [ "$IS_PUT" == "false" ]; do
	docker exec namenode bash -c "hadoop fs -put -f /data/*.csv /input/" && IS_PUT=true || sleep 3
done

echo "Running MapReduce job..."
docker exec namenode bash -c "hadoop jar /jars/${JAR_NAME} ${INPUT_DIR} ${OUTPUT_DIR} ${METRIC}"

mkdir -p ./results
echo "Job completed. Results:"
docker exec namenode hadoop fs -cat "${OUTPUT_DIR}/*" | tail -n +2 > ./results/results.txt
