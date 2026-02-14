#!/bin/bash

set -ex

EXEC_TIMESTAMP="$(date +%d/%m/%Y/%H/%M/%S)"

INPUT_DIR="/input"
OUTPUT_DIR="/output/${EXEC_TIMESTAMP}"
WORKER_NODES="1"
METRIC="sum"
LINESPLIT_NUM="10000"
REDUCERS_NUM="1"

get_help() {
	echo "Usage: $0 [-e <env>] [-d]"
	exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -i|--input)
      INPUT_DIR="$2"
      shift
      shift
      ;;
	  -o|--output)
      OUTPUT_DIR="$2"
      shift
      shift
      ;;
    -w|--workers)
      WORKER_NODES="$2"
      shift
      shift
      ;;
    -m|--metric)
      METRIC="$2"
      shift
      shift
      ;;
    -l|--linesplit)
      LINESPLIT_NUM="$2"
      shift
      shift
      ;;
    -r|--reducers)
      REDUCERS_NUM="$2"
      shift
      shift
      ;;
    -h|--help)
      get_help
      ;;
    *)
      echo "Invalid option: $1" >&2
      exit 1
      ;;
  esac
done

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

echo "Running MapReduce job"
docker exec namenode bash -c "hadoop jar /jars/${JAR_NAME} ${INPUT_DIR} ${OUTPUT_DIR} ${METRIC}" \
	-Dmapred.reduce.tasks=${REDUCERS_NUM} -Dmapreduce.input.lineinputformat.linespermap=${LINESPLIT_NUM}

mkdir -p ./results
echo "Job completed. Results:"
docker exec namenode hadoop fs -cat "${OUTPUT_DIR}/*" | tail -n +2 > ./results/results.txt
