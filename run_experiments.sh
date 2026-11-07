#!/usr/bin/env bash
# Runs scenarios S1-S4 for baseline and proposed. Usage: ./run_experiments.sh
set -e
run() {  # name mode drop relaycrash noack crash
  echo; echo "##### $1 ($2) #####"
  export MODE=$2 DROP_RESPONSE_PROB=$3 RELAY_CRASH_PROB=$4 WORKER_NO_ACK_PROB=$5 CRASH_MIDWAY=$6
  docker compose up -d --build --force-recreate order-service-1 order-service-2 gateway relay payment-worker >/dev/null
  docker compose --profile tools run --rm loadtest
}
for mode in baseline proposed; do
  run S1 $mode 0 0 0 0
  run S2 $mode 0.3 0 0 0
  run S3 $mode 0.3 0.3 0.3 0
  run S4 $mode 0.3 0.3 0.3 1
done
docker compose down
