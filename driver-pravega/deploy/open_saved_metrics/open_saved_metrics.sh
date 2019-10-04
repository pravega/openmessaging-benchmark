#!/usr/bin/env bash
set -e

: ${1?"Usage: $0 PRAVEGA_LOGS_DIRECTORY"}

pravega_logs_dir=$(readlink -f "$1")
echo Loading metrics from: ${pravega_logs_dir}
export INFLUXDB_BACKUP_DIR="${pravega_logs_dir}/influxdb"
export PROMETHEUS_DATA_DIR="${pravega_logs_dir}/prometheus"
docker-compose -p open_saved_metrics -f $(dirname $0)/docker-compose.yml rm --stop --force influxdb

#
# InfluxDB
#

influxdb_tgz="${pravega_logs_dir}/influxdb.tgz"
ls -lh ${influxdb_tgz}
rm -rf "${INFLUXDB_BACKUP_DIR}"
mkdir "${INFLUXDB_BACKUP_DIR}"
tar -xzvf "${influxdb_tgz}" -C "${INFLUXDB_BACKUP_DIR}"
docker-compose -p open_saved_metrics -f $(dirname $0)/docker-compose.yml up -d influxdb
docker-compose -p open_saved_metrics -f $(dirname $0)/docker-compose.yml exec influxdb influxd restore -portable /tmp/backup

#
# Prometheus
#

prometheus_tgz="${pravega_logs_dir}/prometheus.tgz"
ls -lh ${prometheus_tgz}
rm -rf "${PROMETHEUS_DATA_DIR}"
mkdir "${PROMETHEUS_DATA_DIR}"
tar -xzvf "${prometheus_tgz}" --strip-components=3 -C "${PROMETHEUS_DATA_DIR}" opt/prometheus/data
chmod -R a+rwX "${PROMETHEUS_DATA_DIR}"
docker-compose -p open_saved_metrics -f $(dirname $0)/docker-compose.yml up -d prometheus

#
# Grafana
#

docker-compose -p open_saved_metrics -f $(dirname $0)/docker-compose.yml up -d

echo
echo Grafana URL: http://localhost:3000
echo
