#!/usr/bin/env bash
source ./scripts/include.sh

before-start prometheus
(set -x
 prometheus --config.file="${CONFIG_FILES}/prometheus.yml" \
            --storage.tsdb.path="${RUN_DIR}/prometheus/data" \
            > "${RUN_DIR}/prometheus.log" 2>&1 &)
after-start prometheus
