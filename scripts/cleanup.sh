#!/usr/bin/env bash
source ./scripts/include.sh

#1. Stop servers
for proc in \
  ndb_mgmd \
  ndbmtd \
  mysqld \
  mysqld_exporter \
  rdrs2 \
  prometheus \
  grafana \
  grafana-server \
  nginx \
  sysbench \
  locust \
  valkey \
  ;
do stop $proc; done

#2. cleanup directories
rm -rf ${RUN_DIR}/ndb_mgmd/data/*
rm -rf ${RUN_DIR}/ndb_mgmd/config/*
rm -rf ${RUN_DIR}/ndbmtd/data/*
rm -rf ${RUN_DIR}/ndbmtd/ndb_data/*
rm -rf ${RUN_DIR}/ndbmtd/ndb_disk_columns/*
rm -rf ${RUN_DIR}/mysqld/data/*
rm -rf ${RUN_DIR}/rdrs/*
rm -rf ${RUN_DIR}/prometheus/*
rm -rf ${RUN_DIR}/grafana/*
rm -rf ${RUN_DIR}/nginx/*
rm -rf /home/${USER}/uploads
