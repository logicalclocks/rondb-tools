#!/usr/bin/env bash
source ./scripts/include.sh

# 1. check the role
case "$NODEINFO_ROLE" in
  ndb_mgmd|ndbmtd|mysqld|rdrs|prometheus|grafana|bench)
    echo "Deploying $NODEINFO_ROLE"
    ;;
  *)
    echo "Unknown role: $NODEINFO_ROLE"
    exit 1
    ;;
esac

# 2. Install RonDB
TARBALL_EXTRACTED_DIR=/tmp/${TARBALL_NAME%%.tar.gz}

if need_rondb; then
  rm -rf ${WORKSPACE}
  mkdir -p ${WORKSPACE}
  cd ${WORKSPACE}
  ln -s ${TARBALL_EXTRACTED_DIR} rondb
fi

sudo sysctl -w kernel.core_pattern=core.%e.%p

# Install prometheus exporter for OS metrics on all nodes
(set -x
 sudo apt-get install -yq prometheus-node-exporter)

# 4. Install services and create directories
case "$NODEINFO_ROLE" in
  ndb_mgmd)
    rm -rf ${RUN_DIR}
    mkdir -p ${RUN_DIR}/ndb_mgmd/data
    mkdir -p ${RUN_DIR}/ndb_mgmd/config
    ;;
  ndbmtd)
    rm -rf ${RUN_DIR}
    mkdir -p ${RUN_DIR}/ndbmtd/data
    mkdir -p ${RUN_DIR}/ndbmtd/ndb_data
    mkdir -p ${RUN_DIR}/ndbmtd/ndb_disk_columns
    ;;
  mysqld)
    rm -rf ${RUN_DIR}
    mkdir -p ${RUN_DIR}/mysqld/data
    (set -x
     sudo apt-get install -yq golang)
    cd ${WORKSPACE}
    git clone https://github.com/logicalclocks/mysqld_exporter.git
    cd mysqld_exporter
    (set -x
     git checkout -q origin/ndb)
    go build
    ;;
  rdrs)
    rm -rf ${RUN_DIR}
    mkdir -p ${RUN_DIR}/rdrs
    (set -x
     sudo apt-get install -yq libjsoncpp-dev)
    ;;
  prometheus)
    rm -rf ${RUN_DIR}
    mkdir -p ${RUN_DIR}/prometheus
    sudo systemctl mask prometheus
    (set -x
     sudo apt-get install -yq prometheus)
    ;;
  grafana)
    rm -rf ${RUN_DIR}
    mkdir -p ${RUN_DIR}/grafana ${RUN_DIR}/nginx
    sudo systemctl mask nginx
    (set -x
     sudo DEBIAN_FRONTEND=noninteractive \
          apt-get install -yq software-properties-common nginx)
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://packages.grafana.com/gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/grafana.gpg > /dev/null
    echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://packages.grafana.com/oss/deb stable main" | \
      sudo tee /etc/apt/sources.list.d/grafana.list
    (set -x
     sudo apt-get update -yq
     sudo apt-get install -yq grafana)
    ;;
  bench)
    rm -rf ${RUN_DIR}
    # Install python3 and python3-venv, needed for locust.
    # Install redis-tools, needed for valkey.
    # Install nginx, needed for automatic authentication.
    sudo systemctl mask nginx
    sudo DEBIAN_FRONTEND=noninteractive \
         apt-get install -y python3 python3-venv redis-tools nginx
    # Install python deps for locust and demo UI
    mkdir -p ${RUN_DIR}/locust ${RUN_DIR}/nginx ${RUN_DIR}/demo
    python3 -m venv ${RUN_DIR}/locust
    source ${RUN_DIR}/locust/bin/activate
    pip install -q --upgrade pip
    pip install -q locust
    deactivate
    python3 -m venv ${RUN_DIR}/demo
    source ${RUN_DIR}/locust/bin/activate
    pip install -q --upgrade pip
    pip install -q psutil fastapi uvicorn mysql-connector-python requests
    deactivate
    ;;
esac
