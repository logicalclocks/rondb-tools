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
TARBALL=${TARBALL_NAME%%.tar.gz}

if need_rondb; then
  rm -rf ${WORKSPACE}
  mkdir -p ${WORKSPACE}
  cd ${WORKSPACE}
  tar xzf /tmp/${TARBALL_NAME}
  ln -s ${TARBALL} rondb
fi

# Install prometheus exporter for OS metrics on all nodes
sudo apt-get install -y prometheus-node-exporter

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
    sudo apt-get install -y golang
    cd ${WORKSPACE}
    git clone https://github.com/logicalclocks/mysqld_exporter.git
    cd mysqld_exporter
    git checkout origin/ndb
    go build
    ;;
  rdrs)
    rm -rf ${RUN_DIR}
    mkdir -p ${RUN_DIR}/rdrs
    sudo apt-get install -y libjsoncpp-dev
    ;;
  prometheus)
    rm -rf ${RUN_DIR}
    mkdir -p ${RUN_DIR}/prometheus
    sudo systemctl mask prometheus
    sudo apt-get install -y prometheus
    ;;
  grafana)
    rm -rf ${RUN_DIR}
    mkdir -p ${RUN_DIR}/grafana ${RUN_DIR}/nginx
    sudo systemctl mask nginx
    sudo DEBIAN_FRONTEND=noninteractive \
         apt-get install -y software-properties-common nginx
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://packages.grafana.com/gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/grafana.gpg > /dev/null
    echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://packages.grafana.com/oss/deb stable main" | \
      sudo tee /etc/apt/sources.list.d/grafana.list
    sudo apt-get update -y
    sudo apt-get install -y grafana
    ;;
  bench)
    rm -rf ${RUN_DIR}
    # Install python3 and python3-venv, needed for locust.
    # Install redis-tools, needed for valkey.
    # Install nginx, needed for automatic authentication.
    sudo systemctl mask nginx
    sudo DEBIAN_FRONTEND=noninteractive \
         apt-get install -y python3 python3-venv redis-tools nginx
    # Install locust
    mkdir -p ${RUN_DIR}/locust ${RUN_DIR}/nginx
    python3 -m venv ${RUN_DIR}/locust
    source ${RUN_DIR}/locust/bin/activate
    pip install -q --upgrade pip
    pip install -q locust
    ;;
esac
