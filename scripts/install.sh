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

# Some things we only need to do once
first_install() { [ ! -f /tmp/rondb-tools-install-done ]; }
apt_install() {
  if first_install; then
    (set -x
     sudo DEBIAN_FRONTEND=noninteractive apt-get update -yq
     sudo DEBIAN_FRONTEND=noninteractive apt-get install -yq "$@")
  fi
}

# Install prometheus exporter for OS metrics on all nodes
apt_install prometheus-node-exporter

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
    apt_install golang
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
    apt_install libjsoncpp-dev
    ;;
  prometheus)
    rm -rf ${RUN_DIR}
    mkdir -p ${RUN_DIR}/prometheus
    if first_install; then
      sudo systemctl mask prometheus
      apt_install prometheus
    fi
    ;;
  grafana)
    rm -rf ${RUN_DIR}
    mkdir -p ${RUN_DIR}/grafana
    if first_install; then
      apt_install software-properties-common
      sudo mkdir -p /etc/apt/keyrings
      curl -fsSL https://packages.grafana.com/gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/grafana.gpg > /dev/null
      echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://packages.grafana.com/oss/deb stable main" | \
        sudo tee /etc/apt/sources.list.d/grafana.list
      apt_install grafana
    fi
    ;;
  bench)
    rm -rf ${RUN_DIR}
    if first_install; then
      # Install python3 and python3-venv, needed for locust.
      # Install redis-tools, needed for valkey.
      # Install nginx dependencies
      apt_install -yq python3 python3-venv redis-tools curl gnupg2 \
                  ca-certificates lsb-release ubuntu-keyring
      # Install nginx. We need a newer version than Ubuntu provides in order to
      # support the `-e` option.
      sudo systemctl mask nginx
      curl https://nginx.org/keys/nginx_signing.key | gpg --dearmor |
        sudo tee /usr/share/keyrings/nginx-archive-keyring.gpg >/dev/null
      echo "deb [signed-by=/usr/share/keyrings/nginx-archive-keyring.gpg] http://nginx.org/packages/ubuntu $(lsb_release -cs) nginx" |
        sudo tee /etc/apt/sources.list.d/nginx.list
      echo -e "Package: *\nPin: origin nginx.org\nPin: release o=nginx\nPin-Priority: 900\n" |
        sudo tee /etc/apt/preferences.d/99nginx
      apt_install nginx
    fi
    # Install python deps for locust and demo UI
    mkdir -p ${RUN_DIR}/locust ${RUN_DIR}/nginx ${RUN_DIR}/demo ${DURABLE_DIR}
    python3 -m venv ${RUN_DIR}/venv
    source ${RUN_DIR}/venv/bin/activate
    pip install -q --upgrade pip
    pip install -q locust psutil fastapi uvicorn mysql-connector-python requests
    deactivate
    ;;
esac

# Mark install as successful
touch /tmp/rondb-tools-install-done
