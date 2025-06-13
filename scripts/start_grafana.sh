#!/usr/bin/env bash
source ./scripts/include.sh
before-start grafana
(set -x
 grafana-server --homepath /usr/share/grafana \
                --config ./config_files/grafana/grafana.ini \
                > "${RUN_DIR}/grafana/grafana.out" 2>&1 &)
after-start grafana
before-start nginx
(set -x
 nginx -c ${CONFIG_FILES}/nginx_grafana.conf
)
after-start nginx
