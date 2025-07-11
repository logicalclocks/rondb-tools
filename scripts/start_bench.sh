#!/usr/bin/env bash
source ./scripts/include.sh
before-start nginx
(set -x
 nginx -c ${CONFIG_FILES}/nginx.conf 2>/dev/null
)
after-start nginx
