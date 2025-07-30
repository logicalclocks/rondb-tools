#!/usr/bin/env bash
source ./scripts/include.sh

# Only operate on the first bench node
if [ "$NODEINFO_IDX" != 0 ]; then exit; fi

nginx_error_log="${RUN_DIR}/nginx/nginx_error.log"
before-start nginx
(set -x
 # On a successful start, nginx will daemonize by itself and doesn't need the
 # `&` to run in the background. However, if there is a problem it will exit
 # with a non-zero exit code, causing the script to exit immediately. In order
 # to give `after-start` a chance to report the reason, we add the `&` to
 # suppress the exit-on-failure.
 #
 # We provide the error log path using the -e option rather than via
 # configuration, since otherwise we get a spurious warning (see
 # https://stackoverflow.com/questions/34258894)
 nginx -c "${CONFIG_FILES}/nginx.conf" -e "${nginx_error_log}" &
)
after-start nginx "$nginx_error_log"
