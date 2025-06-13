#!/usr/bin/env bash
source ./scripts/include.sh

before-start ndb_mgmd
(set -x
 $bin/ndb_mgmd --initial -f ./config_files/config.ini \
               --configdir=${RUN_DIR}/ndb_mgmd/config)
after-start ndb_mgmd
