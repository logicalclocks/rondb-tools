#!/usr/bin/env bash
source ./scripts/include.sh

before-start rdrs2
(set -x
 $bin/rdrs2  -c ./config_files/rdrs_${NODEINFO_IDX}.json \
             > ${RUN_DIR}/rdrs/rdrs.log 2>&1 &)
after-start rdrs2
