#!/usr/bin/env bash
source ./scripts/include.sh

if [ "$RDRS_MAJOR_VERSION" == 1 ]; then
  RDRS_BIN=rdrs
elif [ "$RDRS_MAJOR_VERSION" == 2 ]; then
  RDRS_BIN=rdrs2
else
  exit 1
fi

before-start $RDRS_BIN
(set -x
 $bin/$RDRS_BIN --config "./config_files/rdrs_${NODEINFO_IDX}.json" \
             > "${RUN_DIR}/rdrs/rdrs.log" 2>&1 &)
after-start $RDRS_BIN "${RUN_DIR}/rdrs/rdrs.log"
