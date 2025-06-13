#!/usr/bin/env bash
source ./scripts/include.sh

before-start ndbmtd
(set -x
 $bin/ndbmtd --initial --ndb-nodeid="${NODEINFO_NODEIDS}" \
             --ndb-connectstring=${NDB_MGMD_PRI}:1186)
after-start ndbmtd
