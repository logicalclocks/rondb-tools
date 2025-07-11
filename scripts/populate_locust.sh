#!/usr/bin/env bash
source ./scripts/include.sh

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 COLUMNS ROWS BATCH_SIZE COLUMN_INFO"
  exit 1
fi

COLUMNS=$1
ROWS=$2
BATCH_SIZE=$3
COLUMN_INFO=$4

# Populate
VALUES="$((ROWS * COLUMNS))"
echo "Populating benchmark database with $ROWS rows × $COLUMNS columns = $VALUES values,"
echo "in batches of $BATCH_SIZE rows."
if [ "$VALUES" -gt 1000000 ]; then
  echo "This might take a while."
fi
(set -x
 $mysql -e "USE benchmark; CALL generate_table_data('benchmark', 'bench_tbl', $COLUMNS, $ROWS, $BATCH_SIZE, $COLUMN_INFO)")
