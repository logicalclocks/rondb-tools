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
SENTINEL_TABLE="benchmark.bench_tbl_sentinel"
PARAM_STRING="$COLUMNS,$ROWS,$BATCH_SIZE,$COLUMN_INFO"

# Ensure sentinel table exists
$mysql -e "CREATE TABLE IF NOT EXISTS $SENTINEL_TABLE (params TEXT) ENGINE=NDB CHARACTER SET latin1;"

# Check existing params
existing=$($mysql -Nse "SELECT params FROM $SENTINEL_TABLE LIMIT 1;")
if [ "$existing" == "$PARAM_STRING" ]; then
  echo "Population already done with matching parameters. Skipping."
  exit 0
fi

$mysql -e "DELETE FROM $SENTINEL_TABLE;"

# Populate
VALUES="$((ROWS * COLUMNS))"
echo "Populating benchmark database with $ROWS rows × $COLUMNS columns = $VALUES values,"
echo "in batches of $BATCH_SIZE rows."
if [ "$VALUES" -gt 1000000 ]; then
  echo "This might take a while."
fi
(set -x
 $mysql -e "USE benchmark; CALL generate_table_data('bench_tbl', $COLUMNS, $ROWS, $BATCH_SIZE, $COLUMN_INFO)")

$mysql -e "INSERT INTO $SENTINEL_TABLE (params) VALUES ('$PARAM_STRING');"
