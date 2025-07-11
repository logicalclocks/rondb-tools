#!/usr/bin/env bash
source ./scripts/include.sh

# Start mysqld
before-start mysqld
(set -x
 $bin/mysqld --defaults-file=./config_files/my.cnf --initialize-insecure
 $bin/mysqld --defaults-file=./config_files/my.cnf &)
after-start mysqld

# Operations to run on the first node only
if [ $NODEINFO_IDX -eq 0 ]; then
  # Wait for mysqld to start
  WAITED=0
  MAX_WAIT=100
  while true; do
    if $bin/mysqladmin ping -uroot --silent 2>/dev/null; then
      break
    fi
    if [ $WAITED -ge $MAX_WAIT ]; then
      echo "Timeout waiting"
      exit 1
    fi
    echo "Waiting for MySQL to start..."
    sleep 5
    WAITED=$((WAITED + 1))
  done
  # Start prometheus mysqld exporter
  before-start mysqld_exporter
  export DATA_SOURCE_NAME='root:@tcp(127.0.0.1:3306)/'
  (set -x
   ${WORKSPACE}/mysqld_exporter/mysqld_exporter --no-collect.slave_status \
               > "${RUN_DIR}/mysqld_exporter.log" 2>&1 &)
  after-start mysqld_exporter
  # Init database
  echo "Creating the procedure for creating rdrs benchmark table" \
       "on MySQL ${MYSQLD_PUB_1}"
  $mysql -e "source ./scripts/benchmark_load.sql"
  echo "Creating the rondis tables on MySQL ${MYSQLD_PUB_1}"
  $mysql -e "source ./scripts/create_rondis_tables.sql"
  $mysql -e "use benchmark;call CreateRondisTables(2)"
  $mysql -e "DROP USER IF EXISTS 'db_create_user'@'$BENCH_PRI_1';"
  $mysql -e "CREATE USER 'db_create_user'@'$BENCH_PRI_1' IDENTIFIED BY '$DEMO_MYSQL_PW';"
  $mysql -e "GRANT ALL PRIVILEGES ON *.* TO 'db_create_user'@'$BENCH_PRI_1';"
fi
