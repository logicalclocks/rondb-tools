set -euo pipefail
ulimit -c unlimited

source ./config_files/shell_vars
source ./config_files/nodeinfo

bin="${WORKSPACE}/rondb/bin"
mysql="$bin/mysql -uroot"

possible_procs="ndb_mgmd ndbmtd mysqld mysqld_exporter rdrs rdrs2 prometheus grafana grafana-server sysbench locust valkey nginx uvicorn"

is-running() {
  # linux process names are max 15 characters.
  pgrep -x -- "${1:0:15}" >/dev/null
}

before-start() {
  if is-running "$1"; then
    echo "Cannot start $1 because it's already running"
    exit 1
  fi
}

after-start() {
  sleep 2
  # Technically is-running could return true if with-restarts is in an infinite
  # failure loop, but it's unlikely. OK for now.
  if ! is-running "$1"; then
    echo "Failed to start $1"
    if [[ -n "$2" ]]; then
      cat "$2"
    fi
    exit 1
  fi
}

with-restarts() {
  local ec
  local tag
  tag="$1"
  shift
  while true; do
    set +e
    "$@"
    ec="$?"
    set -e
    if [[ -e "$RUN_DIR/suppress-restarts-for-$tag" ]]
    then return "$ec"
    else sleep 1
    fi
  done
}

stop() {
  local ec
  touch "$RUN_DIR/suppress-restarts-for-$1"
  sleep 1.5 # Break any fast-fail infinite loop (is-running won't detect a
            # with-restarts that is sleeping)
  if ! is-running "$1"
  then rm -f "$RUN_DIR/suppress-restarts-for-$1"
       return 0
  fi
  pkill -TERM -x -- "$1"
  sleep .5
  local -i WAITCOUNT=0
  while is-running "$1"; do
    if (( WAITCOUNT == 12 )); then
      echo "$1 is still running 60 seconds after asked to stop. Sending SIGKILL."
      if pkill -KILL -x -- "$1"; then
        # At least one process was sent a signal, as expected
        :
      else
        ec="$?"
        if is-running "$1" || [[ "$ec" -ne 1 ]]; then
          echo "pkill -KILL $1 failed with code $ec"
          rm -f "$RUN_DIR/suppress-restarts-for-$1"
          exit "$ec"
        else
          # pkill failed with code 1 due to no process found, that's ok
          :
        fi
      fi
    fi
    echo "Waiting for $1 to stop..."
    sleep 5
    WAITCOUNT=$((WAITCOUNT + 1))
  done
  rm -f "$RUN_DIR/suppress-restarts-for-$1"
}

need_rondb() {
  case $NODEINFO_ROLE in
    ndb_mgmd|ndbmtd|mysqld|rdrs|bench) return 0 ;;
    *) return 1 ;;
  esac
}
