#!/usr/bin/env bash
source ./scripts/include.sh

source ${RUN_DIR}/venv/bin/activate
before-start uvicorn
cd scripts
(set -x
 uvicorn python_server:app --host 0.0.0.0 --port 8000 > "${RUN_DIR}/demo/uvicorn.log" 2> "${RUN_DIR}/demo/uvicorn.err" &)
after-start uvicorn "${RUN_DIR}/demo/uvicorn.err"
