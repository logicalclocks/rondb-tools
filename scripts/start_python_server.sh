#!/usr/bin/env bash
source ./scripts/include.sh

python3 -m venv ${RUN_DIR}/demo-venv
source ${RUN_DIR}/locust/bin/activate
before-start uvicorn
cd scripts
(set -x
 uvicorn python_server:app --host 0.0.0.0 --port 8000 > ${RUN_DIR}/demo.log 2> ${RUN_DIR}/demo.err &)
after-start uvicorn
