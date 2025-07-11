from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Header
import re
import signal
import psutil
import uuid
import subprocess
import os
import stat
import time
import mysql.connector
import requests
import socket
from threading import Lock
from fastapi.responses import HTMLResponse, FileResponse, Response
from itertools import chain
import json

app = FastAPI()

from pathlib import Path
RUN_DIR = Path(os.environ['RUN_DIR'])
CONFIG_FILES = Path(os.environ['CONFIG_FILES'])
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
MYSQL_HOST=os.environ['MYSQLD_PRI_1']
MYSQL_PASSWORD=os.environ['DEMO_MYSQL_PW']
GRAFANA_HOST=os.environ['GRAFANA_PRI_1']
GUI_SECRET=os.environ['GUI_SECRET']
GRAFANA_URL = f"http://{GRAFANA_HOST}:3000"
MYSQL_CONFIG = {
    "host": MYSQL_HOST,
    "user": "db_create_user",
    "password": MYSQL_PASSWORD
}
MAX_WORKER_COUNT=4
MAX_ACTIVE_DATABASES = 10
SESSION_TTL = 600 # wait 10 min

user_sessions = {}  # gui_secret → {"db", "expires_at", "locust_master_port", "locust_http_port", "locust_pids"}
session_lock = Lock()
session_lock.acquire() # Make sure startup() gets to access it first

def save_user_sessions():
    user_sessions_file = RUN_DIR / "demo_user_sessions.json"
    with open(user_sessions_file, 'w') as f:
        f.write(json.dumps(user_sessions, indent=4))

@app.get("/favicon.png")
async def favicon():
    return FileResponse("demo_static/favicon.png", media_type="image/png")

@app.get("/")
async def index():
    return FileResponse("demo_static/index.html", media_type="text/html")

def validate_gui_secret(secret: str) -> bool:
    return bool(re.fullmatch(r"[a-f0-9]{20}", secret))

@app.on_event("startup")
def startup():
    global user_sessions
    # Read user_sessions from RUN_DIR/demo_user_sessions.json
    user_sessions_file = RUN_DIR / "demo_user_sessions.json"
    if user_sessions_file.exists():
        try:
            with open(user_sessions_file, 'r') as f:
                user_sessions = json.loads(f.read())
        except json.JSONDecodeError:
            print(f"Error reading {user_sessions_file}, starting with empty sessions")
            user_sessions = {}
    else:
        print(f"{user_sessions_file} does not exist, starting with empty sessions")
        user_sessions = {}
    session_lock.release() # Lock was acquired at file load
    cleanup()

# todo don't create if already created
@app.post("/create-database")
async def create_database(response: Response, background_tasks: BackgroundTasks):
    with session_lock:
        active_dbs = len(user_sessions)
        if active_dbs >= MAX_ACTIVE_DATABASES:
            raise HTTPException(status_code=429, detail="Maximum number of active databases reached")

        gui_secret = os.urandom(10).hex()
        db_name = f"db_{os.urandom(8).hex()}"
        # get both http and master ports
        occupied_locust_ports = set(chain(
            *[(session["locust_http_port"], session["locust_master_port"])
              for session in user_sessions.values()]))
        locust_master_port = 33000
        while locust_master_port in occupied_locust_ports:
            locust_master_port += 1
        locust_http_port = 44000
        while locust_http_port in occupied_locust_ports:
            locust_http_port += 1
        user_sessions[gui_secret] = {
            "db": db_name,
            "expires_at": time.time() + SESSION_TTL,
            "locust_master_port": locust_master_port,
            "locust_http_port": locust_http_port,
            "locust_pids": [],
        }
        save_user_sessions()

    # 1. Create DB + table
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE `{db_name}`")
    cursor.execute("USE benchmark")
    call_sql = (
        f"CALL generate_table_data("
        f"'{db_name}',"             # database name
        f"'bench_tbl',"             # table name
        f"10,"                      # column count
        f"100000,"                  # row count
        f"1000,"                    # batch size
        f"1)"                       # column_info
    )
    cursor.execute(call_sql)
    conn.commit()
    cursor.close()
    conn.close()

    # 2. Update NGINX config and reload
    update_nginx_config()

    # 3. Schedule background cleanup
    background_tasks.add_task(wait_and_cleanup)

    # 4. Send gui_secret in response
    return {"message": "Database created", "gui_secret": gui_secret}

@app.post("/run-locust")
async def run_locust(
    x_auth: str = Header(None),
    worker_count: int = Query(0, ge=1, le=MAX_WORKER_COUNT)
):
    with session_lock:
        if not x_auth or not validate_gui_secret(x_auth) or x_auth not in user_sessions:
            raise HTTPException(status_code=403, detail="Invalid or expired session")

        session = user_sessions[x_auth]
        db_name = session["db"]
        locust_master_port = session["locust_master_port"]
        locust_http_port = session["locust_http_port"]
        # Prevent double-start
        if session["locust_pids"] == True:
            return {"message": "Locust already starting"}
        if session["locust_pids"]:
            return {"message": "Locust already running"}
        session["locust_pids"] = True

    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute(f"USE {db_name}")
    except:
        raise HTTPException(status_code=404, detail="Database not found")
    finally:
        cursor.close()
        conn.close()

    def daemon(outpath, errpath, *cmd):
        with open(outpath, "w") as out, open(errpath, "w") as err:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=out,
                stderr=err,
                start_new_session=True,
                close_fds=True)
            return proc.pid

    # Start master
    master_pid = daemon(
        f"{RUN_DIR}/locust-{x_auth}-master.log",
        f"{RUN_DIR}/locust-{x_auth}-master.err",
        "locust",
        "-f", f"{SCRIPTS_DIR}/locust_batch_read.py",
        "--host", os.environ['RDRS_URI'],
        "--batch-size=100",
        "--table-size=100000",
        f"--database-name={db_name}",
        "--master-bind-port", str(locust_master_port),
        "--web-port", str(locust_http_port),
        "--master",
    )
    time.sleep(2)

    # Start workers
    worker_pids = []
    for i in range(worker_count):
        worker_pid = daemon(
            f"{RUN_DIR}/locust-{x_auth}-worker-{i}.log",
            f"{RUN_DIR}/locust-{x_auth}-worker-{i}.err",
            "locust",
            "-f", "/home/ubuntu/scripts/locust_batch_read.py",
            "--worker",
            "--master-port", str(locust_master_port),
        )
        worker_pids.append(worker_pid)

    # Save PIDs
    with session_lock:
        session['locust_pids'] = [master_pid] + worker_pids
        save_user_sessions()

    return {"message": f"Distributed Locust UI started with {worker_count} workers"}

def w(name, *content):
    Path(name).write_text("\n".join(content) + "\n")

# WARNING: Keep this in sync with nginx-dynamic.conf generation in ../cluster_ctl
def update_nginx_config():
    # We have two types of GUI secrets to take into account here. There is
    # GUI_SECRET which is created by ../cluster_ctl from a command line. This
    # secret is typically used by the same person that created the cluster,
    # should map to a hard coded port and never expire. The secrets in
    # user_sessions belong to anonymous users of the demo UI.
    w(f"{CONFIG_FILES}/nginx-dynamic.conf",
      # Map GUI secret to validity
       'map $gui_secret $secret_is_valid {',
      f'    "{GUI_SECRET}" 1;',
      *[f'    "{gui_secret}" 1;'
        for gui_secret in user_sessions],
       '    default 0;',
       '}',
      # Map GUI secret to locust http port. The cluster secret is mapped to
      # 8089, which is the default for locust --master-bind-port. Unknown
      # secrets map to 0.
       'map $gui_secret $locust_http_port {',
      f'    "{GUI_SECRET}" 8089;',
      *[f'    "{gui_secret}" {session["locust_http_port"]};'
        for gui_secret, session in user_sessions.items()],
       '    default 0;',
       '}',
      )
    # Attempt to trigger nginx to reload config.
    subprocess.run(
        ["nginx", "-s", "reload", "-c", f"{CONFIG_FILES}/nginx.conf"],
        check=True)

def wait_and_cleanup():
    time.sleep(SESSION_TTL + 0.1)
    cleanup()

def cleanup():
    until = time.time()
    do_update_nginx_config = False
    with session_lock:
        for gui_secret, session in user_sessions.copy().items():
            if until < session["expires_at"]:
                continue
            for pid in session["locust_pids"]:
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            # Drop DB
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()
            cursor.execute(f"DROP DATABASE IF EXISTS `{session['db']}`")
            conn.commit()
            cursor.close()
            conn.close()
            # Remove session object
            user_sessions.pop(gui_secret)
            do_update_nginx_config = True
    # Remove NGINX config + reload
    if do_update_nginx_config:
        update_nginx_config()
