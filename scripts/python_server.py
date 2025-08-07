from fastapi import FastAPI, Request, BackgroundTasks, HTTPException, Query, Header
import re
import signal
import uuid
import subprocess
import os
import stat
import time
import datetime
import mysql.connector
import requests
import socket
from fastapi.responses import HTMLResponse, FileResponse, Response
import json
import asyncio
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Union, Optional, List
from enum import Enum
from contextlib import suppress, asynccontextmanager

@asynccontextmanager
async def lifespan(_: FastAPI):
    await startup()
    stop = asyncio.Event()
    maintenance_task = asyncio.create_task(maintenance(stop))
    try:
        yield
    finally:
        stop.set()
        await maintenance_task

app = FastAPI(lifespan=lifespan)

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
LOCUST_WORKER_COUNT=4
MAX_ACTIVE_DATABASES = 10
SESSION_TTL = 600 # max 10 min of database use per user
DEFAULT_GRAFANA_DASHBOARD = f"rdrs{os.environ['RDRS_MAJOR_VERSION']}_overview"

# Global state
class SessionStatus(Enum):
    NORMAL  = "NORMAL"
    CREATING_DATABASE = "creating_database"
    STARTING_LOCUST = "starting_locust"
class UserSession:
    def __init__(self,
                 status: SessionStatus = SessionStatus.NORMAL,
                 locust_port_offset: Optional[int] = None,
                 locust_pids: Optional[List[int]] = None,
                 db: Optional[str] = None,
                 expires_at: Optional[float] = None,
                 ):
        self.status = status
        self.lock = asyncio.Lock()
        self.locust_port_offset = locust_port_offset
        self.locust_pids = locust_pids
        self.db = db
        self.expires_at = expires_at or (time.time() + SESSION_TTL)
    def viewmodel(self) -> dict:
        """
        Return a view model of this session, suitable for the GUI.
        """
        return {
            "status": self.status.name,
            "locust_running": self.locust_pids is not None,
            "locust_workers": (
                0 if self.locust_pids is None else len(self.locust_pids)-1),
            "has_db": self.db is not None,
            "expires_at": self.expires_at,
            "default_grafana_dashboard": DEFAULT_GRAFANA_DASHBOARD,
        }
class AppState:
    def __init__(self):
        self.user_sessions: dict[str, UserSession] = {}
        self.next_locust_port_offset: int = 0
    async def set_next_locust_port_offset(self, new_value) -> None:
        self.next_locust_port_offset = new_value
        await change_persisted_state_atomically(lambda data: {
            **data,
            "next_locust_port_offset": self.next_locust_port_offset,
        })
    async def set_session(self, gui_secret, session) -> None:
        self.user_sessions[gui_secret] = session
        await change_persisted_state_atomically(lambda data: {
            **data,
            "user_sessions": {
                **data["user_sessions"],
                gui_secret: {
                    "status": session.status.name,
                    "locust_port_offset": session.locust_port_offset,
                    "locust_pids": session.locust_pids,
                    "db": session.db,
                    "expires_at": session.expires_at,
                }
            },
        })
    async def rm_session(self, gui_secret) -> None:
        if gui_secret in self.user_sessions:
            del self.user_sessions[gui_secret]
            await change_persisted_state_atomically(lambda data: {
                **data,
                "user_sessions": {
                    k: v for k, v in data["user_sessions"].items()
                    if k != gui_secret
                },
            })
state = AppState()
state_lock = asyncio.Lock()
state_file = RUN_DIR / "demo_state.json"
state_file_lock = asyncio.Lock() # protects state_file
async def change_persisted_state_atomically(func):
    data = {
        "user_sessions": {},
        "next_locust_port_offset": 0,
    }
    async with state_file_lock:
        if state_file.exists():
            data = json.loads(state_file.read_text())
        data = func(data)
        tmp = state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2))
        os.replace(tmp, state_file)

def validate_gui_secret(gui_secret: str) -> bool:
        return isinstance(gui_secret, str) and \
            re.fullmatch(r"[0-9a-f]{20}", gui_secret)

# Generate and set a secret X-AUTH cookie unless it already exists
class EnsureAuthCookieMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        gui_secret = request.cookies.get("X-AUTH")
        need_new_secret = not validate_gui_secret(gui_secret)
        if need_new_secret:
            gui_secret = os.urandom(10).hex()
        await state_lock.acquire()
        if gui_secret not in state.user_sessions:
            await state.set_session(gui_secret, UserSession())
        request.state.gui_secret = gui_secret
        request.state.session = state.user_sessions[gui_secret]
        # To avoid deadlocks, we always acquire first state_lock, then
        # state.user_sessions[gui_secret].lock. state_lock might be required in
        # some parts of endpoint logic, however holding both locks during
        # execution of all endpoint logic would eliminate concurrency. Here we
        # provide a way for the endpoint to release state_lock at any time, by
        # means of an idempotent closure. Calling this from endpoint logic is
        # optional.
        state_lock_released = False
        def release_state_lock_for_this_request():
            nonlocal state_lock_released
            if not state_lock_released:
                state_lock.release()
                state_lock_released = True
        request.state.release_state_lock = release_state_lock_for_this_request
        try:
            async with request.state.session.lock:
                response = await call_next(request)
        finally:
            release_state_lock_for_this_request()
        if need_new_secret:
            response.set_cookie(
                key = "X-AUTH",
                value = gui_secret,
                path = "/",
                # Hide the cookie from JavaScript and include it automatically
                # in every request
                httponly = True,
                samesite = "lax",
                secure = False,
            )
        return response
app.add_middleware(EnsureAuthCookieMiddleware)

def ts(): return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00","Z")
def log_error(msg, **other_keys): print(json.dumps({"ts": ts(), "type":"error", "msg":msg, **other_keys}))
def log_info(msg, **other_keys): print(json.dumps({"ts": ts(), "type":"info", "msg":msg, **other_keys}))

@app.get("/favicon.png")
async def favicon(request: Request):
    request.state.release_state_lock()
    return FileResponse("demo_static/favicon.png", media_type="image/png")

@app.get("/")
async def index(request: Request):
    request.state.release_state_lock()
    return FileResponse("demo_static/index.html", media_type="text/html")

async def startup():
    async with state_lock:
        if state_file.exists():
            async with state_file_lock:
                data = json.loads(state_file.read_text())
                state.next_locust_port_offset = data["next_locust_port_offset"]
                for gui_secret, session_json in data["user_sessions"].items():
                    session = UserSession(
                        status=SessionStatus[session_json["status"]],
                        locust_port_offset=session_json["locust_port_offset"],
                        locust_pids=session_json["locust_pids"],
                        db=session_json["db"],
                        expires_at=session_json["expires_at"],
                        )
            for gui_secret, session in state.user_sessions.items():
                with session.lock:
                    if session.status == SessionStatus.CREATING_DATABASE:
                        await drop_database(
                            session.db,
                            "Startup discovered creation in progress",
                            gui_secret)
                        session.status = SessionStatus.NORMAL
                        session.db = None
                        state.set_session(gui_secret, session)
                    elif session.status == SessionStatus.STARTING_LOCUST:
                        if session.locust_pids:
                            await kill_pids(session.locust_pids, gui_secret)
                            session.locust_pids = []
                        session.status = SessionStatus.NORMAL
                        state.set_session(gui_secret, session)
                    else:
                        assert session.status == SessionStatus.NORMAL, "Bug"
                    state.user_sessions[gui_secret] = session
        else:
            log_info("State file does not exist, starting with no sessions",
                     path=state_file)

@app.get("/viewmodel")
async def status(request: Request):
    request.state.release_state_lock()
    return request.state.session.viewmodel()

@app.post("/create-database")
async def create_database(request: Request,
                          background_tasks: BackgroundTasks,
                          response: Response):
    session = request.state.session
    if session.status == SessionStatus.CREATING_DATABASE:
        raise HTTPException(status_code=409, detail="Busy creating database.")
    if session.status == SessionStatus.STARTING_LOCUST:
        raise HTTPException(status_code=409, detail="Busy starting locust.")
    assert session.status == SessionStatus.NORMAL, "Bug"
    if session.db is not None:
        raise HTTPException(status_code=409,
                            detail="Database already created for this session.")
    active_dbs = sum(
        1 for s in state.user_sessions.values()
        if s.db is not None or s.status == SessionStatus.CREATING_DATABASE)
    if MAX_ACTIVE_DATABASES <= active_dbs:
        raise HTTPException(status_code=409,
                            detail="Maximum number of databases reached.")
    session.status = SessionStatus.CREATING_DATABASE
    session.db = f"db_{os.urandom(8).hex()}"
    gui_secret = request.state.gui_secret
    await state.set_session(gui_secret, session)
    request.state.release_state_lock()
    db_name = session.db
    async def create_db_in_background():
        success = False
        try:
            log_info("Creating database in background",
                     db_name=db_name,
                     session=gui_secret)
            await sql(
                f"CREATE DATABASE `{db_name}`",
                "USE benchmark",
                (
                    f"CALL generate_table_data("
                    f"'{db_name}',"             # database name
                    f"'bench_tbl',"             # table name
                    f"10,"                      # column count
                    f"100000,"                  # row count
                    f"1000,"                    # batch size
                    f"1)"                       # column_info
                ))
            success = True
        except Exception as e:
            log_error("Error creating database", db_name=db_name)
        async with state_lock:
            assert gui_secret in state.user_sessions, "Bug"
            session = state.user_sessions[gui_secret]
            async with session.lock:
                assert session.status == SessionStatus.CREATING_DATABASE, "Bug"
                session.status = SessionStatus.NORMAL
                if not success:
                    session.db = None
                await state.set_session(gui_secret, session)
        if success:
            # Update NGINX config and reload
            await update_nginx_config()
        else:
            await drop_database(db_name, "Creation failed", gui_secret)
    background_tasks.add_task(create_db_in_background)
    # Return the same data as /viewmodel
    return session.viewmodel()

@app.post("/run-locust")
async def run_locust(request: Request):
    session = request.state.session
    if session.locust_port_offset is None:
        # Pick a free port offset
        # Hopefully holding state_lock is enough to read .locust_port_offset
        # from all sessions, since we only write it here under the same lock.
        used = {s.locust_port_offset for s in state.user_sessions.values()}
        next_offset = state.next_locust_port_offset
        while next_offset in used:
            next_offset = (next_offset + 1) % 10_000
        session.locust_port_offset = next_offset
        next_offset = (next_offset + 1) % 10000
        await state.set_next_locust_port_offset(next_offset)
        await state.set_session(gui_secret, session)
    request.state.release_state_lock()
    locust_master_port = 33000 + session.locust_port_offset
    locust_http_port = 44000 + session.locust_port_offset
    if session.status == SessionStatus.CREATING_DATABASE:
        raise HTTPException(status_code=409, detail="Busy creating database.")
    if session.status == SessionStatus.STARTING_LOCUST:
        raise HTTPException(status_code=409, detail="Busy starting locust.")
    assert session.status == SessionStatus.NORMAL, "Bug"
    if session.locust_pids is not None:
        raise HTTPException(status_code=409,
                            detail="Locust already running")
    session.status = SessionStatus.STARTING_LOCUST
    gui_secret = request.state.gui_secret
    await state.set_session(gui_secret, session)
    db_name = session.db
    try:
        await sql(f"USE {db_name}")
    except:
        raise HTTPException(status_code=404, detail="Database not found")

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
        f"{RUN_DIR}/locust-{gui_secret}-master.log",
        f"{RUN_DIR}/locust-{gui_secret}-master.err",
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
    log_info("Started locust master", pid=master_pid, session=gui_secret)
    session.locust_pids = [master_pid]
    await state.set_session(gui_secret, session)
    time.sleep(1)

    # Start workers
    worker_pids = []
    for i in range(LOCUST_WORKER_COUNT):
        worker_pid = daemon(
            f"{RUN_DIR}/locust-{gui_secret}-worker-{i}.log",
            f"{RUN_DIR}/locust-{gui_secret}-worker-{i}.err",
            "locust",
            "-f", "/home/ubuntu/scripts/locust_batch_read.py",
            "--worker",
            "--master-port", str(locust_master_port),
        )
        log_info("Started locust worker", worker_idx=i, pid=worker_pid, session=gui_secret)
        session.locust_pids = [*session.locust_pids, worker_pid]
        await state.set_session(gui_secret, session)

    session.status = SessionStatus.NORMAL
    await state.set_session(gui_secret, session)

    return session.viewmodel()

# WARNING: Keep this in sync with nginx-dynamic.conf generation in ../cluster_ctl
async def update_nginx_config():
    # We have two types of GUI secrets to take into account here. There is
    # GUI_SECRET which is created by ../cluster_ctl from a command line. This
    # secret is typically used by the same person that created the cluster,
    # should map to a hard coded port and never expire. The secrets in
    # user_sessions belong to anonymous users of the demo UI.
    async with state_lock:
        content = [
            # Map GUI secret to validity
             'map $gui_secret $secret_is_valid {',
            f'    "{GUI_SECRET}" 1;',
            *[f'    "{gui_secret}" 1;'
              for gui_secret in state.user_sessions],
             '    default 0;',
             '}',
            # Map GUI secret to locust http port. The cluster secret is mapped to
            # 8089, which is the default for locust --master-bind-port. Unknown
            # secrets map to 0.
             'map $gui_secret $locust_http_port {',
            f'    "{GUI_SECRET}" 8089;',
            *[f'    "{gui_secret}" {44000 + session.locust_port_offset};'
              for gui_secret, session in state.user_sessions.items()
              # Could be false for renamed sessions in process of deletion
              if validate_gui_secret(gui_secret)
              # Only allow users with active database
              and session.db is not None
              ],
             '    default 0;',
             '}',
        ]
    async with state_file_lock:
        Path(f"{CONFIG_FILES}/nginx-dynamic.conf") \
            .write_text("\n".join(content) + "\n")
        # Attempt to trigger nginx to reload config.
        subprocess.run(
            ["nginx",
             "-s", "reload",
             "-c", f"{CONFIG_FILES}/nginx.conf",
             # We provide the error log path using the -e option rather than via
             # configuration, since otherwise we get a spurious warning (see
             # https://stackoverflow.com/questions/34258894)
             "-e", os.environ['NGINX_ERROR_LOG']],
            check=True)

async def maintenance(stop: asyncio.Event):
    while not stop.is_set():
        until = time.time()
        sessions_to_remove = []
        need_nginx_reconfig = False
        async with state_lock:
            for gui_secret, session in state.user_sessions.copy().items():
                async with session.lock:
                    if session.status != SessionStatus.NORMAL:
                        continue
                    if until < session.expires_at:
                        continue
                    # Change session name, so that the user can create a new
                    # session right away.
                    rm_name = f"{gui_secret}_removing_{os.urandom(3).hex()}"
                    state.set_session(rm_name, session)
                    state.rm_session(gui_secret)
                    sessions_to_remove.append(rm_name)
                    log_info("Starting cleanup", session=gui_secret, session_renamed_to=rm_name)
                    need_nginx_reconfig = True
        if need_nginx_reconfig:
            await update_nginx_config()
        for session_name in sessions_to_remove:
            async with state_lock:
                assert session_name in state.user_sessions, "Bug"
                session = state.user_sessions[session_name]
                await session.lock.acquire()
            if session.db:
                await drop_database(session.db, "Session cleanup", session_name)
            if session.locust_pids:
                await kill_pids(session.locust_pids, session_name)
            async with state_lock:
                state.rm_session(session_name)
            log_info("Cleanup done", session=session_name)
        await asyncio.sleep(10)

async def drop_database(db_name: str, reason, session) -> None:
    log_info("Dropping database",
             db_name=db_name,
             reason=reason,
             session=session)
    await sql(f"DROP DATABASE IF EXISTS `{db_name}`")
    log_info("Dropping database done", db_name=db_name, session=session)

async def sql(*statements):
    def blocking_db_work():
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        try:
            for statement in statements:
                cursor.execute(statement)
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    await asyncio.to_thread(blocking_db_work)

async def kill_pids(pids: List[int], session_name: str) -> None:
    await asyncio.gather(*(kill_pid(pid, session_name) for pid in pids))

async def kill_pid(pid: int, session_name: str) -> None:
    log_info("Terminating process", pid=pid, session=session_name)
    do_kill = False
    term_count = 0
    kill_count = 0
    while True:
        if term_count == 20:
            do_kill = True
        try:
            os.kill(pid, signal.SIGKILL if do_kill else signal.SIGTERM)
        except ProcessLookupError:
            if term_count == 0:
                log_info("Process already gone", pid=pid, session=session_name)
            elif kill_count > 0:
                log_info("Process exited after SIGKILL",
                         pid=pid,
                         session=session_name,
                         sigterm_count=term_count,
                         sigkill_count=kill_count)
            else:
                log_info("Process exited after SIGTERM",
                         pid=pid,
                         session=session_name,
                         sigterm_count=term_count)
            return
        if do_kill:
            kill_count += 1
        else:
            term_count += 1
        if kill_count == 1:
            log_info("Process hasn't terminated, using SIGKILL from now on",
                     pid=pid,
                     session=session_name,
                     sigterm_count=term_count)
        if kill_count == 100:
            log_info("Process did not exit despite many SIGKILLs, giving up",
                     pid=pid,
                     session=session_name,
                     sigterm_count=term_count,
                     sigkill_count=kill_count)
            return
        await asyncio.sleep(1)
