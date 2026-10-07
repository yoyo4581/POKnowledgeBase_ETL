"""
Desktop -> dump -> container, the logic behind dags/neo4j_snapshot.py.

Split out for the same reason qdrant_flow.py is: Airflow re-parses everything
under dags/ on a timer, and a DAG file should say what runs in what order
rather than how.
"""
import logging
import os
import subprocess
import time
from pathlib import Path

from airflow.sdk.exceptions import AirflowSkipException
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# parents[2]: this module is at src/workflow/, two levels down -- it was
# [1] when these bodies lived in dags/.
REPO_ROOT = Path(__file__).resolve().parents[2]
DUMPS_DIR = REPO_ROOT / "data" / "neo4j" / "dumps"

# Desktop's store, seen from WSL. No default: guessing a UUID would silently
# dump the wrong DBMS -- there are eight of them under that directory.
DESKTOP_DATA = os.getenv("NEO4J_DESKTOP_DATA", "")

# The image is only ever used for its neo4j-admin. Community reads the source's
# record-aligned-1.1 store fine; it is the same format both editions use.
NEO4J_IMAGE = os.getenv("NEO4J_IMAGE", "neo4j:5.19.0-community")
DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

# The container the graph is being replicated INTO, as the application sees it.
# Defaults to the plain port, which is what a machine without Neo4j Desktop
# sees. On a host where Desktop owns 7687 -- this one, where NEO4J_URI points
# at it and the ETL writes to it -- set NEO4J_BOLT_PORT and a matching
# NEO4J_CONTAINER_URI in .env. Two different servers, two different
# credentials; nothing requires them to agree.
CONTAINER_URI = os.getenv("NEO4J_CONTAINER_URI", "bolt://localhost:7687")
CONTAINER_USER = os.getenv("NEO4J_CONTAINER_USER", "neo4j")
CONTAINER_PASSWORD = os.getenv("NEO4J_CONTAINER_PASSWORD", "")

# A replica that lost most of the graph is worse than a stale one, because
# nothing downstream can tell. Refuse to promote a load that does not match.
DRIFT_TOLERANCE = float(os.getenv("NEO4J_SNAPSHOT_TOLERANCE", "0.01"))


COUNTS = "MATCH (n) RETURN count(n) AS nodes"
REL_COUNTS = "MATCH ()-[r]->() RETURN count(r) AS rels"

def run(*cmd: str, timeout: int = 1800) -> str:
    """Shell out, log the command, surface stderr on failure."""
    logger.info("$ %s", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True, timeout=timeout)
    for line in proc.stdout.splitlines():
        logger.info("  %s", line)
    if proc.returncode != 0:
        for line in proc.stderr.splitlines():
            logger.error("  %s", line)
        raise RuntimeError(f"{cmd[0]} {cmd[1] if len(cmd) > 1 else ''} exited {proc.returncode}")
    return proc.stdout


def source_driver():
    """Desktop, via NEO4J_URI -- the same instance the rest of the ETL writes to."""
    from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
    return Neo4j_ETL().driver


def container_driver():
    """
    The serving replica. Unrelated to source_driver(): different server,
    different credentials, and nothing requires the two to agree.

    An empty NEO4J_CONTAINER_PASSWORD means auth is disabled on the container
    (NEO4J_AUTH: none in docker-compose.yml), not that configuration is
    missing -- so connect unauthenticated rather than refusing. That is only
    sane because the container's ports are bound to 127.0.0.1; if you ever
    publish them more widely, set NEO4J_AUTH back to neo4j/<password> on a
    fresh volume and put the same value here.

    If a password IS set it must match what NEO4J_AUTH baked in on the
    container's FIRST start -- changing the compose value afterwards does
    nothing, because NEO4J_AUTH is ignored once /data holds a system database.
    """
    from neo4j import GraphDatabase
    auth = (CONTAINER_USER, CONTAINER_PASSWORD) if CONTAINER_PASSWORD else None
    return GraphDatabase.driver(CONTAINER_URI, auth=auth)


def graph_size(driver, database: str | None = None) -> dict:
    from neo4j import RoutingControl
    out = {}
    for key, query in (("nodes", COUNTS), ("rels", REL_COUNTS)):
        records, _, _ = driver.execute_query(query, database_=database,
                                             routing_=RoutingControl.READ)
        out[key] = records[0][key]
    return out


def dump_source() -> dict:
    """
    Take the source database offline just long enough to dump it.

    STOP DATABASE is a Cypher statement against `system`, so it needs no CLI
    and no filesystem access -- which is the whole reason this runs from WSL
    at all. The restart is in a finally: leaving a developer's database
    stopped because a dump failed would be a much worse outcome than a
    missed snapshot.
    """
    if not DESKTOP_DATA:
        raise AirflowSkipException(
            "NEO4J_DESKTOP_DATA is unset, so there is no store to dump. Find it "
            "with: ls -dt /mnt/c/Users/*/.Neo4jDesktop/relate-data/dbmss/dbms-*/data "
            "| head -1   (pick by recency -- there are several old DBMSs there.)")
    data_dir = Path(DESKTOP_DATA)
    if not (data_dir / "databases" / DATABASE).is_dir():
        raise RuntimeError(
            f"{data_dir}/databases/{DATABASE} does not exist. NEO4J_DESKTOP_DATA "
            f"is pointing at the wrong DBMS -- Desktop keeps every one you have "
            f"ever created under relate-data/dbmss/.")

    driver = source_driver()
    before = graph_size(driver)
    logger.info("Source graph: %(nodes)s nodes, %(rels)s relationships", before)

    DUMPS_DIR.mkdir(parents=True, exist_ok=True)
    started = time.time()
    driver.execute_query(f"STOP DATABASE {DATABASE} WAIT", database_="system")
    logger.info("Source database stopped; dumping.")
    try:
        # --user is not optional. The image starts as root but its
        # entrypoint drops to the neo4j user (uid 7474) before running
        # neo4j-admin, and DUMPS_DIR is an ordinary host directory owned
        # by whoever cloned the repo -- so the dump dies with
        # "AccessDeniedException: /dumps" after taking the database
        # offline. Running as the host user makes /dumps writable and
        # leaves the dump owned by us, which is also what lets it be read
        # and shipped afterwards. Reading Desktop's store is unaffected:
        # it is this user's own files on /mnt/c.
        run("docker", "run", "--rm",
            "--user", f"{os.getuid()}:{os.getgid()}",
            "-v", f"{data_dir}:/data",
            "-v", f"{DUMPS_DIR}:/dumps",
            NEO4J_IMAGE,
            "neo4j-admin", "database", "dump", DATABASE,
            "--to-path=/dumps", "--overwrite-destination=true")
    finally:
        driver.execute_query(f"START DATABASE {DATABASE} WAIT", database_="system")
        logger.info("Source database restarted after %.0fs offline.",
                    time.time() - started)

    dump = DUMPS_DIR / f"{DATABASE}.dump"
    if not dump.exists():
        raise RuntimeError(f"{dump} was not produced despite a clean exit.")
    logger.info("Wrote %s (%.0f MB)", dump, dump.stat().st_size / 1e6)

    driver.close()
    return {"dump": str(dump), **before}


def load_into_container(source: dict) -> dict:
    """
    Stop the container, import the dump into its volume, start it again.

    The import writes into the neo4j_data named volume, so it survives every
    later stop/start -- this task is what makes the container's graph
    persistent, and it is exactly why the loader is profile-gated in
    docker-compose.yml rather than wired to depends_on. A plain
    `docker compose up` must never re-import.
    """
    run("docker", "compose", "stop", "neo4j")
    try:
        run("docker", "compose", "--profile", "load", "run", "--rm", "neo4j-load")
    finally:
        # `up -d`, not `start`. `docker compose start` only starts a
        # container that already EXISTS, and exits 0 when none does -- so
        # on a host that has never run `up`, the whole task succeeds while
        # leaving no server at all, and the verification below then fails
        # with a bare "connection refused" that looks like a port problem.
        # `up -d` creates it if missing, starts it if stopped, and
        # recreates it when the compose config has changed since.
        run("docker", "compose", "up", "-d", "neo4j")

    # The server needs a moment before Bolt answers; the compose healthcheck
    # has a 40s start_period for the same reason.
    deadline = time.time() + 180
    last = None
    while time.time() < deadline:
        try:
            driver = container_driver()
            after = graph_size(driver)
            driver.close()
            break
        except Exception as e:                      # noqa: BLE001 - retrying on purpose
            last = e
            time.sleep(5)
    else:
        raise RuntimeError(f"Container did not accept Bolt within 180s: {last}")

    logger.info("Container graph: %(nodes)s nodes, %(rels)s relationships", after)
    for key in ("nodes", "rels"):
        src, dst = source[key], after[key]
        drift = abs(src - dst) / src if src else 0.0
        if drift > DRIFT_TOLERANCE:
            raise RuntimeError(
                f"{key}: source had {src:,}, container has {dst:,} "
                f"({drift:.1%} off, tolerance {DRIFT_TOLERANCE:.1%}). The load did "
                f"not reproduce the graph -- do not point the application at this "
                f"container until it does.")
        logger.info("  %s match: %s vs %s", key, f"{src:,}", f"{dst:,}")

    return {"source": source, "container": after}
