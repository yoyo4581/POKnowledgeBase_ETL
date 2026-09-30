"""
Replicates the graph the ETL builds on Neo4j Desktop into the Docker container
that serves it, on every neo4j://kgml_complete.

    STOP DATABASE neo4j   (Bolt, against `system`)
    neo4j-admin dump      (a container, bind-mounting Desktop's store via /mnt/c)
    START DATABASE neo4j  (Bolt)
    load into the container volume, verify counts

Every step runs from WSL. Nothing invokes a Windows binary and nothing needed
changing on the Desktop side, which is worth spelling out because the two
obvious routes both do:

  online backup   `neo4j-admin database backup` runs against a LIVE server and
                  would need no stop at all -- but server.backup.listen_address
                  is 127.0.0.1:6362, so WSL cannot reach it. Opening it means a
                  neo4j.conf edit plus a Windows firewall rule. Enterprise-only
                  on the source, too, and the container is Community.
  neo4j-admin.bat Desktop's own CLI lives under a UUID'd Windows path and is a
                  .bat -- runnable from WSL through cmd.exe, but grim.

So instead: the stop/start is pure Cypher over Bolt (Enterprise supports
STOP DATABASE; Desktop ships Enterprise), and the dump runs inside a container
that bind-mounts Desktop's store directory through /mnt/c. neo4j-admin never
has to exist on this side.

The database goes briefly offline during the dump. That is unavoidable without
the backup port -- `neo4j-admin database dump` needs the store to itself -- and
it is why start_source_database runs under a finally, and why the DAG is
scheduled on an asset that fires after a build rather than on a clock.

Prerequisites, both one-off:
    NEO4J_DESKTOP_DATA   the live DBMS's data dir under /mnt/c. Find it with:
      ls -dt /mnt/c/Users/*/.Neo4jDesktop/relate-data/dbmss/dbms-*/data | head -1
    docker compose up -d neo4j
"""
from airflow.sdk import Asset, dag, task
from airflow.exceptions import AirflowSkipException

import logging
import os
import shutil
import subprocess
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
DUMPS_DIR = REPO_ROOT / "data" / "neo4j" / "dumps"

# Desktop's store, seen from WSL. No default: guessing a UUID would silently
# dump the wrong DBMS -- there are eight of them under that directory.
DESKTOP_DATA = os.getenv("NEO4J_DESKTOP_DATA", "")

# The image is only ever used for its neo4j-admin. Community reads the source's
# record-aligned-1.1 store fine; it is the same format both editions use.
NEO4J_IMAGE = os.getenv("NEO4J_IMAGE", "neo4j:5.19.0-community")
DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

# The container the graph is being replicated INTO, as the application sees it.
CONTAINER_URI = os.getenv("NEO4J_CONTAINER_URI", "bolt://localhost:7687")
CONTAINER_USER = os.getenv("NEO4J_CONTAINER_USER", "neo4j")
CONTAINER_PASSWORD = os.getenv("NEO4J_CONTAINER_PASSWORD", "")

# A replica that lost most of the graph is worse than a stale one, because
# nothing downstream can tell. Refuse to promote a load that does not match.
DRIFT_TOLERANCE = float(os.getenv("NEO4J_SNAPSHOT_TOLERANCE", "0.01"))

NEO4J_KG_COMPLETE = Asset("neo4j://kgml_complete")
NEO4J_CONTAINER_LIVE = Asset("neo4j://container_live")

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
    from neo4j import GraphDatabase
    if not CONTAINER_PASSWORD:
        raise RuntimeError(
            "NEO4J_CONTAINER_PASSWORD is not set. It must match the password baked "
            "in by NEO4J_AUTH on the container's FIRST start -- changing the compose "
            "value afterwards does nothing, because NEO4J_AUTH is ignored once "
            "/data holds a system database.")
    return GraphDatabase.driver(CONTAINER_URI, auth=(CONTAINER_USER, CONTAINER_PASSWORD))


def graph_size(driver, database: str | None = None) -> dict:
    from neo4j import RoutingControl
    out = {}
    for key, query in (("nodes", COUNTS), ("rels", REL_COUNTS)):
        records, _, _ = driver.execute_query(query, database_=database,
                                             routing_=RoutingControl.READ)
        out[key] = records[0][key]
    return out


@dag(
    schedule=[NEO4J_KG_COMPLETE],
    catchup=False,
    tags=["neo4j", "docker"],
)
def neo4j_snapshot():
    """
    Desktop -> dump -> container, after every successful graph build.

    The container is a serving replica, not a second source of truth. Nothing
    here rebuilds the graph: the ETL already did that, and re-running it against
    the container would duplicate the work and drag SQL Server into the compose
    network for nothing.
    """

    @task()
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
            run("docker", "run", "--rm",
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

    @task(outlets=[NEO4J_CONTAINER_LIVE])
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
            run("docker", "compose", "start", "neo4j")

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

    load_into_container(dump_source())


neo4j_snapshot()
