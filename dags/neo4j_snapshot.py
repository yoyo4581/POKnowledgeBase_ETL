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

(The container itself needs no setup: load_into_container brings it up with
`docker compose up -d`, which creates it on first run.)

Orchestration only. Both task bodies live in src/workflow/neo4j_flow.py.
"""
import logging

from airflow.sdk import Asset, dag, task

from src.workflow import neo4j_flow as flow

logger = logging.getLogger(__name__)

NEO4J_KG_COMPLETE = Asset("neo4j://kgml_complete")
NEO4J_CONTAINER_LIVE = Asset("neo4j://container_live")


@dag(schedule=[NEO4J_KG_COMPLETE], catchup=False, tags=["neo4j", "docker"])
def neo4j_snapshot():
    """
    Desktop -> dump -> container, after every successful graph build.

    The container is a serving replica, not a second source of truth. Nothing
    here rebuilds the graph: the ETL already did, and re-running it against the
    container would duplicate the work and drag SQL Server into the compose
    network for nothing.
    """

    @task()
    def dump_source() -> dict:
        return flow.dump_source()

    @task(outlets=[NEO4J_CONTAINER_LIVE])
    def load_into_container(source: dict) -> dict:
        return flow.load_into_container(source)

    load_into_container(dump_source())


neo4j_snapshot()
