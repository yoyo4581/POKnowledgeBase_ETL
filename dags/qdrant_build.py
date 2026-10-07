"""
Takes whatever the Colab notebook produced, proves it is good enough, and
swaps it into the Qdrant server behind an alias.

    incoming/*.zip  ->  unpack  ->  (build if needed)  ->  evaluate  ->
    restore into function_records__<version>  ->  flip the alias

The alias is the point. store.py's `restore --recreate` deletes the live
collection and then refills it, which leaves the MCP server answering out of
an empty or half-populated store for the length of the upsert. Nobody is
watching an automated run, so instead this restores into a versioned
collection nothing is reading yet, checks it, and then repoints the name the
application queries. A bad build never becomes the served build, and rolling
back is one more alias flip.

Triggered by the notebook's last cell over the Airflow REST API, or by hand.
Nothing schedules it: the artifacts it consumes are produced by a human with
a GPU, so there is no upstream asset that can honestly say "new model ready".

Prerequisites on the ETL host:
    pip install -r src/builders/Qdrant/requirements.txt
    (qdrant-client, fastembed and mcp are not currently in the venv)
    a reachable Qdrant server at QDRANT_URL

Orchestration only. Every task body lives in src/workflow/qdrant_flow.py --
see that module for why each step does what it does; this file is the order
they run in.
"""
import logging

from airflow.sdk import Asset, dag, task

from src.workflow import qdrant_flow as flow

logger = logging.getLogger(__name__)

QDRANT_COLLECTIONS_LIVE = Asset("qdrant://collections_live")


@dag(schedule=None, catchup=False, tags=["qdrant", "mcp", "docker"])
def qdrant_collection_build():

    @task()
    def ingest_artifacts() -> dict:
        return flow.ingest_artifacts()

    @task()
    def build_collections(ingested: dict) -> dict:
        return flow.build_collections(ingested)

    @task()
    def evaluate_gate(built: dict) -> dict:
        return flow.evaluate_gate(built)

    @task(outlets=[QDRANT_COLLECTIONS_LIVE], trigger_rule="none_failed")
    def restore_to_server(gate: dict) -> dict:
        return flow.restore_to_server(gate)

    @task()
    def prune_old_collections(restored: dict) -> None:
        flow.prune_old_collections(restored)

    @task(trigger_rule="none_failed")
    def snapshot_collections(restored: dict) -> dict:
        return flow.snapshot_collections(restored)

    @task()
    def publish_snapshots(snapshotted: dict) -> str:
        return flow.publish_snapshots(snapshotted)

    ingested = ingest_artifacts()
    built = build_collections(ingested)
    gate = evaluate_gate(built)
    restored = restore_to_server(gate)
    pruned = prune_old_collections(restored)
    # Serial, not parallel: pruning is the only step that frees disk and
    # snapshotting needs the most of it. See qdrant_flow.snapshot_collections.
    snapshotted = snapshot_collections(restored)
    pruned >> snapshotted
    publish_snapshots(snapshotted)


qdrant_collection_build()
