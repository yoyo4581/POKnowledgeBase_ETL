"""
Qdrant collection build: ingest, gate, promote, snapshot, publish.

The logic behind dags/qdrant_build.py. Split out for the same reason
reactome_flow.py is: a DAG file should say what runs in what order, and
Airflow re-parses everything under dags/ on a timer, so bodies that never
change do not belong there.

Raises AirflowSkipException directly for the "nothing to do" cases -- an
unchanged artifact, an already-published release. Airflow is a hard
dependency of this repo and nothing outside dags/ imports this module, so
routing that through a private exception bought indirection and no
portability. The standalone tools that DO run without Airflow
(build/store.py, mcp_server.py, analysis/evaluate.py) live under
src/builders/ and import nothing from here.
"""
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

from airflow.sdk.exceptions import AirflowSkipException
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)



# parents[2], not [1]: this module sits at src/workflow/, two levels down.
# It was [1] when these bodies lived in dags/, and the move silently
# rebased every path onto src/ -- QDRANT_SRC then pointed at a directory
# that does not exist, so `from core.config import ...` failed with a
# ModuleNotFoundError that looks nothing like a path bug.
REPO_ROOT = Path(__file__).resolve().parents[2]
QDRANT_SRC = REPO_ROOT / "src" / "builders" / "Qdrant"
DATASET_DIR = REPO_ROOT / "data" / "qdrant" / "go_contrastive"

# Where artifacts land on their way in from Drive. See ingest_artifacts().
INCOMING_DIR = Path(os.getenv("QDRANT_INCOMING_DIR", REPO_ROOT / "data" / "qdrant" / "incoming"))
ARCHIVE_DIR = INCOMING_DIR / "consumed"

EXPORT_DIR = REPO_ROOT / "data" / "qdrant" / "qdrant_export"
STORE_DIR = REPO_ROOT / "data" / "qdrant" / "qdrant_store"
EVAL_DIR = REPO_ROOT / "data" / "qdrant" / "eval_results"
# Shipping format. Bind-mounted into the qdrant container at /snapshots via
# QDRANT__STORAGE__SNAPSHOTS_PATH, so the server writes here directly and a
# consumer's `docker compose --profile load run --rm qdrant-load` reads here.
SNAPSHOT_DIR = REPO_ROOT / "data" / "qdrant" / "snapshots"
MODEL_DIR = Path(os.getenv("BIOBERT_MODEL_PATH",
                           REPO_ROOT / "EmbeddingModel" / "biobert-go-retrieval"))

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")

# These MUST match the notebook's WINDOW/STRIDE. store.py's own argparse defaults
# are 3/2, but at window=3 the median 3-sentence record collapses into a single
# chunk identical to itself (55% of the corpus), so the shipped export is built at
# 2/1. They are only consulted on a LOCAL rebuild; an ingested export carries its
# own values in the manifest, and restore_to_server warns if the two disagree.
WINDOW = int(os.getenv("QDRANT_WINDOW", "2"))
STRIDE = int(os.getenv("QDRANT_STRIDE", "1"))
# Re-encode even when usable collections are already on disk.
FORCE_REBUILD = os.getenv("QDRANT_FORCE_REBUILD", "").lower() in ("1", "true", "yes")
# Move previously-consumed archives back into incoming/ and ingest them again.
REINGEST = os.getenv("QDRANT_REINGEST", "").lower() in ("1", "true", "yes")
BATCH_SIZE = int(os.getenv("QDRANT_BATCH_SIZE", "64"))

# record_hybrid is the default search mode, so it is the arm whose quality
# actually reaches the application.
GATE_METRIC = os.getenv("QDRANT_GATE_METRIC", "ndcg@10")
# This gate is a TRIPWIRE, not a measurement. Retrieval quality was already
# measured on the full held-out set during fine-tuning, on a GPU, and recorded
# in the training log. What is left to establish here is narrower and cruder:
# that the artifact which actually arrived is not gimped. The failure modes it
# exists to catch --
#     the encoder does not match the collections it is querying (the README is
#       explicit that nothing detects this; results just go quietly poor),
#     a partially populated or corrupted export,
#     the wrong model zip dropped in incoming/,
# -- are not 0.02 regressions. They land the score at roughly raw BioBERT
# (ndcg@10 ~= 0.05) against a working model's ~0.23 dense-only, higher again
# once BM25 joins it under record_hybrid. That is a fivefold gap.
#
# So the floor sits far below the expected value rather than just under it: low
# enough that sampling noise can never trip it, high enough that nothing broken
# survives. Calibrate from your first green run, but keep the margin wide --
# a gate that cries wolf gets disabled, and then it is guarding nothing.
GATE_FLOOR = float(os.getenv("QDRANT_GATE_FLOOR", "0.15"))

# The gate reads exactly one number out of evaluate.py: summary[GATE_ARM][ndcg@10].
# evaluate.py's defaults are the EXPERIMENT's -- both arms and a 100-deep ranking
# so the paired bootstrap has something to chew on -- and both are wasted here.
#   --variants <arm>  runs record_hybrid only. chunk_rerank stays the default in
#                     evaluate.py for interactive use; it is simply not something
#                     this gate reads, so it is not something this gate pays for.
#                     (evaluate.py already sends snippets_per_record=0, so with a
#                     single record-level arm the chunk collection is never
#                     queried at all.)
#   --top-k 10        ndcg@10 only looks at the top 10, and top_k also sizes the
#                     MatchAny accession filter any chunk work would run.
GATE_ARM = os.getenv("QDRANT_GATE_ARM", "record_hybrid")
GATE_TOP_K = int(os.getenv("QDRANT_GATE_TOP_K", "10"))

# Subsample by default. Distinguishing 0.05 from 0.25 does not need 651 queries
# -- at ~150 the standard error on ndcg@10 is around 0.025, so a working model
# sits several SE clear of the floor and a broken one is nowhere near it.
# evaluate.py's --limit takes sorted(queries)[:N], i.e. the numerically lowest GO
# ids, which skew old and general. That bias is irrelevant at this separation and
# it is deterministic, so run-to-run comparisons still hold. Set 0 for the full
# set if you ever want the gate's number to be comparable to the training log's.
GATE_LIMIT = int(os.getenv("QDRANT_GATE_LIMIT", "150")) or None

# Versioned collections kept behind the live one, for rollback.
# How many superseded collections stay resident on the SERVER. 0 keeps only the
# live one, because rollback history lives on disk now (KEEP_SNAPSHOTS below).
# A retained collection costs ~162MB of RAM -- vectors are created with
# on_disk=False -- while a snapshot of the same data costs only disk. Set this
# to 1 to buy back a seconds-fast alias-flip rollback for one step.
KEEP_VERSIONS = int(os.getenv("QDRANT_KEEP_VERSIONS", "0"))
# How many versioned snapshots to keep in data/qdrant/snapshots/. THIS is the
# rollback store. Every other artifact here is overwritten in place --
# qdrant_export/ is rmtree'd on ingest, EVAL_DIR on every gate -- so without
# these, no copy of any previous build exists anywhere.
KEEP_SNAPSHOTS = int(os.getenv("QDRANT_KEEP_SNAPSHOTS", "3"))
# Seconds for a single snapshot API call. Generously over the ~4s a
# 235MB chunk snapshot takes, because the cost of being wrong is a
# failed task and the cost of being generous is nothing.
SNAPSHOT_TIMEOUT = int(os.getenv("QDRANT_SNAPSHOT_TIMEOUT", "600"))

# Where a serving release is published. The snapshots and the Neo4j dump are
# only valid as a SET -- a snapshot restores into the Qdrant image pinned in
# docker-compose.yml, and the collections only retrieve sensibly with the
# encoder that built them -- so they go out as one release, not three.
GITHUB_REPO = os.getenv("GITHUB_DATASET_REPO", "yoyo4581/POKnowledgeBase_ETL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
NEO4J_DUMP = REPO_ROOT / "data" / "neo4j" / "dumps" / "neo4j.dump"
# Pinned in docker-compose.yml. Recorded in the release body because a
# consumer restoring into a different Qdrant major may simply fail: snapshots
# are tied to the storage format, unlike qdrant_export/.
QDRANT_IMAGE = os.getenv("QDRANT_IMAGE", "qdrant/qdrant:v1.19.1")
NEO4J_IMAGE = os.getenv("NEO4J_IMAGE", "neo4j:5.19.0-community")


# Artifact name -> where it unpacks to. The notebook produces exactly these.
ARTIFACTS = {
    "biobert-go-retrieval.zip": MODEL_DIR,
    "qdrant_export.zip": EXPORT_DIR,
    "qdrant_store.zip": STORE_DIR,
}

def run_cli(script: Path, *args: str) -> None:
    """
    Run one of the Qdrant entry points as a subprocess rather than importing it.

    store.py and evaluate.py both start with core.cli.bootstrap(), which inserts
    src/builders/Qdrant on sys.path so that `import core` resolves -- a very
    generic name to graft onto a long-lived worker's import table. They also
    hold an exclusive lock on the embedded store folder and lru_cache the
    encoder. A subprocess gives all of that a hard boundary, and the logged
    command line is one you can paste into a shell to reproduce a failure.
    """
    cmd = [sys.executable, str(script), *args]
    logger.info("$ %s", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True)
    for line in proc.stdout.splitlines():
        logger.info("  %s", line)
    if proc.returncode != 0:
        for line in proc.stderr.splitlines():
            logger.error("  %s", line)
        raise RuntimeError(f"{script.name} exited {proc.returncode}")


def ingest_artifacts() -> dict:
    """
    Unpack whatever is sitting in the incoming folder.

    The folder is the seam for the Drive hop, deliberately left as a plain
    directory so the DAG runs today without Google credentials. To automate
    the hop, make this task's first step one of:

      rclone      `rclone copy gdrive:POKnowledgeBase/qdrant {INCOMING_DIR}`
                  -- one binary, a token in rclone.conf, no provider install.
                  Simplest thing that works, and it handles a personal Drive.
      provider    pip install apache-airflow-providers-google, then
                  GoogleDriveToLocalOperator with a service account that the
                  Drive folder is shared with. More Airflow-native; the
                  service account cannot own files in a personal Drive, only
                  read a folder shared into it.

    Either way the contract below is unchanged: zips land here, this task
    unpacks them, and a consumed archive is moved aside so a re-run does not
    silently reprocess the same model.
    """
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)

    if REINGEST and ARCHIVE_DIR.is_dir():
        back = list(ARCHIVE_DIR.glob("*.zip"))
        for z in back:
            shutil.move(str(z), str(INCOMING_DIR / z.name))
        logger.info("QDRANT_REINGEST: moved %d archive(s) back from %s",
                    len(back), ARCHIVE_DIR)

    found = {}

    for name, destination in ARTIFACTS.items():
        archive = INCOMING_DIR / name
        if not archive.exists():
            continue
        logger.info("Unpacking %s -> %s", archive.name, destination)
        if destination.exists():
            shutil.rmtree(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive) as zf:
            # The notebook zips the folder itself, so entries are prefixed
            # with it. Strip that prefix rather than trusting the zip's
            # layout to match the destination name.
            staging = destination.parent / f".{destination.name}.unpack"
            if staging.exists():
                shutil.rmtree(staging)
            zf.extractall(staging)
            roots = list(staging.iterdir())
            inner = roots[0] if len(roots) == 1 and roots[0].is_dir() else staging
            shutil.move(str(inner), str(destination))
            shutil.rmtree(staging, ignore_errors=True)
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        shutil.move(str(archive), str(ARCHIVE_DIR / archive.name))
        found[name] = str(destination)

    if found:
        logger.info("Ingested: %s", ", ".join(sorted(found)))
    else:
        logger.info("Nothing in %s; falling back to whatever is already on disk.",
                    INCOMING_DIR)
    return {"ingested": sorted(found), "had_export": "qdrant_export.zip" in found,
            "had_store": "qdrant_store.zip" in found}


def build_collections(ingested: dict) -> dict:
    """
    Encode the corpus into an embedded store plus a portable export.

    Skipped when Colab already did it -- it had the model loaded on a GPU,
    so rebuilding here would burn CPU minutes to produce the same vectors.
    Run locally when only the corpus changed and the model did not, which
    is the common case between fine-tunes.
    """
    # Decided from what is ON DISK, not from what arrived this run. The
    # earlier version keyed off ingested["had_export"], which made a re-run
    # destructive: ingest_artifacts moves consumed zips to consumed/, so the
    # second run sees nothing ingested, concludes it must rebuild, deletes
    # the perfectly good collections Colab produced, and spends tens of CPU
    # minutes re-encoding 26k texts -- with the local WINDOW/STRIDE, which
    # need not match the ones the export was built with. Re-running a DAG
    # must not be destructive.
    manifest_path = EXPORT_DIR / "collection_config.json"
    have_export = manifest_path.exists()
    have_store = STORE_DIR.is_dir() and any(STORE_DIR.iterdir())

    if have_export and have_store and not FORCE_REBUILD:
        m = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        logger.info("Reusing the collections already on disk -- built %s, "
                    "model=%s, window=%s/stride=%s, %s records / %s chunks. "
                    "Set QDRANT_FORCE_REBUILD=1 to re-encode anyway.",
                    m.get("created"), m.get("model"), m.get("window"),
                    m.get("stride"), m.get("n_records"), m.get("n_chunks"))
        return {"built_locally": False}

    # The artifacts may be sitting right there, un-ingested. That happens when
    # only THIS task is cleared instead of the whole DAG run: ingest_artifacts
    # does not re-run, so build_collections reuses its previous (empty) XCom,
    # concludes nothing arrived, and starts a tens-of-minutes CPU encode while
    # the finished collections wait in a zip two directories away. Refuse, and
    # say which button to press.
    waiting = sorted(n for n in ARTIFACTS if (INCOMING_DIR / n).exists())
    if waiting and not FORCE_REBUILD:
        raise RuntimeError(
            f"Refusing to re-encode: {', '.join(waiting)} are sitting unconsumed "
            f"in {INCOMING_DIR}, so ingest_artifacts did not run in this attempt. "
            f"Clear the DAG run from ingest_artifacts DOWNSTREAM (not this task "
            f"alone) so the zips get unpacked, or set QDRANT_FORCE_REBUILD=1 to "
            f"encode locally and leave them where they are.")

    logger.warning(
        "No usable export+store on disk (export=%s, store=%s)%s -- re-encoding "
        "the whole corpus locally. That is ~26k texts through BioBERT on CPU "
        "and takes tens of minutes; it is slow, not hung. If Colab already "
        "built these, cancel, put the zips back in %s (they are in %s, or set "
        "QDRANT_REINGEST=1) and re-run instead.",
        have_export, have_store, " [QDRANT_FORCE_REBUILD]" if FORCE_REBUILD else "",
        INCOMING_DIR, ARCHIVE_DIR)

    if not MODEL_DIR.exists():
        raise RuntimeError(
            f"No encoder at {MODEL_DIR} and no export was ingested. Either drop "
            f"the Colab artifacts in {INCOMING_DIR} or point BIOBERT_MODEL_PATH "
            f"at a fine-tuned model.")

    corpus = DATASET_DIR / "eval_corpus.json"
    if not corpus.exists():
        raise RuntimeError(
            f"{corpus} is missing -- run embedding_dataset_export first. "
            f"Despite the name it is the whole corpus, not the held-out slice.")

    # Only now, having committed to rebuilding. Both targets are this DAG's
    # own scratch space, not anything being served -- the live collections are
    # touched only by restore_to_server below, and only via the alias.
    for path in (STORE_DIR, EXPORT_DIR):
        if path.exists():
            shutil.rmtree(path)

    args = [
        "build",
        "--corpus", str(corpus),
        "--model", str(MODEL_DIR),
        "--storage", str(STORE_DIR),
        "--export", str(EXPORT_DIR),
        "--window", str(WINDOW),
        "--stride", str(STRIDE),
        "--batch-size", str(BATCH_SIZE),
    ]
    entrez_map = DATASET_DIR / "entrez_to_uniprots.json"
    if entrez_map.exists():
        args += ["--entrez-map", str(entrez_map)]

    run_cli(QDRANT_SRC / "build" / "store.py", *args)
    return {"built_locally": True}


def evaluate_gate(built: dict) -> dict:
    """
    Score the freshly built store before anything points at it.

    Runs against the *embedded* store, not the server, so the gate never
    touches production. The queries are the held-out GO terms and relevance
    is "annotated to this term" -- the same judgments the fine-tune selected
    checkpoints on, so a number here is comparable to the training log.

    An automated pipeline needs this for a reason the README states outright:
    an encoder/collection mismatch is undetectable at query time. It does not
    error, it just quietly retrieves badly. A metric floor is the only thing
    standing between "the wrong model got pinned" and "search got worse and
    nobody noticed for a month".
    """
    if not STORE_DIR.exists():
        raise AirflowSkipException(
            f"No embedded store at {STORE_DIR} to evaluate. Colab shipped an "
            f"export without qdrant_store.zip; the restore below will proceed "
            f"ungated.")

    if EVAL_DIR.exists():
        shutil.rmtree(EVAL_DIR)

    # max(--ks) and --map-k may not exceed --top-k; evaluate.py exits if they do.
    ks = [k for k in (1, 3, 5, 10) if k <= GATE_TOP_K] or [GATE_TOP_K]
    argv = ["--data-dir", str(DATASET_DIR),
            "--model", str(MODEL_DIR),
            "--storage", str(STORE_DIR),
            "--out", str(EVAL_DIR),
            "--variants", GATE_ARM,
            "--top-k", str(GATE_TOP_K),
            "--map-k", str(GATE_TOP_K),
            "--ks", *[str(k) for k in ks]]
    if GATE_LIMIT:
        argv += ["--limit", str(GATE_LIMIT)]
    run_cli(QDRANT_SRC / "analysis" / "evaluate.py", *argv)

    results = json.loads((EVAL_DIR / "summary.json").read_text())
    score = results["summary"][GATE_ARM][GATE_METRIC]
    logger.info("Gate: %s %s = %.4f (floor %.4f)", GATE_ARM, GATE_METRIC, score, GATE_FLOOR)

    if score < GATE_FLOOR:
        raise RuntimeError(
            f"{GATE_ARM} {GATE_METRIC}={score:.4f} is below the floor "
            f"{GATE_FLOOR:.4f}. Not promoting this build. Check that "
            f"BIOBERT_MODEL_PATH is the encoder the export was built with -- "
            f"a mismatch produces exactly this and nothing else reports it.")

    return {"score": score, "metric": GATE_METRIC, "arm": GATE_ARM}


def restore_to_server(gate: dict) -> dict:
    """
    Restore the export into versioned collections, then flip the aliases.

    store.py's cmd_restore writes to the fixed names function_records /
    function_chunks, which is exactly what must not happen to a live server.
    Its pieces take the collection name as a parameter though, so this
    composes them against a versioned physical name while still deriving
    point ids from the *logical* one -- ids_for() branches on `name ==
    RECORDS`, so passing it the versioned name would hash every record as a
    chunk.

    Imports are inside the task because store.py mutates sys.path at import
    time (see run_cli) and because qdrant-client may not be installed yet;
    a missing dependency should fail this task, not un-parse the DAG.
    """
    sys.path.insert(0, str(QDRANT_SRC))
    from build.store import (check_bm25_params, ensure_collection, ids_for,
                             read_export, upsert)
    from core.config import CHUNKS, RECORDS, load_sparse_encoder, open_client
    from qdrant_client import models

    manifest, data = read_export(EXPORT_DIR)
    version = manifest["created"].replace("-", "").replace(":", "").replace("T", "").rstrip("Z")
    logger.info("Restoring export built %s (model=%s, dim=%s, window=%s/stride=%s) as v%s",
                manifest["created"], manifest["model"], manifest["dim"],
                manifest["window"], manifest["stride"], version)

    # restore verifies bm25_params but NOT window/stride, so a divergence
    # between the shipped export and this host's config is otherwise silent.
    # It is not fatal here -- the export's own values are what got built --
    # but it means a later local rebuild would produce different granularity.
    if (manifest.get("window"), manifest.get("stride")) != (WINDOW, STRIDE):
        logger.warning(
            "Export was built at window=%s/stride=%s but this host is "
            "configured for %s/%s. Restoring the export as-is (its values "
            "win), but set QDRANT_WINDOW/QDRANT_STRIDE to match, or a local "
            "rebuild will chunk differently than what was evaluated.",
            manifest.get("window"), manifest.get("stride"), WINDOW, STRIDE)

    sparse_encoder = load_sparse_encoder()
    check_bm25_params(manifest, sparse_encoder)

    client = open_client(url=QDRANT_URL, api_key=QDRANT_API_KEY)
    live: dict[str, str] = {}
    try:
        physical = {}
        for logical in (RECORDS, CHUNKS):
            # A real collection sitting on the logical name blocks the alias.
            # That happens when someone ran `store.py restore` by hand once.
            if client.collection_exists(logical) and logical not in {
                    a.alias_name for a in client.get_aliases().aliases}:
                raise RuntimeError(
                    f"{logical!r} exists as a real collection, not an alias, so the "
                    f"alias cannot be created. It was probably made by a manual "
                    f"`store.py restore`. Delete it once and this DAG owns the name "
                    f"from then on.")

            name = f"{logical}__v{version}"
            payloads, dense = data[logical]
            ensure_collection(client, name, manifest["dim"], on_disk=False, recreate=True)
            logger.info("  re-deriving bm25 for %d %s...", len(payloads), logical)
            sparse = list(sparse_encoder.embed([p["text"] for p in payloads],
                                               batch_size=BATCH_SIZE))
            # ids_for() gets the LOGICAL name; the collection gets the versioned one.
            upsert(client, name, payloads, dense, sparse, ids_for(logical, payloads))
            physical[logical] = name

        # One atomic batch: for the application there is no moment where the
        # name resolves to nothing or to a half-written collection.
        live = {a.alias_name: a.collection_name for a in client.get_aliases().aliases}
        operations = []
        for logical, name in physical.items():
            if logical in live:
                operations.append(models.DeleteAliasOperation(
                    delete_alias=models.DeleteAlias(alias_name=logical)))
            operations.append(models.CreateAliasOperation(
                create_alias=models.CreateAlias(collection_name=name, alias_name=logical)))
        client.update_collection_aliases(change_aliases_operations=operations)

        for logical, name in physical.items():
            logger.info("Alias %s -> %s (was %s)", logical, name, live.get(logical, "none"))
    finally:
        client.close()

    return {"version": version, "collections": physical,
            "previous": live, "gate": gate}


def prune_old_collections(restored: dict) -> None:
    """
    Drop superseded versions, keeping KEEP_VERSIONS behind the live one.

    Defaults to 0, so only the live collection stays resident. Rollback
    history is kept instead as versioned snapshots on disk (see
    snapshot_collections) -- the same insurance paid for in disk rather
    than RAM. The trade is speed: an alias flip is seconds, re-uploading a
    snapshot is minutes.
    """
    sys.path.insert(0, str(QDRANT_SRC))
    from core.config import CHUNKS, RECORDS, open_client

    client = open_client(url=QDRANT_URL, api_key=QDRANT_API_KEY)
    try:
        aliased = {a.collection_name for a in client.get_aliases().aliases}
        for logical in (RECORDS, CHUNKS):
            versions = sorted(
                (c.name for c in client.get_collections().collections
                 if c.name.startswith(f"{logical}__v")),
                reverse=True)
            for name in versions[KEEP_VERSIONS + 1:]:
                if name in aliased:
                    continue
                logger.info("Dropping superseded collection %s", name)
                client.delete_collection(name)
            logger.info("%s: keeping %s", logical, versions[:KEEP_VERSIONS + 1])
    finally:
        client.close()


def snapshot_collections(restored: dict) -> dict:
    """
    Snapshot the just-promoted collections into data/qdrant/snapshots/.

    The Qdrant half of what neo4j_snapshot does for the graph: turn the live
    server's state into a file another machine can start from with Docker
    alone (docker compose --profile load run --rm qdrant-load). It runs
    AFTER the alias flip on purpose -- what ships is then exactly what was
    gated and promoted, which is the property a separately-shipped
    export/store pair does not have.

    No stop/start, unlike neo4j-dump: Qdrant snapshots a live collection.

    Fetched over HTTP rather than read off the /snapshots bind mount, even
    though the mount is right there. Qdrant writes snapshots as root with
    mode 0600, and the Airflow worker is not root -- reading or moving the
    file directly fails with PermissionError. The download endpoint hands
    back the same bytes through the API, written as whoever is running
    this, so the ownership question never arises. It also means this task
    works against a Qdrant that is not on this host at all.

    The server-side copy is deleted afterwards: it has already been written
    to its final home, and leaving it costs storage on every build.
    """
    import urllib.request

    sys.path.insert(0, str(QDRANT_SRC))
    from core.config import CHUNKS, RECORDS, open_client

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    headers = {"api-key": QDRANT_API_KEY} if QDRANT_API_KEY else {}

    # Explicit timeout: qdrant-client defaults to 5s, and create_snapshot on
    # the chunk collection runs ~4s idle -- fine in isolation, intermittent
    # here, where this fires straight after restore_to_server has upserted
    # tens of thousands of points and prune has dropped collections. It
    # surfaces as a bare "ResponseHandlingException: timed out" with the
    # snapshot usually finishing on the server anyway.
    client = open_client(url=QDRANT_URL, api_key=QDRANT_API_KEY, timeout=SNAPSHOT_TIMEOUT)
    written = {}
    try:
        for logical in (RECORDS, CHUNKS):
            # The physical versioned name, not the alias: the snapshot API
            # needs a real collection, and naming it explicitly means this
            # cannot race a later flip.
            physical = restored["collections"][logical]
            desc = client.create_snapshot(collection_name=physical, wait=True)
            if desc is None:
                raise RuntimeError(
                    f"create_snapshot({physical}) returned no description; the "
                    f"server accepted the call but reported nothing to collect.")

            # Versioned filename: this directory IS the rollback store now
            # (KEEP_VERSIONS defaults to 0, so the server keeps no spare
            # collections). The YYYYMMDDHHMMSS version sorts lexicographically
            # in time order, which is what lets both the pruning below and
            # the compose loader pick "newest" without parsing anything.
            target = SNAPSHOT_DIR / f"{logical}-v{restored['version']}.snapshot"
            url = f"{QDRANT_URL}/collections/{physical}/snapshots/{desc.name}"
            tmp = target.with_suffix(".snapshot.part")
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=1800) as r, open(tmp, "wb") as f:
                shutil.copyfileobj(r, f, length=1024 * 1024)
            tmp.replace(target)       # atomic: no half-written .snapshot on failure

            client.delete_snapshot(collection_name=physical,
                                   snapshot_name=desc.name, wait=True)

            size_mb = target.stat().st_size / 1e6
            if size_mb == 0:
                raise RuntimeError(f"{target} downloaded as 0 bytes.")
            logger.info("Snapshot %s -> %s (%.0f MB)", physical, target.name, size_mb)
            written[logical] = {"file": str(target), "from": physical,
                                "size_mb": round(size_mb, 1)}

            # Prune this logical name's older snapshots. Done per name and
            # only after its new one is safely on disk, so a failure here
            # can never leave a collection with no snapshot at all.
            kept = sorted(SNAPSHOT_DIR.glob(f"{logical}-v*.snapshot"), reverse=True)
            for stale in kept[KEEP_SNAPSHOTS:]:
                logger.info("Dropping superseded snapshot %s", stale.name)
                stale.unlink()
            logger.info("%s: keeping %s", logical,
                        [f.name for f in kept[:KEEP_SNAPSHOTS]])
    finally:
        client.close()

    return {"snapshots": written, "version": restored["version"]}


def publish_snapshots(snapshotted: dict) -> str:
    """
    Attach this build's snapshots -- and the Neo4j dump, if one is current
    -- to a GitHub release, so a consumer can stand the whole stack up with
    Docker and curl and nothing else.

    One release, all assets. They are only valid as a set: the Qdrant
    snapshots restore into the image pinned in docker-compose.yml, and the
    collections only retrieve sensibly with the encoder that built them. A
    consumer who mixes versions gets no error, just worse answers -- so the
    body records the image tags and the encoder the manifest names.

    Tagged by the collection version rather than a content hash. Unlike
    the dataset, a snapshot is not reproducible byte-for-byte from the same
    inputs (it carries build timestamps and segment layout), so hashing it
    would publish a new release every run. The version already identifies
    the build that produced it.

    Skips rather than fails when GITHUB_TOKEN is absent: snapshots on disk
    are still a usable rollback store, publishing is the optional half.
    """
    import requests

    if not GITHUB_TOKEN:
        raise AirflowSkipException(
            f"GITHUB_TOKEN is not set; snapshots are in {SNAPSHOT_DIR} but not "
            f"published. Set it in .env to enable the release handoff.")

    version = snapshotted["version"]
    tag = f"serving-v{version}"
    api = f"https://api.github.com/repos/{GITHUB_REPO}"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}",
               "Accept": "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28"}

    if requests.get(f"{api}/releases/tags/{tag}", headers=headers, timeout=30).status_code == 200:
        raise AirflowSkipException(
            f"{tag} is already published -- this collection build has been "
            f"released, so there is nothing new to ship.")

    # (local file, PUBLISHED name). The published name deliberately drops the
    # version, because GitHub serves
    #   /releases/latest/download/<asset-name>
    # as a permanent URL to the newest release -- but only when the name is
    # stable across releases. That URL is what lets compose.serve.yaml fetch
    # the data itself with no clone and no pinned tag; a versioned asset name
    # would 404 the moment the next build ships. Nothing is lost: the version
    # is in the release tag, the body, and the snapshot's own manifest.
    assets = [(Path(v["file"]), f"{logical}.snapshot")
              for logical, v in snapshotted["snapshots"].items()]
    if NEO4J_DUMP.exists():
        assets.append((NEO4J_DUMP, NEO4J_DUMP.name))
    else:
        # Not fatal: the vector half is independently useful, and the dump
        # is produced by a different DAG on a different trigger. But a
        # release without it cannot stand up the graph, so say so loudly.
        logger.warning(
            "%s does not exist, so this release ships the vector store only. "
            "Run neo4j_snapshot first if consumers need the graph too.",
            NEO4J_DUMP)

    # Checksums as their own asset: a consumer curling 235MB over a flaky
    # link has no other way to tell a truncated download from a good one,
    # and Qdrant's failure mode on a corrupt snapshot is not obvious.
    digests = {}
    for path, published in assets:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for block in iter(lambda: f.read(1024 * 1024), b""):
                h.update(block)
        # Keyed by PUBLISHED name, not the local one: a consumer runs
        # `sha256sum -c SHA256SUMS` against what they downloaded, so the
        # file has to name the assets as they arrive.
        digests[published] = h.hexdigest()
        logger.info("  %s -> %s  %.0f MB  %s", path.name, published,
                    path.stat().st_size / 1e6, h.hexdigest()[:16])

    sums = SNAPSHOT_DIR / "SHA256SUMS"
    sums.write_text("".join(f"{d}  {n}\n" for n, d in sorted(digests.items())))

    manifest_path = EXPORT_DIR / "collection_config.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig")) if manifest_path.exists() else {}
    body = (
        f"Serving artifacts for the knowledge base: restore these and the stack runs "
        f"with Docker alone.\n\n"
        f"- collection version: `{version}`\n"
        f"- records / chunks: {manifest.get('n_records', '?')} / {manifest.get('n_chunks', '?')}\n"
        f"- entrez ids: {manifest.get('n_entrez_ids', '?')}\n"
        f"- encoder: `{manifest.get('model', '?')}`\n"
        f"- window/stride: {manifest.get('window', '?')}/{manifest.get('stride', '?')}\n"
        f"- graph dump: {'included' if NEO4J_DUMP.exists() else 'NOT included'}\n\n"
        f"Pinned images -- a snapshot is tied to the storage format, so these are "
        f"not advisory:\n"
        f"- `{QDRANT_IMAGE}`\n- `{NEO4J_IMAGE}`\n\n"
        f"Install:\n"
        f"```\n"
        f"# drop *.snapshot in data/qdrant/snapshots/ and neo4j.dump in data/neo4j/dumps/\n"
        f"sha256sum -c SHA256SUMS\n"
        f"docker compose up -d qdrant neo4j\n"
        f"docker compose --profile load run --rm qdrant-load\n"
        f"docker compose stop neo4j\n"
        f"docker compose --profile load run --rm neo4j-load\n"
        f"docker compose up -d neo4j\n"
        f"```\n"
    )

    created = requests.post(
        f"{api}/releases", headers=headers, timeout=60,
        json={"tag_name": tag, "name": f"Serving {version}", "body": body})
    created.raise_for_status()
    release = created.json()

    upload_url = release["upload_url"].split("{")[0]
    for path, published in assets + [(sums, sums.name)]:
        # Streamed from the file handle, not read_bytes(): the chunk
        # snapshot is ~235MB and there is no reason to hold it in memory.
        with open(path, "rb") as fh:
            up = requests.post(
                upload_url, headers={**headers, "Content-Type": "application/octet-stream"},
                params={"name": published}, data=fh, timeout=1800)
        up.raise_for_status()
        logger.info("Uploaded %s", published)

    logger.info("Published %s -> %s", tag, release["html_url"])
    return release["html_url"]
