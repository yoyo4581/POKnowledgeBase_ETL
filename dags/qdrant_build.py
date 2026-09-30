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
"""
from airflow.sdk import Asset, dag, task
from airflow.exceptions import AirflowSkipException

import json
import logging
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
UUID = os.getenv('uuid')

REPO_ROOT = Path(__file__).resolve().parents[1]
QDRANT_SRC = REPO_ROOT / "src" / "builders" / "Qdrant"
DATASET_DIR = REPO_ROOT / "data" / "qdrant" / "go_contrastive"

# Where artifacts land on their way in from Drive. See ingest_artifacts().
INCOMING_DIR = Path(os.getenv("QDRANT_INCOMING_DIR", REPO_ROOT / "data" / "qdrant" / "incoming"))
ARCHIVE_DIR = INCOMING_DIR / "consumed"

EXPORT_DIR = REPO_ROOT / "data" / "qdrant" / "qdrant_export"
STORE_DIR = REPO_ROOT / "data" / "qdrant" / "qdrant_store"
EVAL_DIR = REPO_ROOT / "data" / "qdrant" / "eval_results"
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
KEEP_VERSIONS = int(os.getenv("QDRANT_KEEP_VERSIONS", "2"))

QDRANT_COLLECTIONS_LIVE = Asset("qdrant://collections_live")

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


@dag(
    schedule=None,
    catchup=False,
    tags=["qdrant", "embedding"],
)
def qdrant_collection_build():
    """
    Ingest -> build -> evaluate -> restore behind an alias.

    Runs after the Colab notebook has dropped its artifacts, but is also
    correct with no artifacts at all: with a model already on disk and a
    refreshed corpus, it rebuilds the collections locally on CPU. Encoding
    ~6k records plus their chunks through BioBERT is a single forward pass,
    minutes of CPU -- the GPU was only ever needed for the fine-tune.
    """

    @task()
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

    @task()
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

    @task()
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

    @task(outlets=[QDRANT_COLLECTIONS_LIVE], trigger_rule="none_failed")
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

    @task()
    def prune_old_collections(restored: dict) -> None:
        """
        Drop superseded versions, keeping KEEP_VERSIONS behind the live one so a
        bad promotion can be rolled back with an alias flip rather than a rebuild.
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

    ingested = ingest_artifacts()
    built = build_collections(ingested)
    gate = evaluate_gate(built)
    restored = restore_to_server(gate)
    prune_old_collections(restored)


qdrant_collection_build()
