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

WINDOW = int(os.getenv("QDRANT_WINDOW", "3"))
STRIDE = int(os.getenv("QDRANT_STRIDE", "2"))
BATCH_SIZE = int(os.getenv("QDRANT_BATCH_SIZE", "64"))

# The gate. record_hybrid is the default search mode, so it is the arm whose
# quality actually reaches the application.
GATE_METRIC = os.getenv("QDRANT_GATE_METRIC", "ndcg@10")
GATE_ARM = os.getenv("QDRANT_GATE_ARM", "record_hybrid")
GATE_FLOOR = float(os.getenv("QDRANT_GATE_FLOOR", "0.35"))

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
        if ingested["had_export"] and ingested["had_store"]:
            logger.info("Colab shipped both the export and the embedded store; "
                        "not re-encoding.")
            return {"built_locally": False}

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

        # --recreate because both targets are this DAG's own scratch space, not
        # anything being served. The live collections are only touched by
        # restore_to_server below, and only via the alias.
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

        run_cli(QDRANT_SRC / "analysis" / "evaluate.py",
                "--data-dir", str(DATASET_DIR),
                "--model", str(MODEL_DIR),
                "--storage", str(STORE_DIR),
                "--out", str(EVAL_DIR))

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
