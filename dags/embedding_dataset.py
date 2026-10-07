"""
The CPU half of the embedding pipeline: build the GO contrastive training
dataset from the live databases, publish it somewhere Colab can reach, and
measure whether the model currently in production still earns its keep
against the freshly held-out terms.

Nothing here needs a GPU. Fine-tuning does, and Colab cannot be started
from Airflow -- there is no headless execution API for it -- so the GPU
step is a human opening Finetune_BioBERT_Colab.ipynb.
embedding_drift_check below exists so that "when do I open it?" is a
measured threshold instead of a guess.

    embedding_dataset_export   neo4j://kgml_complete -> dataset + release
    embedding_drift_check      qdrant://dataset_export -> retrain? y/n
    (human)                    Colab notebook -> model + store on Drive
    qdrant_collection_build    dags/qdrant_build.py, triggered by the notebook

Why the dataset goes out as a GitHub *release asset* rather than a commit:
train.jsonl is ~56MB and is regenerated on every run of this DAG. Committing
it would add that to the repository's permanent history each month, which no
later `git rm` can undo. A release asset is versioned, fetchable with one
authenticated GET, and contributes nothing to clone size.
"""
from airflow.sdk import Asset, dag, task, Metadata
from airflow.exceptions import AirflowSkipException

import hashlib
import json
import logging
import os
import tarfile
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
UUID = os.getenv('uuid')

REPO_ROOT = Path(__file__).resolve().parents[1]
DATASET_DIR = REPO_ROOT / "data" / "qdrant" / "go_contrastive"
DRIFT_LOG = REPO_ROOT / "data" / "qdrant" / "drift_history.jsonl"

# The model the application is serving right now. The drift check scores THIS
# one against the newly held-out terms; it is not the checkpoint Colab is about
# to produce.
MODEL_DIR = Path(os.getenv("BIOBERT_MODEL_PATH",
                           REPO_ROOT / "EmbeddingModel" / "biobert-go-retrieval"))

GITHUB_REPO = os.getenv("GITHUB_DATASET_REPO", "yoyo4581/POKnowledgeBase_ETL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# Absolute floor: below this the retrieval quality is not worth serving.
NDCG_FLOOR = float(os.getenv("EMBEDDING_NDCG_FLOOR", "0.35"))
# Relative floor: a drop this large against the previous measurement means the
# corpus has moved under the model even if the absolute number still looks fine.
NDCG_DROP_TOLERANCE = float(os.getenv("EMBEDDING_NDCG_DROP_TOLERANCE", "0.02"))

# Declared by URI, so this is the same asset go_ontology_annotation writes in
# dags/routine.py -- GO annotation edges in Neo4j are current. The other half of
# the corpus (dbo.FunctionData) comes from the manual function_data_build DAG,
# which has no asset to wait on; a stale FunctionData shows up here as an
# unchanged fingerprint and a skipped publish rather than as a wrong dataset.
NEO4J_KG_COMPLETE = Asset("neo4j://kgml_complete")

EMBEDDING_DATASET_READY = Asset("qdrant://dataset_export")
EMBEDDING_RETRAIN_NEEDED = Asset("qdrant://retrain_needed")

# Written by export_dataset(), plus the entrez crosswalk this DAG adds.
DATASET_FILES = ("train.jsonl", "eval_queries.json", "eval_corpus.json",
                 "eval_relevant.json", "stats.json", "entrez_to_uniprots.json")


def fingerprint(directory: Path) -> str:
    """
    Content hash of the exported dataset, used purely as a change detector.

    build_dataset() is seeded (Config.seed=13) and its inputs are the two
    databases, so identical databases produce byte-identical files -- which is
    what makes "same hash" mean "same dataset" rather than "same minute". The
    tarball itself is not hashed because tar embeds mtimes and would differ on
    every run regardless of content.
    """
    h = hashlib.sha256()
    for name in DATASET_FILES:
        path = directory / name
        h.update(name.encode())
        if path.exists():
            h.update(path.read_bytes())
    return h.hexdigest()


@dag(
    schedule=[NEO4J_KG_COMPLETE],
    catchup=False,
    tags=["qdrant", "embedding", "neo4j", "sql"],
)
def embedding_dataset_export():
    """
    Rebuilds the GO contrastive dataset from Neo4j + SQL and publishes it as a
    GitHub release asset for the Colab fine-tuning notebook to pull.

    1. extract() the annotations/hierarchy/terms from Neo4j and the protein
       function text from SQL, run build_dataset(), export_dataset() to
       data/qdrant/go_contrastive/.
    2. Write entrez_to_uniprots.json alongside it -- export_dataset() does not,
       and store.py's --entrez-map is what puts an entrez qualifier on each
       Qdrant record, so a caller holding either id form can reach it.
    3. Tar it and attach it to a release, unless the content hash says nothing
       changed since the last publish.
    """

    @task(outlets=[EMBEDDING_DATASET_READY])
    def export_training_dataset() -> dict:
        """
        Neo4j + SQL -> data/qdrant/go_contrastive/.

        Imports are deferred into the task body on purpose: dataset.py pulls in
        the Neo4j/SQL callers, and Neo4jCaller validates NEO4J_USERNAME/PASSWORD
        at import time. Doing that at DAG-parse time would take the whole file
        out of the scheduler the moment a credential goes missing, instead of
        failing one task run.
        """
        from EmbeddingModel.BioBERT_Files.dataset import (Config, build_dataset,
                                                          extract, load_entrez_crosswalk)
        from EmbeddingModel.BioBERT_Files.train import export_dataset
        from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
        from src.builders.SQL.SQLCaller import SQL_ETL

        neo4j_caller = Neo4j_ETL()
        sql_caller = SQL_ETL(run_id=UUID)

        raw = extract(neo4j_caller, sql_caller)
        data = build_dataset(**raw, cfg=Config())
        logger.info("build_dataset stats: %s", data.stats)

        # A dataset with no triplets or no held-out queries is not a dataset.
        # export_dataset() will happily write 0-byte files, this task would
        # still succeed, and EMBEDDING_DATASET_READY would still fire -- which
        # is exactly how an empty export once got published and only surfaced
        # two runs later as an embedding_drift_check failure. The usual cause
        # is dbo.FunctionData being empty: function_data_build must run first.
        if not data.rows or not data.eval_queries:
            raise RuntimeError(
                f"Refusing to publish an empty dataset: {len(raw['genes'])} corpus texts, "
                f"{len(raw['annotations'])} annotations, {len(data.rows)} triplets, "
                f"{len(data.eval_queries)} held-out queries. "
                "An empty dbo.FunctionData is the usual cause -- run function_data_build.")

        out_dir = export_dataset(data, DATASET_DIR)

        # The entrez crosswalk, NOT the Gene.id map extract() uses internally.
        # Under Reactome those are different maps: Gene.id is already the
        # accession, so extract()'s map is the identity, and writing that out
        # would stamp entrez_id = "P04637" onto every Qdrant record. Scoped to
        # the exported corpus so the file carries no dead entries.
        entrez_to_uniprots = load_entrez_crosswalk(sql_caller, corpus=data.eval_corpus)
        covered = {u for us in entrez_to_uniprots.values() for u in us}
        (out_dir / "entrez_to_uniprots.json").write_text(
            json.dumps(entrez_to_uniprots, indent=2), encoding="utf-8")
        logger.info("Wrote entrez_to_uniprots.json (%d entrez ids covering %d/%d corpus accessions)",
                    len(entrez_to_uniprots), len(covered), len(data.eval_corpus))

        digest = fingerprint(out_dir)
        logger.info("Dataset fingerprint %s (%d triplets, %d held-out terms, %d corpus texts)",
                    digest[:12], len(data.rows), len(data.eval_queries), len(data.eval_corpus))

        return {
            "fingerprint": digest,
            "n_rows": len(data.rows),
            "n_eval_queries": len(data.eval_queries),
            "n_corpus": len(data.eval_corpus),
        }

    @task()
    def publish_dataset(exported: dict) -> str:
        """
        Attaches the dataset as a release asset, so the Colab notebook can fetch
        it with one GET instead of a Drive upload by hand.

        Skips when the fingerprint matches the most recent published release --
        the dataset is deterministic given the databases, so an identical hash
        means an identical file and a new release would only add noise to the
        tag list.
        """
        import requests

        if not GITHUB_TOKEN:
            raise AirflowSkipException(
                "GITHUB_TOKEN is not set; the dataset is exported to "
                f"{DATASET_DIR} but not published. Set it in .env to enable "
                "the Colab handoff.")

        digest = exported["fingerprint"]
        tag = f"dataset-{digest[:12]}"
        api = f"https://api.github.com/repos/{GITHUB_REPO}"
        headers = {"Authorization": f"Bearer {GITHUB_TOKEN}",
                   "Accept": "application/vnd.github+json",
                   "X-GitHub-Api-Version": "2022-11-28"}

        existing = requests.get(f"{api}/releases/tags/{tag}", headers=headers, timeout=30)
        if existing.status_code == 200:
            raise AirflowSkipException(
                f"{tag} is already published -- the dataset is unchanged since "
                f"the last export, so there is nothing new for Colab to train on.")

        archive = DATASET_DIR.parent / f"go_contrastive-{digest[:12]}.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            for name in DATASET_FILES:
                path = DATASET_DIR / name
                if path.exists():
                    tar.add(path, arcname=f"go_contrastive/{name}")
        size_mb = archive.stat().st_size / 1e6
        logger.info("Packed %s (%.1f MB)", archive.name, size_mb)

        body = (
            f"GO contrastive training dataset.\n\n"
            f"- fingerprint: `{digest}`\n"
            f"- triplets: {exported['n_rows']}\n"
            f"- held-out GO terms: {exported['n_eval_queries']}\n"
            f"- corpus accessions: {exported['n_corpus']}\n"
            f"- exported: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n\n"
            f"Consumed by `Finetune_BioBERT_Colab.ipynb`."
        )
        created = requests.post(
            f"{api}/releases", headers=headers, timeout=60,
            json={"tag_name": tag, "name": f"Dataset {digest[:12]}",
                  "body": body, "prerelease": True})
        created.raise_for_status()
        release = created.json()

        upload = requests.post(
            release["upload_url"].split("{")[0],
            headers={**headers, "Content-Type": "application/gzip"},
            params={"name": archive.name},
            data=archive.read_bytes(), timeout=600)
        upload.raise_for_status()

        logger.info("Published %s -> %s", archive.name, release["html_url"])
        return release["html_url"]

    publish_dataset(export_training_dataset())


@dag(
    schedule=[EMBEDDING_DATASET_READY],
    catchup=False,
    tags=["qdrant", "embedding"],
)
def embedding_drift_check():
    """
    Scores the model currently in production against the freshly held-out GO
    terms, and raises the retrain flag only when the number says so.

    This is the whole reason the pipeline can tolerate a manual GPU step. The
    expensive question is "has the corpus moved far enough that the fine-tune
    is stale?", and it is answerable on CPU with the same
    InformationRetrievalEvaluator and the same metric
    (heldout_go_cosine_ndcg@10) that train_and_evaluate() selects checkpoints
    on -- so the number here is directly comparable to the one printed during
    the last training run.

    Two thresholds, because they catch different failures:
      absolute  the model is simply not good enough to serve
      relative  the model is still decent but is losing ground run over run,
                which is the corpus drifting rather than the model breaking
    """

    @task(outlets=[EMBEDDING_RETRAIN_NEEDED])
    def score_current_model():
        from sentence_transformers import SentenceTransformer
        from sentence_transformers.sentence_transformer.evaluation import (
            InformationRetrievalEvaluator)

        if not MODEL_DIR.exists():
            logger.warning("No model at %s -- nothing to score, a first fine-tune is needed.",
                           MODEL_DIR)
            yield Metadata(EMBEDDING_RETRAIN_NEEDED,
                           {"reason": "no model on disk", "model_path": str(MODEL_DIR)})
            return

        queries = json.loads((DATASET_DIR / "eval_queries.json").read_text())
        corpus = json.loads((DATASET_DIR / "eval_corpus.json").read_text())
        relevant = {t: set(g) for t, g in
                    json.loads((DATASET_DIR / "eval_relevant.json").read_text()).items()}

        logger.info("Scoring %s over %d held-out terms against %d corpus texts (CPU)",
                    MODEL_DIR, len(queries), len(corpus))
        model = SentenceTransformer(str(MODEL_DIR))
        evaluator = InformationRetrievalEvaluator(
            queries=queries, corpus=corpus, relevant_docs=relevant, name="heldout_go")
        scores = evaluator(model)
        metric = "heldout_go_cosine_ndcg@10"
        current = scores[metric]

        previous = None
        if DRIFT_LOG.exists():
            history = [json.loads(line) for line in
                       DRIFT_LOG.read_text().splitlines() if line.strip()]
            if history:
                previous = history[-1].get("ndcg@10")

        entry = {
            "measured": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "model": str(MODEL_DIR),
            "dataset_fingerprint": fingerprint(DATASET_DIR),
            "n_queries": len(queries),
            "n_corpus": len(corpus),
            "ndcg@10": current,
            "previous_ndcg@10": previous,
            "scores": scores,
        }
        # Recorded before any decision, so a healthy run still leaves a data
        # point -- the relative threshold below is only as good as the history.
        DRIFT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(DRIFT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        drop = (previous - current) if previous is not None else 0.0
        logger.info("%s = %.4f (previous %s, drop %+.4f; floor %.4f, tolerance %.4f)",
                    metric, current,
                    f"{previous:.4f}" if previous is not None else "none",
                    -drop, NDCG_FLOOR, NDCG_DROP_TOLERANCE)

        reasons = []
        if current < NDCG_FLOOR:
            reasons.append(f"{metric}={current:.4f} is below the floor {NDCG_FLOOR:.4f}")
        if drop > NDCG_DROP_TOLERANCE:
            reasons.append(f"{metric} fell {drop:.4f} since the last check "
                           f"({previous:.4f} -> {current:.4f})")

        if not reasons:
            # AirflowSkipException leaves EMBEDDING_RETRAIN_NEEDED unemitted,
            # which is the signal: the model on disk is still good enough.
            raise AirflowSkipException(
                f"{metric}={current:.4f} is healthy; no retrain needed.")

        logger.warning("RETRAIN NEEDED: %s", "; ".join(reasons))
        logger.warning("Open Finetune_BioBERT_Colab.ipynb in Colab; "
                       "it will clone this repo, pull the latest dataset "
                       "release, fine-tune, build the store, and drop the artifacts "
                       "on Drive for qdrant_collection_build to pick up.")
        yield Metadata(EMBEDDING_RETRAIN_NEEDED, {
            "reasons": reasons,
            "ndcg@10": current,
            "previous_ndcg@10": previous,
            "dataset_fingerprint": entry["dataset_fingerprint"],
        })

    score_current_model()


embedding_dataset_export()
embedding_drift_check()
