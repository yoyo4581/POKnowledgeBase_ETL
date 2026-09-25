"""
The wiring every entry point repeated: path bootstrap, store arguments, the
retriever factory, and loading the held-out eval files.

Four scripts each declared --storage/--url/--api-key/--model and each built its
own client and encoder, three of them slightly differently. That is the kind of
duplication that drifts: one script gains a flag, another silently keeps an old
default, and a run is misconfigured in a way nothing reports.

bootstrap() exists because these scripts live in subdirectories now. Running
`python Qdrant/analysis/evaluate.py` puts analysis/ on sys.path, not Qdrant/, so
`core` would not import. One call at the top of each entry point fixes that and
makes the scripts runnable from any working directory.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def bootstrap() -> None:
    """Put the Qdrant/ root on sys.path so `core` imports from a subdirectory."""
    root = str(Path(__file__).resolve().parents[1])
    if root not in sys.path:
        sys.path.insert(0, root)


def add_store_args(parser, *, model_default: str = "biobert-go-retrieval") -> None:
    """Where the data is and which encoder reads it. Shared by every entry point."""
    parser.add_argument("--storage", default="qdrant_store",
                        help="Embedded on-disk store (default: qdrant_store).")
    parser.add_argument("--url", help="Or a Qdrant server, e.g. http://localhost:6333.")
    parser.add_argument("--api-key", help="API key, if the target needs one.")
    parser.add_argument("--model", default=model_default,
                        help="Fine-tuned BioBERT -- must be the one the collections "
                             "were built with. Nothing can detect a mismatch.")


def add_retrieval_args(parser) -> None:
    """The knobs that change what search does, as opposed to where it looks."""
    parser.add_argument("--prefetch-limit", type=int, default=200,
                        help="Candidates per branch (dense, sparse) at the record level.")
    parser.add_argument("--shortlist", type=int, default=100,
                        help="Records carried from stage 1 into the chunk re-rank. Must be "
                             ">= the ranked depth being scored, or the chunk arms are "
                             "judged on a shorter list than the baseline.")
    parser.add_argument("--chunk-prefetch-limit", type=int, default=1000,
                        help="Candidates per branch at the chunk level. Rule of thumb: "
                             ">= shortlist * mean chunks per record. Too low and records "
                             "low in the shortlist contribute no chunks, silently keeping "
                             "their baseline order.")


def settings_from(args):
    from core.config import Settings
    return Settings(
        storage=args.storage, url=getattr(args, "url", None),
        api_key=getattr(args, "api_key", None), model_path=args.model,
        prefetch_limit=getattr(args, "prefetch_limit", 200),
        shortlist=getattr(args, "shortlist", 100),
        chunk_prefetch_limit=getattr(args, "chunk_prefetch_limit", 1000),
    )


def open_retriever(args):
    """One client, one encoder, one sparse encoder -- built the same way everywhere."""
    from core.config import load_encoder, load_sparse_encoder, open_client
    from core.retriever import Retriever

    s = settings_from(args)
    return Retriever(
        client=open_client(storage=s.storage, url=s.url, api_key=s.api_key),
        encoder=load_encoder(s.model_path),
        sparse_encoder=load_sparse_encoder(),
        prefetch_limit=s.prefetch_limit,
        shortlist=s.shortlist,
        chunk_prefetch_limit=s.chunk_prefetch_limit,
    )


def require_populated(client) -> dict[str, int]:
    """
    Refuse to run against an empty store.

    An embedded client CREATES the folder it is pointed at, so a mistyped
    --storage yields a valid, empty database and a table of zeroes rather than
    an error.
    """
    from core.config import CHUNKS, RECORDS

    counts = {n: (client.get_collection(n).points_count
                  if client.collection_exists(n) else 0)
              for n in (RECORDS, CHUNKS)}
    empty = [n for n, c in counts.items() if not c]
    if empty:
        raise SystemExit(f"{' and '.join(empty)} is empty or missing. Build it with "
                         f"build/store.py, or point --storage/--url at the store "
                         f"you actually built.")
    return counts


# --- the held-out eval files --------------------------------------------
#
# eval_corpus.json is the WHOLE corpus despite its name: build_dataset() splits
# on GO terms, not on proteins, and sets eval_corpus = dict(gene_text). Only
# eval_queries and eval_relevant are restricted to held-out terms.

def load_queries(data_dir: str | Path) -> dict[str, str]:
    return json.loads((Path(data_dir) / "eval_queries.json").read_text(encoding="utf-8-sig"))


def load_eval_set(data_dir: str | Path):
    """(usable queries, relevance judgments, corpus) from the exported files."""
    data_dir = Path(data_dir)
    queries = load_queries(data_dir)
    relevant = {term: set(accs) for term, accs in json.loads(
        (data_dir / "eval_relevant.json").read_text(encoding="utf-8-sig")).items()}
    corpus_path = data_dir / "eval_corpus.json"
    corpus = json.loads(corpus_path.read_text(encoding="utf-8-sig")) if corpus_path.exists() else {}

    usable = {qid: text for qid, text in queries.items() if relevant.get(qid)}
    dropped = len(queries) - len(usable)
    if dropped:
        print(f"Skipping {dropped} query/queries with no relevant accessions.")
    return usable, relevant, corpus
