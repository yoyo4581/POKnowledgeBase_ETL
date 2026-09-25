"""
Shared wiring for the protein-function vector store: collection/vector
names, how to open a client against whichever backend is in play, and how
to load the two encoders.

There are three backends in this project's lifecycle and they all have to
name the same things:

  Colab (build)   QdrantClient(path=...)   -> an on-disk folder you zip and
                                              download; embedded, no server.
  here (inspect)  QdrantClient(path=...)   -> the same folder, cp'd over.
  ETL pipeline    QdrantClient(url=...)    -> a real server, populated from
                                              the portable export.

Only one of `storage` / `url` is ever set; open_client() enforces that so a
half-configured environment fails loudly instead of silently querying an
empty embedded store.

The dense encoder is the fine-tuned BioBERT from GO_Contrastive.py --
assumed already trained and saved, and pointed at by BIOBERT_MODEL_PATH.
The sparse side is BM25 computed explicitly with fastembed rather than via
qdrant-client's `models.Document` inference, so that indexing and querying
use the same code path on every client version and the portable export can
re-derive sparse vectors deterministically.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

DENSE, SPARSE = "dense", "sparse"
BM25_MODEL = "Qdrant/bm25"
RECORDS, CHUNKS = "function_records", "function_chunks"

DEFAULT_STORAGE = "qdrant_store"
DEFAULT_EXPORT = "qdrant_export"
DEFAULT_MODEL = "biobert-go-retrieval"


@dataclass
class Settings:
    """Everything the retriever and the MCP server need to reach the data."""
    storage: str | None = None          # embedded on-disk folder
    url: str | None = None              # or a server
    api_key: str | None = None
    model_path: str = DEFAULT_MODEL
    prefetch_limit: int = 200           # candidates per branch at the record level
    shortlist: int = 100                # records carried from stage 1 into the chunk re-rank
    chunk_prefetch_limit: int = 1000    # candidates per branch at the chunk level

    @classmethod
    def from_env(cls) -> "Settings":
        url = os.environ.get("QDRANT_URL")
        storage = os.environ.get("QDRANT_PATH")
        if not url and not storage:
            storage = DEFAULT_STORAGE
        return cls(
            storage=storage,
            url=url,
            api_key=os.environ.get("QDRANT_API_KEY"),
            model_path=os.environ.get("BIOBERT_MODEL_PATH", DEFAULT_MODEL),
            prefetch_limit=int(os.environ.get("QDRANT_PREFETCH_LIMIT", 200)),
            shortlist=int(os.environ.get("QDRANT_SHORTLIST", 100)),
            chunk_prefetch_limit=int(os.environ.get("QDRANT_CHUNK_PREFETCH_LIMIT", 1000)),
        )


def open_client(storage: str | None = None, url: str | None = None, api_key: str | None = None):
    """Embedded folder or server, never both -- ambiguity here is always a config bug."""
    from qdrant_client import QdrantClient

    if url and storage:
        raise ValueError("Pass either a storage path (embedded) or a url (server), not both.")
    if url:
        return QdrantClient(url=url, api_key=api_key, prefer_grpc=False)
    return QdrantClient(path=storage or DEFAULT_STORAGE)


@lru_cache(maxsize=4)
def load_encoder(model_path: str = DEFAULT_MODEL):
    """The fine-tuned BioBERT. Cached: loading it twice in one process is pure waste."""
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(model_path)


@lru_cache(maxsize=1)
def load_sparse_encoder(model_name: str = BM25_MODEL):
    """
    BM25 term frequencies. Qdrant applies IDF itself (the sparse vector params
    carry Modifier.IDF), so these values are corpus-independent -- which is what
    makes them safe to re-derive on the ETL side instead of shipping them.
    """
    from fastembed import SparseTextEmbedding

    return SparseTextEmbedding(model_name=model_name)


def sparse_vector(sparse_encoder, text: str, *, query: bool = False):
    """
    One fastembed SparseEmbedding -> the models.SparseVector Qdrant wants.

    Note the query/document asymmetry, which is BM25 working as intended:
    documents carry length-normalized term frequencies, queries carry a flat 1.0
    per term, and Qdrant supplies IDF at scoring time.
    """
    from qdrant_client import models

    emb = next(iter(sparse_encoder.query_embed(text) if query else sparse_encoder.embed([text])))
    return models.SparseVector(indices=emb.indices.tolist(), values=emb.values.tolist())


# The BM25 term frequency fastembed stores is length-normalized with FIXED
# hyperparameters -- avg_len is a constant 256.0, not measured from the corpus.
# That is exactly what makes a document's sparse vector reproducible from its
# text alone, and therefore what makes re-deriving on restore equivalent to
# shipping the vectors. It also means the guarantee is only as good as these
# values matching on both machines, so the build stamps them into the manifest
# and restore refuses to proceed if they have drifted.
BM25_PARAM_NAMES = ("k", "b", "avg_len", "language", "disable_stemmer")


def sparse_params(sparse_encoder) -> dict:
    """The BM25 hyperparameters actually in force, plus the fastembed version."""
    import importlib.metadata as md

    inner = getattr(sparse_encoder, "model", sparse_encoder)
    params = {name: getattr(inner, name) for name in BM25_PARAM_NAMES if hasattr(inner, name)}
    try:
        params["fastembed"] = md.version("fastembed")
    except Exception:
        pass
    return params


# --- point identity -----------------------------------------------------
#
# The natural key is the UniProt accession. FunctionData is keyed by
# uniprot_id, and dataset.py deliberately keeps one corpus entry per
# accession rather than collapsing isoforms -- a gene with several mapped
# accessions can have genuinely different function text per accession -- so
# the accession, not the entrez id, is what uniquely identifies a record.
#
# Qdrant will not take it as a point id. Point ids are unsigned 64-bit
# integers or UUIDs, and nothing else: "P04637" is rejected as "not a valid
# UUID", and so is "7157-0". That is a type restriction, not a character
# one. Both the server and the embedded backend enforce it identically, so
# this cannot surprise anyone later by passing in Colab and failing in
# Docker.
#
# So the accession is hashed into a UUID. uuid5 rather than a counter
# because it is a pure function of the accession: any process holding an
# accession computes the point id without a lookup table, which is exactly
# what expand_snippet() needs to jump from a chunk to its parent record.
#
# The hash is one-way, so it can never be the only copy of the identity.
# Search returns points the caller did not name -- a UUID plus a payload --
# and the payload is the only channel that can say which protein was found.
# Hence uniprot_id on both collections' payloads (see build_collections.py),
# where it also carries the shortlist filter and the group_by that
# chunk re-ranking depends on. The UUID is addressing only; it never
# appears in a tool argument, a filter, or a result field.

import uuid  # noqa: E402  (kept next to the functions that use it)


def record_id(uniprot_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, uniprot_id))


def chunk_id(uniprot_id: str, i: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{uniprot_id}#{i}"))
