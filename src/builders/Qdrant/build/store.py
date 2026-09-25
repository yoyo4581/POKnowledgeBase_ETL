"""
Builds the two collections the hybrid + chunk-rerank experiment runs on, and
writes them out in two formats so the Colab -> download -> ETL hop is lossless.

    function_records   one point per UniProt accession: the whole function
                       text. Payload carries the split `sentences` list, which
                       is what expand_snippet() widens a chunk against.
    function_chunks    one point per sentence window, tagged with its
                       accession and its [sent_start, sent_end) span.

The unit is the protein, not the gene: FunctionData is keyed by uniprot_id and
dataset.py keeps isoforms as separate corpus entries on purpose, because their
function text genuinely differs. entrez_id rides along on records as the key
back to Neo4j, and is not unique among them.

Both carry a dense vector (fine-tuned BioBERT) and a BM25 sparse vector, so
either collection can be queried hybrid.

Two artifacts, because one format cannot do both jobs:

    --storage DIR   an embedded QdrantClient(path=...) folder. Zip it,
                    download it, query it anywhere with zero infrastructure.
                    It is not loadable into a Qdrant server.
    --export DIR    dense vectors as .npy + payloads as .jsonl + a config
                    manifest. `restore` replays this into any target,
                    embedded or server, which is the ETL path.

Sparse vectors are deliberately NOT exported. Qdrant applies IDF itself (the
sparse params carry Modifier.IDF), so what is stored per point is just BM25
term frequencies -- a pure function of the payload text. Re-deriving them on
restore is exactly equivalent to shipping them, and costs nothing to keep in
sync.

Usage
-----
    # eval_corpus.json is the whole corpus, so this one store serves both the
    # chunk-rerank experiment and production.
    python build/store.py build
        --corpus data/qdrant/go_contrastive/eval_corpus.json
        --model biobert-go-retrieval
        --storage qdrant_store --export qdrant_export
        [--entrez-map entrez_to_uniprots.json]

    # then zip the store and export folders and download them

    # in the ETL pipeline, into a real server
    python build/store.py restore --export qdrant_export --url http://localhost:6333
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


import argparse
import json
import time
import warnings
from pathlib import Path

import numpy as np
from qdrant_client import models

from core.chunking import SPLITTER_NAME, chunk_text
from core.config import (BM25_MODEL, CHUNKS, DEFAULT_EXPORT, DEFAULT_MODEL, DEFAULT_STORAGE,
                    DENSE, RECORDS, SPARSE, chunk_id, load_encoder, load_sparse_encoder,
                    open_client, record_id, sparse_params)

UPSERT_BATCH = 256


# --- corpus loading -----------------------------------------------------
#
# eval_corpus.json is {uniprot_id: text} and, despite the name, it is the
# WHOLE corpus, not the held-out slice. build_dataset() splits on GO terms,
# not on proteins:
#
#     eval_queries  = {t: name for t in test}      # held-out terms
#     eval_relevant = {t: pos[t] for t in test}    # held-out judgments
#     eval_corpus   = dict(gene_text)              # every protein with text
#
# Every text in train.jsonl is gene_text[g] for some g, so the triplets add
# no protein the corpus does not already hold. One source is all there is,
# and it serves both the experiment and production.
#
# The key is a UniProt accession, not a gene. _load_protein_data() says so
# outright: FunctionData is keyed by uniprot_id, EntrezUniprotMap only joins
# the two, and annotations are expanded onto every accession under a gene
# precisely so isoforms with different function text stay distinct records.

def load_corpus(path: str | Path) -> dict[str, str]:
    """
    eval_corpus.json ({uniprot_id: text}), or a .jsonl of
    {"uniprot_id": ..., "text": ...} rows. utf-8-sig throughout, to match how
    dataset.py's own importer reads the same files.
    """
    path = Path(path)
    if path.suffix == ".jsonl":
        corpus = {}
        with open(path, encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if line:
                    row = json.loads(line)
                    corpus[row.get("uniprot_id") or row["gene"]] = row["text"]
    else:
        corpus = json.loads(path.read_text(encoding="utf-8-sig"))
    return {u: t for u, t in corpus.items() if isinstance(t, str) and t.strip()}


def load_entrez_map(path: str | Path) -> dict[str, str]:
    """
    Optional {uniprot_id: entrez_id} enrichment.

    Accepts either direction, because the ETL side has it as
    entrez_to_uniprots ({entrez_id: [uniprot_id, ...]}) -- that is the shape
    _load_protein_data() builds -- while the payload wants the inverse. A
    list-valued file is treated as the entrez->accessions direction and
    inverted; a string-valued one is taken as already accession-keyed.

    Not required. eval_corpus.json carries no entrez ids (export_dataset()
    never writes entrez_to_uniprots), so a store can be built today without
    this and enriched on a later rebuild. What it buys is the join back to
    Neo4j, where annotations live at entrez granularity.
    """
    raw = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not raw:
        return {}
    if isinstance(next(iter(raw.values())), list):
        return {u: str(e) for e, us in raw.items() for u in us}
    return {u: str(e) for u, e in raw.items()}


def merge_corpora(sources: list[tuple[str, dict[str, str]]], limit: int | None = None
                  ) -> dict[str, str]:
    """
    Union of several {uniprot_id: text} sources, first source winning.

    One source is the normal case. This exists for the day the corpus arrives
    in pieces, and mainly to catch the failure that would otherwise be silent:
    two sources holding DIFFERENT text for the same accession, which means they
    were exported from different states of the knowledge base.
    """
    merged: dict[str, str] = {}
    origin: dict[str, str] = {}
    conflicts: list[tuple[str, str, str]] = []

    for name, corpus in sources:
        new = 0
        for uid, text in corpus.items():
            if uid not in merged:
                merged[uid], origin[uid] = text, name
                new += 1
            elif merged[uid] != text:
                conflicts.append((uid, origin[uid], name))
        print(f"  {name}: {len(corpus)} accessions ({new} new, {len(corpus) - new} already present)")

    if conflicts:
        shown = ", ".join(f"{u} ({a} vs {b})" for u, a, b in conflicts[:5])
        print(f"  WARNING: {len(conflicts)} accession(s) have different text in different "
              f"sources; keeping the first. e.g. {shown}")
        print("  That usually means the sources came from different states of the knowledge "
              "base -- re-export them together if so.")

    if limit:
        merged = dict(sorted(merged.items())[:limit])
    return merged


# --- shaping ------------------------------------------------------------

def shape(corpus: dict[str, str], window: int, stride: int,
          entrez: dict[str, str] | None = None) -> tuple[list[dict], list[dict]]:
    """
    corpus -> (record payloads, chunk payloads), in a stable accession order so
    that two builds of the same corpus produce byte-identical exports.

    uniprot_id goes on BOTH payloads. On records it is the identity a search
    result reports back; on chunks it additionally carries the shortlist filter
    and the group_by that chunk re-ranking is built on -- neither of which can
    read a point id.

    entrez_id goes on records only, when a mapping is supplied. Chunks never
    need it: they only ever join to their parent record, and that join is the
    accession. It is the key back to Neo4j, where annotations live at gene
    granularity -- and it is deliberately NOT unique per record, since several
    isoforms of one gene are several records here.
    """
    entrez = entrez or {}
    records, chunks = [], []
    for uid in sorted(corpus):
        sentences, uid_chunks = chunk_text(corpus[uid], window=window, stride=stride)
        record = {"uniprot_id": uid, "text": corpus[uid],
                  "sentences": sentences, "n_chunks": len(uid_chunks)}
        if uid in entrez:
            record["entrez_id"] = entrez[uid]
        records.append(record)
        for ch in uid_chunks:
            chunks.append({"uniprot_id": uid, **ch})
    return records, chunks


# --- writing ------------------------------------------------------------

def ensure_collection(client, name: str, dim: int, on_disk: bool, recreate: bool) -> None:
    if client.collection_exists(name):
        if not recreate:
            raise SystemExit(
                f"Collection {name!r} already exists. Pass --recreate to drop and rebuild it, "
                f"or point --storage at a fresh directory.")
        client.delete_collection(name)

    client.create_collection(
        name,
        vectors_config={DENSE: models.VectorParams(
            size=dim, distance=models.Distance.COSINE, on_disk=on_disk)},
        # Modifier.IDF is what makes the stored term frequencies behave as BM25
        # at query time. Without it, sparse scoring silently degrades to raw TF.
        sparse_vectors_config={SPARSE: models.SparseVectorParams(
            modifier=models.Modifier.IDF)},
    )
    # On a server this is what keeps the chunk stage's MatchAny filter (one
    # clause per shortlisted accession, on every query) from degenerating into a full
    # scan of the chunk collection. Correctness does not depend on it; latency
    # very much does, and the chunk collection is the larger of the two.
    # The embedded backend scans regardless and warns that the call is a no-op.
    # Swallow that noise rather than skipping the call: the same code path has
    # to produce a properly indexed collection when restore targets a server.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*[Pp]ayload indexes have no effect.*")
        client.create_payload_index(name, field_name="uniprot_id",
                                    field_schema=models.PayloadSchemaType.KEYWORD)
        if name == RECORDS:
            # Not used by retrieval, but the application filters by gene when
            # ontology annotations drive the ranking, and that is a server-side
            # scan without it.
            client.create_payload_index(name, field_name="entrez_id",
                                        field_schema=models.PayloadSchemaType.KEYWORD)


def encode(encoder, sparse_encoder, payloads: list[dict], batch_size: int, label: str):
    texts = [p["text"] for p in payloads]
    print(f"  encoding {len(texts)} {label} (dense)...", flush=True)
    dense = encoder.encode(texts, batch_size=batch_size, normalize_embeddings=True,
                           show_progress_bar=True, convert_to_numpy=True).astype(np.float32)
    print(f"  encoding {len(texts)} {label} (bm25)...", flush=True)
    sparse = list(sparse_encoder.embed(texts, batch_size=batch_size))
    return dense, sparse


def upsert(client, name: str, payloads: list[dict], dense, sparse, ids: list[str],
           progress_every: int = 20) -> None:
    for batch_i, start in enumerate(range(0, len(payloads), UPSERT_BATCH)):
        stop = min(start + UPSERT_BATCH, len(payloads))
        client.upsert(name, points=[
            models.PointStruct(
                id=ids[i],
                vector={DENSE: dense[i].tolist(),
                        SPARSE: models.SparseVector(indices=sparse[i].indices.tolist(),
                                                    values=sparse[i].values.tolist())},
                payload=payloads[i],
            ) for i in range(start, stop)
        ], wait=True)
        # A repainting "\r" line renders as nothing in Jupyter/Colab until the
        # loop ends, which makes a slow upsert look hung. Emit a real line
        # every so often instead.
        if batch_i % progress_every == 0 or stop == len(payloads):
            print(f"  upserted {stop}/{len(payloads)} into {name}", flush=True)


# --- export / restore ---------------------------------------------------

def write_export(export_dir: Path, manifest: dict,
                 records: list[dict], rec_dense, chunks: list[dict], ch_dense) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)
    for name, payloads, dense in ((RECORDS, records, rec_dense), (CHUNKS, chunks, ch_dense)):
        np.save(export_dir / f"{name}.dense.npy", dense)
        with open(export_dir / f"{name}.payloads.jsonl", "w", encoding="utf-8") as f:
            for p in payloads:
                f.write(json.dumps(p, ensure_ascii=False) + "\n")
    (export_dir / "collection_config.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote portable export to {export_dir}/ "
          f"({len(records)} records, {len(chunks)} chunks)")


def read_export(export_dir: Path):
    manifest = json.loads((export_dir / "collection_config.json").read_text(encoding="utf-8-sig"))
    out = {}
    for name in (RECORDS, CHUNKS):
        payloads = []
        with open(export_dir / f"{name}.payloads.jsonl", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if line:
                    payloads.append(json.loads(line))
        dense = np.load(export_dir / f"{name}.dense.npy")
        if len(payloads) != len(dense):
            raise SystemExit(f"{name}: {len(payloads)} payloads but {len(dense)} vectors -- "
                             f"the export is inconsistent, rebuild it.")
        out[name] = (payloads, dense)
    return manifest, out


def check_bm25_params(manifest: dict, sparse_encoder, allow_drift: bool = False) -> None:
    """
    Restore re-derives sparse vectors instead of shipping them, which is only
    equivalent while the BM25 hyperparameters match the build. A drifted
    fastembed default would not raise anything -- it would just produce slightly
    different term weights, leaving hybrid search quietly worse than the version
    that was evaluated. Fail loudly instead.
    """
    expected, actual = manifest.get("bm25_params"), sparse_params(sparse_encoder)
    if not expected:
        print("  note: this export predates bm25_params; sparse weights cannot be verified.")
        return
    drift = {k: (v, actual.get(k)) for k, v in expected.items()
             if k != "fastembed" and actual.get(k) != v}
    if expected.get("fastembed") != actual.get("fastembed"):
        print(f"  note: built with fastembed {expected.get('fastembed')}, "
              f"restoring with {actual.get('fastembed')}.")
    if not drift:
        return
    detail = ", ".join(f"{k}: built={b!r} now={n!r}" for k, (b, n) in drift.items())
    if allow_drift:
        print(f"  WARNING: BM25 parameters differ from the build ({detail}). "
              f"Sparse scoring will not match what was evaluated.")
        return
    raise SystemExit(
        f"BM25 parameters differ from the build ({detail}). Re-derived sparse vectors would "
        f"not match the ones the evaluation measured. Pin fastembed to the build version, or "
        f"pass --allow-param-drift if you accept the difference.")


def ids_for(name: str, payloads: list[dict]) -> list[str]:
    """
    Point ids, recomputed from the payload rather than stored beside it -- which
    is why uniprot_id being on the payload is what makes the export restorable
    at all.
    """
    if name == RECORDS:
        return [record_id(p["uniprot_id"]) for p in payloads]
    return [chunk_id(p["uniprot_id"], p["chunk_idx"]) for p in payloads]


# --- commands -----------------------------------------------------------

def cmd_build(args) -> None:
    print("Assembling corpus:")
    corpus = merge_corpora([(str(c), load_corpus(c)) for c in args.corpus], limit=args.limit)
    if not corpus:
        raise SystemExit("No usable texts in any source.")

    entrez = load_entrez_map(args.entrez_map) if args.entrez_map else {}
    if args.entrez_map:
        unmapped = [u for u in corpus if u not in entrez]
        print(f"  {args.entrez_map}: entrez ids for {len(corpus) - len(unmapped)}/"
              f"{len(corpus)} accessions")
        if unmapped:
            # Worth stating plainly, because these records behave differently
            # from the rest and nothing downstream will say so.
            #
            # dataset.py builds protein_rows from every FunctionData row, but
            # expands GO annotations only onto accessions found in
            # EntrezUniprotMap:
            #
            #     annotations = [... for uniprot_id in entrez_to_uniprots.get(a["gene"], ())]
            #
            # An accession missing from that map therefore lands in the corpus
            # with no annotations at all -- never in pos[t], so never in
            # eval_relevant for any term, so never a correct answer. It can
            # still be retrieved, and it is still in all_genes, the pool random
            # training negatives are drawn from.
            print(f"  {len(unmapped)} accession(s) have function text but no entrez id. "
                  f"They are indexed and searchable, but they carry no GO annotations in "
                  f"the KB, so they can never be relevant to an eval query -- they are "
                  f"structural distractors that lower absolute metrics without affecting "
                  f"the between-arm delta. Absent entrez_id is the marker for them.")
            print(f"  e.g. {', '.join(sorted(unmapped)[:5])}")
    print(f"Corpus: {len(corpus)} UniProt accessions")

    records, chunks = shape(corpus, args.window, args.stride, entrez)
    per_record = len(chunks) / len(records)
    print(f"Shaped {len(records)} records -> {len(chunks)} chunks "
          f"(window={args.window}, stride={args.stride}, {per_record:.1f} chunks/record)")

    encoder = load_encoder(args.model)
    sparse_encoder = load_sparse_encoder()
    # sentence-transformers 6 renamed this; the old name still works but warns.
    dim = (encoder.get_embedding_dimension() if hasattr(encoder, "get_embedding_dimension")
           else encoder.get_sentence_embedding_dimension())

    started = time.time()
    rec_dense, rec_sparse = encode(encoder, sparse_encoder, records, args.batch_size, "records")
    ch_dense, ch_sparse = encode(encoder, sparse_encoder, chunks, args.batch_size, "chunks")
    print(f"Encoded everything in {time.time() - started:.0f}s")

    manifest = {
        "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "corpus_sources": [str(c) for c in args.corpus],
        # What a point is keyed by, so a consumer never has to infer it.
        "record_key": "uniprot_id",
        "point_id": "uuid5(NAMESPACE_URL, uniprot_id)",
        "entrez_map": str(args.entrez_map) if args.entrez_map else None,
        "n_entrez_ids": sum(1 for r in records if "entrez_id" in r),
        "model": args.model,
        "dim": dim,
        "distance": "Cosine",
        "sparse_model": BM25_MODEL,
        "sparse_modifier": "IDF",
        "sparse_vectors": "re-derived from payload text on restore (BM25 TF is corpus-independent)",
        "bm25_params": sparse_params(sparse_encoder),
        "splitter": SPLITTER_NAME,
        "window": args.window,
        "stride": args.stride,
        "n_records": len(records),
        "n_chunks": len(chunks),
    }

    if not args.no_store:
        client = open_client(storage=args.storage)
        try:
            for name, payloads, dense, sparse in (
                    (RECORDS, records, rec_dense, rec_sparse),
                    (CHUNKS, chunks, ch_dense, ch_sparse)):
                ensure_collection(client, name, dim, args.on_disk, args.recreate)
                upsert(client, name, payloads, dense, sparse, ids_for(name, payloads))
        finally:
            client.close()          # local mode holds a lock on the folder
        (Path(args.storage) / "collection_config.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"Built embedded store at {args.storage}/ -- zip this folder to move it")

    if not args.no_export:
        write_export(Path(args.export), manifest, records, rec_dense, chunks, ch_dense)


def cmd_restore(args) -> None:
    manifest, data = read_export(Path(args.export))
    print(f"Restoring export built {manifest['created']} "
          f"(model={manifest['model']}, dim={manifest['dim']}, "
          f"window={manifest['window']}/stride={manifest['stride']}, "
          f"splitter={manifest['splitter']})")

    sparse_encoder = load_sparse_encoder()
    check_bm25_params(manifest, sparse_encoder, allow_drift=args.allow_param_drift)
    client = open_client(storage=args.storage, url=args.url, api_key=args.api_key)
    try:
        for name in (RECORDS, CHUNKS):
            payloads, dense = data[name]
            ensure_collection(client, name, manifest["dim"], args.on_disk, args.recreate)
            print(f"  re-deriving bm25 for {len(payloads)} {name}...", flush=True)
            sparse = list(sparse_encoder.embed([p["text"] for p in payloads],
                                               batch_size=args.batch_size))
            upsert(client, name, payloads, dense, sparse, ids_for(name, payloads))
    finally:
        client.close()
    print(f"Restored into {args.url or args.storage}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    build = sub.add_parser("build", help="Encode a corpus into both collections + both artifacts.")
    build.add_argument("--corpus", required=True, nargs="+",
                       help="eval_corpus.json ({uniprot_id: text}) -- which despite its name is "
                            "the WHOLE corpus, since build_dataset() splits on GO terms, not on "
                            "proteins. Several sources may be given; earlier ones win.")
    build.add_argument("--entrez-map",
                       help="Optional JSON adding entrez_id to record payloads. Takes either "
                            "{entrez_id: [uniprot_id, ...]} (the shape _load_protein_data builds) "
                            "or {uniprot_id: entrez_id}. Not needed to build -- eval_corpus.json "
                            "has no entrez ids -- but it is the join back to Neo4j, where "
                            "annotations live at gene granularity.")
    build.add_argument("--model", default=DEFAULT_MODEL,
                       help="Path to the fine-tuned BioBERT saved by GO_Contrastive.py.")
    build.add_argument("--storage", default=DEFAULT_STORAGE,
                       help="Embedded on-disk store to write (the folder you zip and download).")
    build.add_argument("--export", default=DEFAULT_EXPORT,
                       help="Portable export dir (.npy + .jsonl + manifest) for the ETL pipeline.")
    build.add_argument("--window", type=int, default=3, help="Sentences per chunk (default: 3).")
    build.add_argument("--stride", type=int, default=2,
                       help="Sentence step between chunks (default: 2). Lower = finer localization, "
                            "more chunks, more encoding time.")
    build.add_argument("--batch-size", type=int, default=64, help="Encoder batch size (default: 64).")
    build.add_argument("--limit", type=int, help="Only the first N accessions -- for smoke tests.")
    build.add_argument("--on-disk", action="store_true",
                       help="Keep dense vectors on disk instead of in RAM (slower, smaller footprint).")
    build.add_argument("--recreate", action="store_true", help="Drop existing collections first.")
    build.add_argument("--no-store", action="store_true", help="Skip the embedded store.")
    build.add_argument("--no-export", action="store_true", help="Skip the portable export.")
    build.set_defaults(func=cmd_build)

    restore = sub.add_parser("restore", help="Replay a portable export into a server or a new store.")
    restore.add_argument("--export", default=DEFAULT_EXPORT, help="Export dir written by build.")
    restore.add_argument("--url", help="Target Qdrant server, e.g. http://localhost:6333.")
    restore.add_argument("--api-key", help="API key, if the target needs one.")
    restore.add_argument("--storage", help="Or a target embedded store folder.")
    restore.add_argument("--batch-size", type=int, default=64)
    restore.add_argument("--on-disk", action="store_true")
    restore.add_argument("--recreate", action="store_true")
    restore.add_argument("--allow-param-drift", action="store_true",
                         help="Proceed even if the BM25 hyperparameters no longer match the build. "
                              "Sparse scoring will differ from what was evaluated.")
    restore.set_defaults(func=cmd_restore)

    args = parser.parse_args()
    if args.cmd == "restore" and not args.url and not args.storage:
        raise SystemExit("restore needs a target: pass --url or --storage.")
    args.func(args)


if __name__ == "__main__":
    main()
