"""
Does the encoder tell sentences of the SAME record apart?

Chunk re-ranking orders records by their single best-matching passage. That can
only work if the encoder scores a record's own chunks differently from each
other. Cross-record ranking -- which every metric in eval_retriever.py reports,
and which this model is demonstrably decent at -- is a different capability and
says nothing about this one.

If within-record resolution is absent, chunk re-ranking has nothing to rank by,
and no (window, stride) setting recovers it. That makes this the measurement to
take before sweeping chunking parameters: it decides whether the next move is a
parameter search or a retrain.

Two deliberate choices
----------------------
DENSE ONLY. BM25 separates sentences trivially -- different sentences contain
different terms -- so including the sparse branch would mask the dense
encoder's behaviour, which is the thing in question. Scores here are raw cosine
similarities from the dense vector alone, not RRF-fused ranks.

A CONTROL QUERY. An absolute spread is uninterpretable: 0.04 might be large or
negligible depending on the embedding geometry. So every record is also scored
against an unrelated query's vector. If the true query produces no more spread
than an unrelated one, the variation is geometry, not signal -- the encoder is
not responding to this query's content at the sentence level. That comparison
needs no threshold chosen in advance.

What the numbers mean
---------------------
  ratio ~= 1.0   The true query separates a record's sentences no better than a
                 random query does. The encoder has no within-record resolution.
                 Chunk re-ranking cannot work, and this is a model problem, not
                 a chunking-parameter problem.

  ratio >> 1.0   Real, query-specific within-record structure exists. A null
                 retrieval result then points at the corpus or the parameters,
                 and a (window, stride) sweep is worth running.

  best-chunk lift  max(sim(q, chunk)) - sim(q, whole record). Positive means
                 isolating a passage genuinely improves the match -- the
                 localization premise behind chunking. Around zero means the
                 whole-record embedding already captures whatever the best
                 passage captures.

Usage
-----
    python discrimination_test.py --storage /content/qdrant_store \
        --model biobert-go-retrieval-v2-FATokens \
        --data-dir data/qdrant/go_contrastive
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


import argparse
import json
import statistics
from pathlib import Path

from qdrant_client import models

from core.cli import add_store_args, load_queries
from core.config import CHUNKS, DENSE, RECORDS, load_encoder, open_client


def pct(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    return s[min(len(s) - 1, int(p * len(s)))]


def describe(name: str, values: list[float]) -> None:
    if not values:
        print(f"  {name:<34} (no data)")
        return
    print(f"  {name:<34} mean {statistics.mean(values):+.4f}   "
          f"median {statistics.median(values):+.4f}   p90 {pct(values, 0.90):+.4f}")


def chunk_scores(client, dq, accessions: list[str], expected: int) -> dict[str, list[float]]:
    """
    Dense-only cosine similarity of `dq` against every chunk of `accessions`.

    limit is the exact total chunk count: truncating would drop the lowest
    scorers first and shrink the measured spread, biasing the whole test toward
    "no discrimination" -- the conclusion it is supposed to be able to reject.
    """
    if not accessions:
        return {}
    flt = models.Filter(must=[models.FieldCondition(
        key="uniprot_id", match=models.MatchAny(any=accessions))])
    hits = client.query_points(
        CHUNKS, query=dq, using=DENSE, query_filter=flt,
        limit=expected, with_payload=["uniprot_id"],
    ).points
    out: dict[str, list[float]] = {}
    for h in hits:
        out.setdefault(h.payload["uniprot_id"], []).append(h.score)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-dir", default="data/qdrant/go_contrastive")
    add_store_args(p)
    p.add_argument("--shortlist", type=int, default=50,
                   help="Records examined per query (default: 50).")
    p.add_argument("--limit", type=int, default=150,
                   help="Queries to sample (default: 150). The estimate is stable well "
                        "before the full eval set.")
    p.add_argument("--out", help="Optional JSON file for the raw numbers.")
    args = p.parse_args()

    queries = load_queries(args.data_dir)
    qids = sorted(queries)[:args.limit]
    print(f"Loading {args.model} and opening {args.url or args.storage}...")

    client = open_client(storage=args.storage, url=args.url, api_key=args.api_key)
    encoder = load_encoder(args.model)

    if not client.collection_exists(CHUNKS):
        raise SystemExit(f"{CHUNKS} missing at {args.url or args.storage}.")

    # The control pairs an unrelated query with the TRUE query's shortlist. If
    # the shortlist covers most of the collection, every query retrieves the
    # same records and the control's (query, record) pairs become a permutation
    # of the true ones -- which forces the two means to coincide and the ratio
    # to 1.00 regardless of the encoder. Guard against reading that as a result.
    total_records = client.get_collection(RECORDS).points_count
    if args.shortlist >= 0.5 * total_records:
        raise SystemExit(
            f"--shortlist {args.shortlist} covers most of the {total_records}-record "
            f"collection, so the control degenerates into a permutation of the true "
            f"condition and the ratio is 1.00 by construction. Use a shortlist well "
            f"below half the collection.")

    vectors = [encoder.encode(queries[q], normalize_embeddings=True,
                              show_progress_bar=False).tolist() for q in qids]
    print(f"{len(qids)} queries encoded.\n")

    within_true, within_ctrl, across, lift = [], [], [], []
    n_records = n_skipped = 0

    for i, qid in enumerate(qids):
        dq = vectors[i]
        # An unrelated query, paired deterministically. Same records, different
        # question -- so any spread it produces is baseline geometry.
        dq_ctrl = vectors[(i + len(vectors) // 2) % len(vectors)]

        recs = client.query_points(
            RECORDS, query=dq, using=DENSE, limit=args.shortlist,
            with_payload=["uniprot_id", "n_chunks"],
        ).points
        multi = [r for r in recs if (r.payload.get("n_chunks") or 0) > 1]
        n_skipped += len(recs) - len(multi)
        if not multi:
            continue

        accs = [r.payload["uniprot_id"] for r in multi]
        record_sim = {r.payload["uniprot_id"]: r.score for r in multi}
        expected = sum(r.payload["n_chunks"] for r in multi)

        true_scores = chunk_scores(client, dq, accs, expected)
        ctrl_scores = chunk_scores(client, dq_ctrl, accs, expected)

        best_per_record = []
        for acc in accs:
            ts = true_scores.get(acc, [])
            cs = ctrl_scores.get(acc, [])
            if len(ts) > 1:
                within_true.append(max(ts) - min(ts))
                best_per_record.append(max(ts))
                lift.append(max(ts) - record_sim[acc])
                n_records += 1
            if len(cs) > 1:
                within_ctrl.append(max(cs) - min(cs))
        if len(best_per_record) > 1:
            across.append(max(best_per_record) - min(best_per_record))

        if (i + 1) % 25 == 0:
            print(f"  {i + 1}/{len(qids)} queries, {n_records} records examined", flush=True)

    client.close()

    if not within_true:
        raise SystemExit("No multi-chunk records were retrieved -- every record in every "
                         "shortlist has a single chunk. Rebuild with a smaller --window.")

    print(f"\n{'=' * 72}")
    print("DISCRIMINATION DIAGNOSTIC  (dense encoder only, BM25 excluded by design)")
    print(f"{'=' * 72}")
    print(f"{len(qids)} queries, {n_records} multi-chunk records examined, "
          f"{n_skipped} single-chunk records skipped (they cannot vary).\n")

    describe("within-record spread, TRUE query", within_true)
    describe("within-record spread, control", within_ctrl)
    describe("across-record spread (best chunks)", across)
    describe("best chunk minus whole record", lift)

    t, c = statistics.mean(within_true), statistics.mean(within_ctrl)
    if c > 1e-9:
        ratio = t / c
    elif t > 1e-9:
        ratio = float("inf")          # control flat, true query separates: ideal
    else:
        ratio = 1.0                   # both flat: no within-record structure at all
    positive = 100 * sum(1 for x in lift if x > 0) / len(lift)

    print(f"\n  ratio true/control            {ratio:.2f}")
    print(f"  records where best chunk beats the whole record   {positive:.0f}%")

    print("\nReading it:")
    if ratio < 1.15:
        print("  The true query separates a record's sentences barely better than an")
        print("  unrelated one. There is no query-specific within-record structure for")
        print("  chunk re-ranking to exploit. This is the encoder, not the chunking")
        print("  parameters -- a (window, stride) sweep would only produce a more")
        print("  rigorous null. Fix the fine-tuning first.")
    elif ratio < 1.6:
        print("  Weak but real within-record structure. Chunk re-ranking has something")
        print("  to work with, though not much. Worth a parameter sweep on a stratified")
        print("  multi-sentence subset, with modest expectations.")
    else:
        print("  Clear query-specific within-record structure. The encoder does resolve")
        print("  sentences. A null retrieval result points at the corpus or the")
        print("  parameters, so the sweep is the right next step.")
    if positive < 40:
        print("\n  Separately: isolating the best passage rarely beats the whole-record")
        print("  match, so the localization premise itself is not holding on this data.")

    if args.out:
        Path(args.out).write_text(json.dumps({
            "queries": len(qids), "records": n_records,
            "within_true_mean": t, "within_control_mean": c, "ratio": ratio,
            "across_mean": statistics.mean(across) if across else None,
            "lift_mean": statistics.mean(lift), "lift_positive_pct": positive,
        }, indent=2), encoding="utf-8")
        print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
