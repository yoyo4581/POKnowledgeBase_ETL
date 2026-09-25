"""
Measures what chunk re-ranking is worth on top of hybrid search.

The comparison
--------------
Both arms retrieve the same candidate proteins with the same record-level hybrid
query (RRF over fine-tuned BioBERT + BM25). They differ only in the ordering:

    record_hybrid   proteins ordered by whole-record hybrid score   (baseline)
    chunk_rerank    the same proteins re-ordered by their single best
                    matching sentence window                        (arm)

Because the candidate set is shared, every metric delta below is attributable
to the re-ranking step alone. The metrics are the same family
InformationRetrievalEvaluator reports during training (accuracy@k, precision@k,
recall@k, MRR@k, NDCG@k, MAP@k), so a number here is directly comparable to a
number from GO_Contrastive.py.

Queries are GO term names and relevance is "this protein is annotated to this
term", over UniProt accessions. The held-out split is over GO TERMS, not over
proteins -- dataset.py sets eval_corpus = dict(gene_text), the whole corpus --
so the judgments are complete with respect to everything indexed, and the same
store serves the experiment and production.

The terms are the ones the encoder's checkpoint was selected on, so this is an
honest test of the retrieval stack and a generous one for the encoder; it is
not a test of the encoder against unseen ontology.

Search runs through MCP, not through GeneRetriever directly. The tools are
driven over the SDK's in-memory transport: real server object, real tool
schemas, real JSON round-trip, no subprocess. What the evaluation measures is
therefore the same path the application will take.

Why a bootstrap
---------------
A mean NDCG that moves from 0.71 to 0.73 across a few hundred queries is not
self-evidently a real effect. The paired bootstrap at the end resamples queries
to put a confidence interval on the per-query difference, so the result reads
as "chunk re-ranking helps by X, plus or minus Y" rather than as a bare pair of
numbers.

Usage
-----
    python eval_retriever.py \
        --data-dir data/qdrant/go_contrastive \
        --storage qdrant_store \
        --model biobert-go-retrieval \
        --out eval_results
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


import argparse
import asyncio
import json
import statistics
import time
from pathlib import Path

from core.cli import (add_retrieval_args, add_store_args, load_eval_set,
                      open_retriever, require_populated)
from core.config import RECORDS
from core.metrics import (DEFAULT_KS, PRIMARY_METRIC, aggregate, paired_bootstrap,
                         per_query_metrics)
from mcp_server import build_server, is_error, open_session, unwrap



# --- eval set -----------------------------------------------------------

def report_coverage(relevant: dict[str, set], corpus: dict[str, str], indexed: int | None) -> None:
    """
    eval_corpus.json is the WHOLE corpus, not the held-out slice -- dataset.py
    splits on GO terms and sets eval_corpus = dict(gene_text). So the judgments
    here are complete with respect to what is indexed, and the metrics are
    directly comparable to InformationRetrievalEvaluator's.

    Relevant accessions missing from the indexed corpus would cap recall for
    every arm equally, so they could not bias the comparison -- but they would
    explain absolute numbers below 1.0, which is worth knowing before reading
    the table.
    """
    if not corpus:
        return
    all_relevant = set().union(*relevant.values()) if relevant else set()
    missing = all_relevant - set(corpus)
    print(f"Eval set: {len(relevant)} queries, {len(all_relevant)} distinct relevant "
          f"accessions, corpus {len(corpus)} accessions"
          + (f", indexed {indexed}" if indexed is not None else ""))
    if missing:
        print(f"  {len(missing)} relevant accession(s) are absent from the corpus -- "
              f"recall is capped below 1.0 for every arm equally.")


# --- metrics ------------------------------------------------------------

# --- MCP plumbing -------------------------------------------------------

async def run_variant(session, mode: str, queries: dict[str, str], top_k: int,
                      progress_every: int = 25) -> tuple[dict[str, list[str]], list[float]]:
    """One full pass over the eval set through the MCP tool. Returns rankings + latencies."""
    run: dict[str, list[str]] = {}
    latencies: list[float] = []
    started = time.time()
    for i, (qid, text) in enumerate(queries.items(), 1):
        t0 = time.perf_counter()
        result = await session.call_tool("search_proteins", {
            "query": text, "k": top_k, "snippets_per_record": 0, "mode": mode})
        latencies.append(time.perf_counter() - t0)

        if is_error(result):
            raise RuntimeError(f"search_proteins failed on {qid!r} ({mode}): "
                               f"{[getattr(b, 'text', b) for b in (result.content or [])]}")
        run[qid] = [row["uniprot_id"] for row in unwrap(result)["results"]]

        if progress_every and i % progress_every == 0:
            rate = i / (time.time() - started)
            print(f"  {mode}: {i}/{len(queries)} queries ({rate:.1f}/s)", end="\r", flush=True)
    print(f"  {mode}: {len(queries)} queries in {time.time() - started:.1f}s"
          f" (median {statistics.median(latencies) * 1000:.0f} ms/query)")
    return run, latencies


# --- reporting ----------------------------------------------------------

def print_table(summaries: dict[str, dict[str, float]], baseline: str) -> None:
    metrics = list(next(iter(summaries.values())).keys())
    variants = list(summaries)
    width = max(len(m) for m in metrics) + 2

    header = "metric".ljust(width) + "".join(v.rjust(16) for v in variants)
    if baseline in summaries and len(variants) > 1:
        header += "".join(f"{v} delta".rjust(18) for v in variants if v != baseline)
    print("\n" + header)
    print("-" * len(header))
    for m in metrics:
        row = m.ljust(width) + "".join(f"{summaries[v][m]:.4f}".rjust(16) for v in variants)
        if baseline in summaries and len(variants) > 1:
            for v in variants:
                if v == baseline:
                    continue
                d = summaries[v][m] - summaries[baseline][m]
                row += f"{d:+.4f}".rjust(18)
        print(row)


def print_significance(name: str, stats: dict[str, float], metric: str) -> None:
    print(f"\nPaired bootstrap on {metric} ({name} vs baseline, 10k resamples)")
    print(f"  mean delta   {stats['delta']:+.4f}   95% CI [{stats['ci_lo']:+.4f}, {stats['ci_hi']:+.4f}]")
    print(f"  p (2-sided)  {stats['p_two_sided']:.4f}")
    print(f"  queries      {stats['queries_improved']} better / "
          f"{stats['queries_worsened']} worse / {stats['queries_unchanged']} unchanged")
    if stats["ci_lo"] > 0:
        print("  -> the improvement holds across resampling; the effect is real at this sample size.")
    elif stats["ci_hi"] < 0:
        print("  -> chunk re-ranking is reliably WORSE here, not better.")
    else:
        print("  -> the interval spans zero: this eval set cannot distinguish the two arms.")


# --- main ---------------------------------------------------------------

async def evaluate(args) -> dict:
    queries, relevant, corpus = load_eval_set(args.data_dir)
    if args.limit:
        queries = dict(sorted(queries.items())[:args.limit])
        print(f"Limited to {len(queries)} queries.")

    print(f"Loading encoder from {args.model} and opening {args.url or args.storage}...")
    retriever = open_retriever(args)
    counts = require_populated(retriever.client)
    report_coverage(relevant, corpus, counts[RECORDS])

    # One server, one retriever, one encoder, for every variant.
    runs, latencies = {}, {}
    server = build_server(retriever=retriever)
    async with open_session(server) as session:
        tools = {t.name for t in (await session.list_tools()).tools}
        print(f"MCP session up; tools exposed: {', '.join(sorted(tools))}")
        for mode in args.variants:
            runs[mode], latencies[mode] = await run_variant(session, mode, queries, args.top_k)

    ks = tuple(args.ks)
    scores = {
        mode: {qid: per_query_metrics(ranked, relevant[qid], ks, args.map_k)
               for qid, ranked in run.items()}
        for mode, run in runs.items()
    }
    summaries = {mode: aggregate(s) for mode, s in scores.items()}

    print_table(summaries, args.baseline)
    print("\nLatency (median ms/query): " + ", ".join(
        f"{m} {statistics.median(v) * 1000:.0f}" for m, v in latencies.items()))

    significance = {}
    if args.baseline in scores:
        qids = list(queries)
        base = [scores[args.baseline][q][args.metric] for q in qids]
        for mode in args.variants:
            if mode == args.baseline:
                continue
            arm = [scores[mode][q][args.metric] for q in qids]
            significance[mode] = paired_bootstrap(arm, base, seed=args.seed)
            print_significance(mode, significance[mode], args.metric)

    results = {
        "config": {
            "data_dir": str(args.data_dir), "model": args.model,
            "target": args.url or args.storage, "variants": list(args.variants),
            "baseline": args.baseline, "top_k": args.top_k, "ks": list(ks),
            "map_k": args.map_k, "metric": args.metric,
            "prefetch_limit": args.prefetch_limit, "shortlist": args.shortlist,
            "chunk_prefetch_limit": args.chunk_prefetch_limit,
            "n_queries": len(queries), "via": "mcp/in-memory",
        },
        "summary": summaries,
        "significance": significance,
        "latency_ms_median": {m: statistics.median(v) * 1000 for m, v in latencies.items()},
    }

    if args.out:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / "summary.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
        # Per-query rankings and scores, so a surprising aggregate can be traced
        # back to the queries that produced it.
        with open(out / "per_query.jsonl", "w", encoding="utf-8") as f:
            for qid in queries:
                f.write(json.dumps({
                    "qid": qid, "query": queries[qid],
                    "n_relevant": len(relevant[qid]),
                    "runs": {m: runs[m][qid][:args.top_k] for m in args.variants},
                    "scores": {m: scores[m][qid] for m in args.variants},
                }, ensure_ascii=False) + "\n")
        print(f"\nWrote {out}/summary.json and {out}/per_query.jsonl")

    retriever.client.close()
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", default="data/qdrant/go_contrastive",
                        help="Directory holding eval_queries.json / eval_relevant.json / eval_corpus.json.")
    add_store_args(parser)
    parser.add_argument("--variants", nargs="+", default=["record_hybrid", "chunk_rerank"],
                        help="Search modes to compare (default: the baseline and the arm).")
    parser.add_argument("--baseline", default="record_hybrid",
                        help="Variant the deltas and the bootstrap are computed against.")
    parser.add_argument("--metric", default=PRIMARY_METRIC,
                        help=f"Metric the bootstrap runs on (default: {PRIMARY_METRIC}).")
    parser.add_argument("--top-k", type=int, default=100,
                        help="Ranked depth retrieved per query; must be >= max(--ks) and --map-k.")
    parser.add_argument("--ks", type=int, nargs="+", default=list(DEFAULT_KS),
                        help="Cutoffs for accuracy/precision/recall/mrr/ndcg (default: 1 3 5 10).")
    parser.add_argument("--map-k", type=int, default=100, help="Cutoff for MAP (default: 100).")
    add_retrieval_args(parser)
    parser.add_argument("--limit", type=int, help="Only the first N queries -- for smoke tests.")
    parser.add_argument("--seed", type=int, default=0, help="Bootstrap seed.")
    parser.add_argument("--out", default="eval_results", help="Directory for summary.json + per_query.jsonl.")
    args = parser.parse_args()

    if args.top_k < max(max(args.ks), args.map_k):
        raise SystemExit(f"--top-k {args.top_k} is below max(--ks)={max(args.ks)} / --map-k={args.map_k}; "
                         f"metrics past the retrieved depth would be silently truncated.")
    chunk_modes = [v for v in args.variants if v.startswith("chunk")]
    if chunk_modes and args.shortlist < args.top_k:
        raise SystemExit(f"--shortlist {args.shortlist} < --top-k {args.top_k}: "
                         f"{', '.join(chunk_modes)} can only return proteins from the shortlist, so "
                         f"they would be scored on a shorter ranked list than the baseline. Raise "
                         f"--shortlist to at least --top-k.")

    asyncio.run(evaluate(args))


if __name__ == "__main__":
    main()
