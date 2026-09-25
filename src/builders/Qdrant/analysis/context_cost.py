"""
How many tokens does an agent actually read, with snippets versus whole records?

This is the claim the chunk collection rests on once chunk re-ranking is set
aside: retrieve with record_hybrid (the measured-best ordering), but hand the
agent the best-matching passages instead of the full function text. Retrieval
quality is identical by construction -- the ordering is untouched -- so the only
question is what it costs to read, and that had never been measured.

The saving is not uniform and the breakdown matters more than the headline. A
single-chunk record's snippet IS its record text, so short records save exactly
nothing; every token saved comes from the long tail. With this corpus's median
of 3 sentences, a small mean reduction over a large tail reduction is the
expected shape, and which number you quote depends on whether you care about
the average query or the worst one.

Counts use the model's own tokenizer where transformers can load it, since
"tokens" for a context budget means that tokenizer's tokens, not words.

Usage
-----
    python context_cost.py --storage /content/qdrant_store \
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

from core.cli import add_store_args, load_queries, open_retriever
from core.config import RECORDS, record_id

BUCKETS = ((1, 1), (2, 3), (4, 7), (8, 10**9))


def make_counter(model_path: str):
    """The model's tokenizer if it loads, else ~4 chars/token."""
    try:
        from transformers import AutoTokenizer
        tok = AutoTokenizer.from_pretrained(model_path)
        return (lambda t: len(tok(t, add_special_tokens=False)["input_ids"]),
                "model tokenizer")
    except Exception as e:
        print(f"  (tokenizer unavailable -- {type(e).__name__}; "
              f"falling back to chars/4)")
        return (lambda t: max(1, round(len(t) / 4)), "chars/4 estimate")


def bucket_of(n: int) -> str:
    for lo, hi in BUCKETS:
        if lo <= n <= hi:
            return f"{lo}" if lo == hi else (f"{lo}-{hi}" if hi < 10**9 else f"{lo}+")
    return "?"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-dir", default="data/qdrant/go_contrastive")
    add_store_args(p)
    p.add_argument("--mode", default="record_hybrid",
                   help="Ordering to measure against (default: record_hybrid).")
    p.add_argument("--k", type=int, default=10, help="Results per query (default: 10).")
    p.add_argument("--snippets", type=int, default=1,
                   help="Snippets attached per record (default: 1). This is the knob that "
                        "trades context for evidence.")
    p.add_argument("--limit", type=int, default=100, help="Queries to sample (default: 100).")
    p.add_argument("--out", help="Optional JSON file for the raw numbers.")
    args = p.parse_args()

    queries = load_queries(args.data_dir)
    qids = sorted(queries)[:args.limit]

    count, how = make_counter(args.model)
    r = open_retriever(args)
    client = r.client
    print(f"Counting with: {how}\n")

    per_query_full, per_query_snip = [], []
    by_bucket: dict[str, list[tuple[int, int]]] = {}

    for i, qid in enumerate(qids):
        hits = r.search(queries[qid], k=args.k, snippets_per_record=args.snippets,
                        mode=args.mode)
        if not hits:
            continue
        accs = [h["uniprot_id"] for h in hits]
        records = client.retrieve(RECORDS, ids=[record_id(a) for a in accs],
                                  with_payload=["uniprot_id", "text", "n_chunks"])
        text_of = {p.payload["uniprot_id"]: p.payload for p in records}

        q_full = q_snip = 0
        for h in hits:
            payload = text_of.get(h["uniprot_id"])
            if not payload:
                continue
            full = count(payload["text"])
            snip = sum(count(sn["text"]) for sn in h["snippets"]) or full
            q_full += full
            q_snip += snip
            by_bucket.setdefault(bucket_of(payload["n_chunks"]), []).append((full, snip))
        per_query_full.append(q_full)
        per_query_snip.append(q_snip)

        if (i + 1) % 25 == 0:
            print(f"  {i + 1}/{len(qids)} queries", flush=True)

    client.close()
    if not per_query_full:
        raise SystemExit("No results returned -- is the store populated?")

    full_mean = statistics.mean(per_query_full)
    snip_mean = statistics.mean(per_query_snip)
    saved = 100 * (1 - snip_mean / full_mean) if full_mean else 0.0

    print(f"\n{'=' * 70}")
    print(f"CONTEXT COST  (mode={args.mode}, k={args.k}, snippets={args.snippets})")
    print(f"{'=' * 70}")
    print(f"{len(per_query_full)} queries\n")
    print(f"  tokens per query, whole records   {full_mean:>9.0f}   "
          f"(median {statistics.median(per_query_full):.0f})")
    print(f"  tokens per query, snippets only   {snip_mean:>9.0f}   "
          f"(median {statistics.median(per_query_snip):.0f})")
    print(f"  reduction                         {saved:>8.1f}%")

    print(f"\n  by record length ({args.k} results/query pooled across queries)")
    print(f"  {'n_chunks':<10}{'records':>9}{'full':>10}{'snippets':>11}{'reduction':>12}")
    for key in sorted(by_bucket, key=lambda k: int(k.split("-")[0].rstrip("+"))):
        pairs = by_bucket[key]
        f = statistics.mean(x for x, _ in pairs)
        sn = statistics.mean(y for _, y in pairs)
        print(f"  {key:<10}{len(pairs):>9}{f:>10.0f}{sn:>11.0f}"
              f"{100 * (1 - sn / f) if f else 0:>11.1f}%")

    print("\n  The n_chunks=1 row should read ~0% -- a single-chunk record's snippet is\n"
          "  its record text. Anything else there means the chunker and the record are\n"
          "  out of sync. Every real saving comes from the rows below it.")

    if args.out:
        Path(args.out).write_text(json.dumps({
            "mode": args.mode, "k": args.k, "snippets": args.snippets,
            "queries": len(per_query_full), "counter": how,
            "tokens_full_mean": full_mean, "tokens_snippet_mean": snip_mean,
            "reduction_pct": saved,
        }, indent=2), encoding="utf-8")
        print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
