"""
Post-hoc analysis of a finished evaluation, from eval_results/per_query.jsonl.

Re-running eval_retriever.py costs a full retrieval sweep (~25 min/variant on a
6k-record embedded store). But every per-query score for every metric and every
arm is already written to per_query.jsonl, so none of what follows needs the
store, the model, or a single extra query.

Two things it answers that the main run does not:

  1. Significance on the metric that actually moved. eval_retriever.py
     bootstraps --metric, which defaults to ndcg@10. If the effect lives in
     accuracy@1 and mrr@10 instead, that default tests the one place nothing
     happened.

  2. Where the effect comes from. A single averaged delta cannot distinguish
     "helps a little everywhere" from "helps a lot on some queries and hurts on
     others" -- and with 39% of this corpus too short to chunk at all, the
     average is diluted by construction.

Usage
-----
    python analyze_eval.py --results eval_results
    python analyze_eval.py --results eval_results --metrics accuracy@1 mrr@10 ndcg@10
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


import argparse
import json
import statistics
from pathlib import Path

from core.metrics import paired_bootstrap, significant

# Metrics worth testing by default: the rank-position family, where a
# re-ranking effect shows up, plus the set-composition ones it trades against.
DEFAULT_METRICS = ("accuracy@1", "accuracy@3", "accuracy@10",
                   "mrr@10", "ndcg@10", "precision@10", "recall@10", "map@100")

# Buckets over |relevant|. A GO term with few annotated proteins is specific;
# one with many is generic. Chunking should help most on the specific end, where
# the answer is a localized claim rather than a whole-record theme.
BUCKETS = ((1, 5), (6, 15), (16, 40), (41, 10**9))


def load(results_dir: Path) -> list[dict]:
    rows = []
    with open(results_dir / "per_query.jsonl", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    if not rows:
        raise SystemExit(f"{results_dir}/per_query.jsonl is empty.")
    return rows


def bucket_of(n: int) -> str:
    for lo, hi in BUCKETS:
        if lo <= n <= hi:
            return f"{lo}-{hi}" if hi < 10**9 else f"{lo}+"
    return "?"


def significance_table(rows: list[dict], arm: str, baseline: str,
                       metrics: tuple[str, ...], seed: int) -> None:
    print(f"\nPaired bootstrap, {arm} vs {baseline}  ({len(rows)} queries, 10k resamples)")
    print(f"{'metric':<14}{'delta':>10}{'95% CI':>22}{'p':>9}   {'better/worse/same':>18}")
    print("-" * 78)
    for m in metrics:
        if m not in rows[0]["scores"][baseline]:
            print(f"{m:<14}  (not recorded in this run)")
            continue
        a = [r["scores"][arm][m] for r in rows]
        b = [r["scores"][baseline][m] for r in rows]
        s = paired_bootstrap(a, b, seed=seed)
        ci = f"[{s['ci_lo']:+.4f}, {s['ci_hi']:+.4f}]"
        flag = "  *" if significant(s) else "   "
        counts = f"{s['queries_improved']}/{s['queries_worsened']}/{s['queries_unchanged']}"
        print(f"{m:<14}{s['delta']:>+10.4f}{ci:>22}{s['p_two_sided']:>9.4f}{flag}{counts:>15}")
    print("\n  * = 95% interval excludes zero")


def stratified_table(rows: list[dict], arm: str, baseline: str, metric: str) -> None:
    groups: dict[str, list[dict]] = {}
    for r in rows:
        groups.setdefault(bucket_of(r["n_relevant"]), []).append(r)

    print(f"\n{metric} by query specificity (|relevant| = proteins annotated to the term)")
    print(f"{'|relevant|':<12}{'queries':>9}{baseline:>16}{arm:>16}{'delta':>10}{'95% CI':>22}")
    print("-" * 85)
    for key in sorted(groups, key=lambda k: int(k.split("-")[0].rstrip("+"))):
        g = groups[key]
        a = [r["scores"][arm][metric] for r in g]
        b = [r["scores"][baseline][metric] for r in g]
        s = paired_bootstrap(a, b, n_resamples=5000)
        ci = f"[{s['ci_lo']:+.4f}, {s['ci_hi']:+.4f}]"
        print(f"{key:<12}{len(g):>9}{statistics.mean(b):>16.4f}"
              f"{statistics.mean(a):>16.4f}{s['delta']:>+10.4f}{ci:>22}")
    print("\n  Specific terms (few relevant proteins) are where a localized passage match\n"
          "  should pay off most. A gradient across these rows is the mechanism showing\n"
          "  itself; a flat profile means the effect is not specificity-driven.")


def treated_subset(rows: list[dict], arm: str, baseline: str, k: int = 10) -> list[dict]:
    """
    Queries where the two arms actually returned different rankings.

    A record whose only chunk is the whole record has a chunk score equal to its
    record score, so it cannot move. When every record in a query's top-k is
    like that, chunk re-ranking is a no-op and the query contributes an exact
    zero to the mean delta -- diluting the estimate without carrying any
    information about whether re-ranking works.

    Restricting to queries whose ranking changed is conditioning on whether the
    treatment was applied, not on its outcome, so the comparison stays honest.
    It answers the narrower and more answerable question: when chunk re-ranking
    does something, does that something help?
    """
    out = []
    for r in rows:
        runs = r.get("runs") or {}
        a, b = runs.get(arm), runs.get(baseline)
        if a is None or b is None:
            continue
        if a[:k] != b[:k]:
            out.append(r)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results", default="eval_results", help="Directory holding per_query.jsonl.")
    p.add_argument("--arm", default="chunk_rerank")
    p.add_argument("--baseline", default="record_hybrid")
    p.add_argument("--metrics", nargs="+", default=list(DEFAULT_METRICS))
    p.add_argument("--stratify-metric", default="accuracy@3",
                   help="Metric for the specificity breakdown (default: accuracy@3, where the "
                        "effect in this experiment is largest).")
    p.add_argument("--treated-k", type=int, default=10,
                   help="Depth at which two rankings count as different (default: 10).")
    p.add_argument("--seed", type=int, default=0)
    args = p.parse_args()

    rows = load(Path(args.results))
    have = set(rows[0]["scores"])
    for name in (args.arm, args.baseline):
        if name not in have:
            raise SystemExit(f"{name!r} not in this run; it has {sorted(have)}.")

    n_rel = [r["n_relevant"] for r in rows]
    print(f"{len(rows)} queries, |relevant| median {statistics.median(n_rel):.0f} "
          f"mean {statistics.mean(n_rel):.1f} max {max(n_rel)}")

    significance_table(rows, args.arm, args.baseline, tuple(args.metrics), args.seed)

    treated = treated_subset(rows, args.arm, args.baseline, k=args.treated_k)
    if not rows[0].get("runs"):
        print("\n(no `runs` recorded in this per_query.jsonl -- cannot isolate the treated subset)")
    elif not treated:
        print(f"\nNo query's top-{args.treated_k} ranking differed between the arms at all. "
              f"Chunk re-ranking is a complete no-op on this store.")
    else:
        pct = 100 * len(treated) / len(rows)
        print(f"\n{'=' * 78}\nTREATED SUBSET: {len(treated)}/{len(rows)} queries ({pct:.0f}%) "
              f"whose top-{args.treated_k} ranking actually changed.\n"
              f"The other {len(rows) - len(treated)} contribute an exact zero to every delta "
              f"above.\n{'=' * 78}")
        significance_table(treated, args.arm, args.baseline, tuple(args.metrics), args.seed)

    stratified_table(rows, args.arm, args.baseline, args.stratify_metric)


if __name__ == "__main__":
    main()
