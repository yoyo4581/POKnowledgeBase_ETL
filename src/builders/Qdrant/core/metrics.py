"""
Binary-relevance IR metrics and the paired bootstrap. Pure functions only.

These used to live inside the evaluation script, which meant anything wanting a
bootstrap had to import the whole 400-line experiment -- including its argument
parser and its Qdrant client -- to get at twenty lines of arithmetic. They have
no I/O and no dependencies beyond the standard library and numpy, so they
belong on their own.

The metric definitions match sentence-transformers' InformationRetrievalEvaluator,
so a number produced here is directly comparable to one printed during training.
"""
from __future__ import annotations

import math
import statistics

import numpy as np

DEFAULT_KS = (1, 3, 5, 10)
PRIMARY_METRIC = "ndcg@10"


def per_query_metrics(ranked: list[str], relevant: set[str], ks: tuple[int, ...],
                      map_k: int) -> dict[str, float]:
    """One query's ranked list -> every metric at every cutoff."""
    out: dict[str, float] = {}
    for k in ks:
        top = ranked[:k]
        hits = [d for d in top if d in relevant]
        out[f"accuracy@{k}"] = 1.0 if hits else 0.0
        out[f"precision@{k}"] = len(hits) / k
        out[f"recall@{k}"] = len(hits) / len(relevant)

        rr = 0.0
        for i, doc in enumerate(top):
            if doc in relevant:
                rr = 1.0 / (i + 1)
                break
        out[f"mrr@{k}"] = rr

        dcg = sum(1.0 / math.log2(i + 2) for i, doc in enumerate(top) if doc in relevant)
        idcg = sum(1.0 / math.log2(i + 2) for i in range(min(len(relevant), k)))
        out[f"ndcg@{k}"] = dcg / idcg if idcg else 0.0

    top = ranked[:map_k]
    found, precisions = 0, []
    for i, doc in enumerate(top):
        if doc in relevant:
            found += 1
            precisions.append(found / (i + 1))
    out[f"map@{map_k}"] = sum(precisions) / min(len(relevant), map_k)
    return out


def aggregate(scores: dict[str, dict[str, float]]) -> dict[str, float]:
    """Per-query metric dicts -> their means."""
    if not scores:
        return {}
    keys = next(iter(scores.values())).keys()
    return {k: statistics.mean(s[k] for s in scores.values()) for k in keys}


def paired_bootstrap(arm: list[float], baseline: list[float], n_resamples: int = 10000,
                     seed: int = 0) -> dict[str, float]:
    """
    Resample queries with replacement; report the distribution of the mean
    per-query difference.

    Paired, because both arms answer the same queries -- an unpaired test would
    drown the effect in between-query variance.
    """
    deltas = np.asarray(arm) - np.asarray(baseline)
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(deltas), size=(n_resamples, len(deltas)))
    means = deltas[idx].mean(axis=1)
    tail = float(min((means <= 0).mean(), (means >= 0).mean()))
    return {
        "delta": float(deltas.mean()),
        "ci_lo": float(np.percentile(means, 2.5)),
        "ci_hi": float(np.percentile(means, 97.5)),
        "p_two_sided": min(1.0, 2 * tail),
        "queries_improved": int((deltas > 0).sum()),
        "queries_worsened": int((deltas < 0).sum()),
        "queries_unchanged": int((deltas == 0).sum()),
    }


def significant(stats: dict[str, float]) -> bool:
    """Whether the 95% interval excludes zero."""
    return stats["ci_lo"] > 0 or stats["ci_hi"] < 0
