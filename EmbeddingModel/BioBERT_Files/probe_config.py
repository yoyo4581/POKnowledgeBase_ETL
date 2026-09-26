"""
Answers "which GO terms does this Config actually select as anchors?" against
the live graph, without building a 56MB dataset to find out.

Config is applied entirely in Python inside build_dataset() -- Q_ANNOTATIONS,
Q_TERMS and Q_HIERARCHY are unfiltered MATCH dumps. So this reproduces
build_dataset's steps 2-4 in Cypher instead:

    step 2  keep_evidence, drop_qualifier_prefixes, NOT-qualifier split
    step 3  propagation up the is_a/part_of DAG
    step 4  name present, blocklist, keep_namespaces, min_pos <= n <= max_pos

One thing Cypher alone cannot do: len(pos[t]) counts UniProt *accessions*, not
genes. extract() expands every gene-level annotation onto every accession under
that gene via dbo.EntrezUniprotMap, and drops genes with no function text -- both
live in SQL. So the counts come out on the wrong scale in two directions at once:
too low (genes with no mapped accession in FunctionData still counted) and too
high or low per gene (an isoform-rich gene contributes N, not 1).

    --weights   fixes it. Pulls _load_protein_data()'s crosswalk from SQL and
                weights each gene by its accession count, which makes n_pos
                the same number min_pos/max_pos are compared against in
                build_dataset. Needs the SQL connection; without it you get
                gene-scale counts, which are still fine for seeing the *shape*
                of what a threshold change does.

Read-only. Every query is MATCH/RETURN.

    python -m EmbeddingModel.BioBERT_Files.probe_config --inventory
    python -m EmbeddingModel.BioBERT_Files.probe_config --weights
    python -m EmbeddingModel.BioBERT_Files.probe_config --weights --sweep
    python -m EmbeddingModel.BioBERT_Files.probe_config --terms 60 --verdict below_min
"""
from __future__ import annotations

import argparse
from collections import defaultdict

# Neo4jCaller validates NEO4J_USERNAME/PASSWORD at import time and does not load
# .env itself -- the DAGs do it for it. This is a standalone entry point, so it
# has to do the same before that import can succeed. Optional, because
# --verify-export reads files only and has to work on Colab, where there is
# neither a .env nor necessarily python-dotenv.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from EmbeddingModel.BioBERT_Files.dataset import Config

# Variable-length bounds cannot be parameterised in Cypher, so the DAG depth cap
# is baked into the query text. GO's is_a/part_of chains run to roughly 15; 20 is
# slack. Raise it if TERMS_WITH_DEEPER_ANCESTORS below reports a non-zero count.
MAX_DEPTH = 20

# --- step 2: clean direct annotations -------------------------------------
# Mirrors build_dataset's positives branch exactly:
#   negated, qual = parse_qualifier(a["qualifier"])   -> type(r) here
#   if not evs & cfg.keep_evidence:        drop_weak_evidence
#   if qual.startswith(drop_prefixes):     drop_indirect_qualifier
#   direct_pos.add((g, t))                 (negated ones go to direct_not)
#
# r.evidence is always a list -- _upsert_annotation_edges accumulates it with
# `coalesce(r.evidence, []) + row.evidence` -- so `any(e IN ... )` is the set
# intersection `evs & cfg.keep_evidence`.
#
# The relationship TYPE is the raw GAF Qualifier column, backtick-quoted at
# write time, so a negated edge is literally typed `NOT|involved_in`.
_CLEAN = """
MATCH (g:Gene)-[r]->(src:Ontology)
WHERE NOT toUpper(type(r)) STARTS WITH 'NOT'
  AND any(e IN coalesce(r.evidence, []) WHERE e IN $keep_evidence)
  AND NOT any(p IN $drop_qualifier_prefixes WHERE toLower(type(r)) STARTS WITH p)
  %(gene_filter)s
WITH src, collect(DISTINCT g.id) AS direct
"""

# --- step 3: propagate ----------------------------------------------------
# up() adds a gene to its term AND every ancestor, where ancestors follow
# OUTGOING is_a/part_of (dag.parents[child].add(parent) for (c)-[r]->(p)).
# *0.. includes the term itself, which is the `{t} | dag.ancestors(t)` union.
# UNWIND-then-count does the set dedup that `out[x].add(g)` does in Python.
_PROPAGATE = f"""
MATCH (src)-[:is_a|part_of*0..{MAX_DEPTH}]->(t:Ontology)
UNWIND direct AS gene
WITH t, collect(DISTINCT gene) AS genes
"""

_COUNT_GENES = "WITH t, size(genes) AS n_pos\n"
_COUNT_WEIGHTED = ("WITH t, reduce(s = 0, x IN genes | s + coalesce($gene_weights[x], 0)) "
                   "AS n_pos\n")

# --- step 4: eligibility --------------------------------------------------
_ELIGIBLE = """
WITH t, n_pos,
     (t.name IS NOT NULL
      AND NOT t.id IN $blocklist
      AND t.hasOBONamespace IN $keep_namespaces) AS eligible
"""

# Deliberately grouped by namespace, and WITHOUT the keep_namespaces filter, so
# the row for cellular_component shows what admitting CC would actually buy.
Q_FUNNEL = _CLEAN + _PROPAGATE + "%(count)s" + """
WITH t, n_pos,
     (t.name IS NOT NULL AND NOT t.id IN $blocklist) AS named
RETURN coalesce(t.hasOBONamespace, '(none)') AS namespace,
       count(*) AS terms_with_positives,
       sum(CASE WHEN named THEN 1 ELSE 0 END) AS eligible_terms,
       sum(CASE WHEN named AND n_pos < $min_pos THEN 1 ELSE 0 END) AS below_min_pos,
       sum(CASE WHEN named AND n_pos > $max_pos THEN 1 ELSE 0 END) AS above_max_pos,
       sum(CASE WHEN named AND n_pos >= $min_pos AND n_pos <= $max_pos
                THEN 1 ELSE 0 END) AS anchors,
       max(n_pos) AS max_n_pos
ORDER BY anchors DESC
"""

Q_TERMS = _CLEAN + _PROPAGATE + "%(count)s" + _ELIGIBLE + """
WITH t, n_pos WHERE eligible
RETURN t.id AS term, t.name AS name, t.hasOBONamespace AS namespace, n_pos,
       CASE WHEN n_pos < $min_pos THEN 'below_min'
            WHEN n_pos > $max_pos THEN 'above_max'
            ELSE 'ANCHOR' END AS verdict
ORDER BY n_pos DESC
"""

# What is actually on the edges. keep_evidence and drop_qualifier_prefixes are
# only meaningful against the values the graph really carries -- a code in the
# frozenset that never appears is dead config, and one that appears but is
# missing silently costs coverage.
Q_INVENTORY = """
MATCH (:Gene)-[r]->(:Ontology)
WITH type(r) AS qualifier, coalesce(r.evidence, []) AS evs
UNWIND (CASE WHEN size(evs) = 0 THEN [null] ELSE evs END) AS ev
RETURN qualifier, ev AS evidence, count(*) AS edges
ORDER BY edges DESC
"""

# If this is not zero, MAX_DEPTH above is truncating the propagation and every
# n_pos is an undercount.
Q_DEPTH_CHECK = f"""
MATCH (c:Ontology)-[:is_a|part_of*{MAX_DEPTH}]->(:Ontology)
RETURN count(DISTINCT c) AS terms_at_or_beyond_max_depth
"""


def params(cfg: Config) -> dict:
    return {
        "keep_evidence": sorted(cfg.keep_evidence),
        "drop_qualifier_prefixes": [p.lower() for p in cfg.drop_qualifier_prefixes],
        "keep_namespaces": sorted(cfg.keep_namespaces),
        "blocklist": sorted(cfg.blocklist),
        "min_pos": cfg.min_pos,
        "max_pos": cfg.max_pos,
    }


def gene_weights() -> dict[str, int]:
    """
    entrez_id -> how many of its UniProt accessions have function text.

    This is exactly the multiplier extract() applies: each gene-level annotation
    is expanded onto every accession in entrez_to_uniprots[gene], and that map is
    built only from accessions present in dbo.FunctionData.
    """
    from EmbeddingModel.BioBERT_Files.dataset import _load_protein_data
    from src.builders.SQL.SQLCaller import SQL_ETL

    entrez_to_uniprots, protein_rows = _load_protein_data(SQL_ETL())
    weights = {gene: len(accessions) for gene, accessions in entrez_to_uniprots.items()}
    multi = sum(1 for n in weights.values() if n > 1)
    print(f"  {len(protein_rows)} accessions with function text across {len(weights)} genes "
          f"({multi} of them with more than one accession)")
    return weights


def verify_export(data_dir, cfg: Config) -> None:
    """
    Recover the min_pos / max_pos / pos_per_anchor an exported dataset was built
    with, and say whether they still match dataset.py.

    Needs no database -- it reads only the exported files, so it runs on Colab
    against a freshly downloaded release. What each file pins down:

      eval_relevant.json  {t: pos[t]} for the held-out anchors, UNCAPPED. Every
                          held-out term passed the filter, so every set size
                          satisfies min_pos <= len <= max_pos. This is the only
                          file that can see max_pos at all.
      train.jsonl         one row per sampled positive, so rows-per-anchor is
                          min(pos_per_anchor, len(pos[t])). The max reads off
                          pos_per_anchor exactly; the min reads off min_pos. The
                          cap is why max_pos is invisible here.
      stats.json          pos_size_near_min's keys run 1..min_pos+3, which pins
                          min_pos exactly -- on exports new enough to carry it.

    The bounds are one-sided: an observed maximum of 148 proves max_pos >= 148,
    not that it is 150. With hundreds of held-out terms the observed extremes sit
    essentially on the thresholds, so a real disagreement shows up as a gap far
    larger than one or two.
    """
    import json as _json
    from collections import Counter
    from pathlib import Path as _Path

    d = _Path(data_dir)
    print(f"\nVerifying {d}")

    relevant = {t: v for t, v in
                _json.loads((d / "eval_relevant.json").read_text()).items()}
    sizes = sorted(len(v) for v in relevant.values())
    print(f"\n  eval_relevant.json: {len(sizes)} held-out anchors, "
          f"|pos| from {sizes[0]} to {sizes[-1]}")
    print(f"    => min_pos <= {sizes[0]}   and   max_pos >= {sizes[-1]}")

    per_anchor = Counter()
    with open(d / "train.jsonl", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                per_anchor[_json.loads(line)["anchor"]] += 1
    counts = sorted(per_anchor.values())
    print(f"\n  train.jsonl: {sum(counts)} rows over {len(per_anchor)} train anchors, "
          f"{counts[0]}-{counts[-1]} rows each")
    print(f"    => pos_per_anchor == {counts[-1]}   and   min_pos <= {counts[0]}")

    observed = {"min_pos": min(sizes[0], counts[0]),
                "max_pos": sizes[-1],
                "pos_per_anchor": counts[-1]}

    stats_path = d / "stats.json"
    if stats_path.exists():
        stats = _json.loads(stats_path.read_text())
        near = stats.get("pos_size_near_min")
        if near:
            # keys are range(1, min_pos + 4), so max key == min_pos + 3
            exact = max(int(k) for k in near) - 3
            print(f"\n  stats.json: pos_size_near_min spans 1..{max(int(k) for k in near)} "
                  f"=> min_pos == {exact} exactly")
            observed["min_pos"] = exact
        else:
            print("\n  stats.json: predates pos_size_near_min; min_pos stays a bound.")
        for k in ("anchors_total", "anchors_train", "anchors_lost_to_buffer",
                  "eligible_terms", "rejected_below_min_pos", "rejected_above_max_pos"):
            if k in stats:
                print(f"    {k:26s} {stats[k]}")

    print(f"\n  {'parameter':<16} {'in dataset.py':>14} {'from export':>14}   verdict")
    ok = True
    for name, rel in (("min_pos", "<="), ("max_pos", ">="), ("pos_per_anchor", "==")):
        want, got = getattr(cfg, name), observed[name]
        if rel == "==":
            good = want == got
        elif rel == "<=":
            good = want <= got
        else:
            good = want >= got
        ok &= good
        note = "consistent" if good else f"MISMATCH (needs {name} {rel} {got})"
        print(f"  {name:<16} {want:>14} {got:>14}   {note}")

    print()
    if ok:
        print("  The export is consistent with the current Config.")
    else:
        print("  This export was NOT built with the Config now in dataset.py. Re-run\n"
              "  embedding_dataset_export before fine-tuning, or Colab will train on\n"
              "  the old thresholds while you believe it used the new ones.")


def run(driver, query: str, database: str | None = None, **kwargs) -> list[dict]:
    from neo4j import RoutingControl

    records, _, _ = driver.execute_query(query, database_=database,
                                         routing_=RoutingControl.READ, **kwargs)
    return [r.data() for r in records]


def table(rows: list[dict], columns: list[str]) -> None:
    if not rows:
        print("  (no rows)")
        return
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in columns}
    print("  " + "  ".join(c.ljust(widths[c]) for c in columns))
    print("  " + "  ".join("-" * widths[c] for c in columns))
    for r in rows:
        print("  " + "  ".join(str(r.get(c, "")).ljust(widths[c]) for c in columns))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--weights", action="store_true",
                        help="Weight genes by accession count from SQL, so n_pos is on the "
                             "same scale min_pos/max_pos are compared against. Needs pyodbc.")
    parser.add_argument("--verify-export", metavar="DIR", nargs="?",
                        const="data/qdrant/go_contrastive",
                        help="Recover min_pos/max_pos/pos_per_anchor from an exported "
                             "dataset and check them against dataset.py. Reads files "
                             "only -- no Neo4j, no SQL, so it runs on Colab.")
    parser.add_argument("--inventory", action="store_true",
                        help="Only show which qualifiers and evidence codes the graph carries.")
    parser.add_argument("--terms", type=int, metavar="N",
                        help="Also list the N terms with the most positives.")
    parser.add_argument("--verdict", choices=["ANCHOR", "below_min", "above_max"],
                        help="Restrict --terms to one verdict. below_min is the interesting "
                             "one when deciding whether min_pos is too high.")
    parser.add_argument("--bands", action="store_true",
                        help="Break eligible terms into n_pos bands and show what each "
                             "contributes to the triplet set, plus what a lower max_pos "
                             "would exclude by name. The view for tuning max_pos.")
    parser.add_argument("--sweep", action="store_true",
                        help="Anchor count across a grid of min_pos/max_pos values.")
    parser.add_argument("--min-pos", type=int, help="Override Config.min_pos.")
    parser.add_argument("--max-pos", type=int, help="Override Config.max_pos.")
    parser.add_argument("--namespaces", nargs="+",
                        help="Override Config.keep_namespaces, e.g. --namespaces "
                             "molecular_function biological_process cellular_component")
    parser.add_argument("--database", help="Neo4j database name, if not the default.")
    args = parser.parse_args()

    cfg = Config()
    if args.min_pos is not None:
        cfg = Config(**{**cfg.__dict__, "min_pos": args.min_pos})
    if args.max_pos is not None:
        cfg = Config(**{**cfg.__dict__, "max_pos": args.max_pos})
    if args.namespaces:
        cfg = Config(**{**cfg.__dict__, "keep_namespaces": frozenset(args.namespaces)})

    if args.verify_export:
        # Before the Neo4j import: this path touches no database, which is what
        # makes it usable from the Colab notebook against a downloaded release.
        verify_export(args.verify_export, cfg)
        return

    from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
    driver = Neo4j_ETL().driver

    if args.inventory:
        print("\nQualifier x evidence code, as the graph actually holds them")
        print(f"(keep_evidence = {sorted(cfg.keep_evidence)})")
        rows = run(driver, Q_INVENTORY, args.database)
        table(rows, ["qualifier", "evidence", "edges"])
        seen = {r["evidence"] for r in rows}
        unused = sorted(cfg.keep_evidence - seen)
        missing = sorted(c for c in seen - cfg.keep_evidence if c)
        print(f"\n  in keep_evidence but absent from the graph: {unused or 'none'}")
        print(f"  present in the graph but excluded:           {missing or 'none'}")
        return

    p = params(cfg)
    gene_filter = ""
    count = _COUNT_GENES
    if args.weights:
        print("Loading accession weights from SQL...")
        p["gene_weights"] = gene_weights()
        # Genes with no accession carrying function text never reach pos[] at all,
        # because extract() expands through entrez_to_uniprots and build_dataset
        # then drops any pair whose gene is not in gene_text.
        gene_filter = "AND $gene_weights[g.id] IS NOT NULL"
        count = _COUNT_WEIGHTED

    scale = "accessions (matches build_dataset)" if args.weights else "genes (approximate)"
    print(f"\nConfig: min_pos={cfg.min_pos} max_pos={cfg.max_pos} "
          f"namespaces={sorted(cfg.keep_namespaces)}")
    print(f"n_pos is counted in {scale}\n")

    depth = run(driver, Q_DEPTH_CHECK, args.database)[0]["terms_at_or_beyond_max_depth"]
    if depth:
        print(f"  WARNING: {depth} terms have ancestors at depth >= {MAX_DEPTH}; "
              f"propagation is truncated and every n_pos below is an undercount. "
              f"Raise MAX_DEPTH.\n")

    funnel = Q_FUNNEL % {"gene_filter": gene_filter, "count": count}
    print("Anchor funnel by namespace (keep_namespaces is NOT applied here, so you")
    print("can see what admitting another namespace would add):")
    table(run(driver, funnel, args.database, **p),
          ["namespace", "terms_with_positives", "eligible_terms",
           "below_min_pos", "above_max_pos", "anchors", "max_n_pos"])
    print("\n  Summing anchors over keep_namespaces should land within a handful of")
    print("  stats.json's anchors_total. build_dataset records eligible_terms /")
    print("  rejected_below_min_pos / rejected_above_max_pos too, but those were added")
    print("  after the checked-in stats.json was written -- they appear on the next export.")

    if args.sweep:
        print("\nAnchors admitted, by threshold (keep_namespaces applied):")
        grid_min = [1, 2, 3, 5, 8, 10, 15, 20]
        grid_max = [100, 200, 300, 500, 1000, 5000]
        terms = run(driver, Q_TERMS % {"gene_filter": gene_filter, "count": count},
                    args.database, **{**p, "min_pos": 0, "max_pos": 10 ** 9})
        counts = [r["n_pos"] for r in terms]
        header = ["min_pos \\ max_pos"] + [str(m) for m in grid_max]
        rows = []
        for lo in grid_min:
            row = {"min_pos \\ max_pos": str(lo)}
            for hi in grid_max:
                row[str(hi)] = sum(1 for n in counts if lo <= n <= hi)
            rows.append(row)
        table(rows, header)
        print(f"\n  ({len(counts)} eligible terms total; the current Config sits at "
              f"min_pos={cfg.min_pos}, max_pos={cfg.max_pos})")

    if args.bands:
        import statistics
        rows = [r for r in run(driver, Q_TERMS % {"gene_filter": gene_filter, "count": count},
                               args.database, **{**p, "min_pos": 0, "max_pos": 10 ** 9})
                if r["n_pos"] > 0]
        corpus = sum(p["gene_weights"].values()) if args.weights else len(
            {g for g in []}) or None

        # pos_per_anchor caps every anchor's contribution, so a term at 300 and a
        # term at 31 are worth exactly the same number of rows. max_pos therefore
        # is NOT a volume knob -- it decides which anchors exist, not how much
        # data they bring.
        cap = cfg.pos_per_anchor
        print(f"\nn_pos bands (pos_per_anchor={cap} caps each anchor's rows):")
        band_rows = []
        for lo, hi in [(cfg.min_pos, 25), (26, 50), (51, 100), (101, 200),
                       (201, 300), (301, 1000), (1001, 10 ** 9)]:
            b = [r for r in rows if lo <= r["n_pos"] <= hi]
            if not b:
                continue
            band_rows.append({
                "band": f"{lo}-{hi}" if hi < 10 ** 9 else f"{lo}+",
                "terms": len(b),
                "triplet_rows": sum(min(cap, r["n_pos"]) for r in b),
                "median_n_pos": int(statistics.median(r["n_pos"] for r in b)),
                "pct_corpus_relevant": (f"{100 * statistics.median(r['n_pos'] for r in b) / corpus:.1f}%"
                                        if corpus else "-"),
            })
        table(band_rows, ["band", "terms", "triplet_rows", "median_n_pos",
                          "pct_corpus_relevant"])

        keep = [r for r in rows if cfg.min_pos <= r["n_pos"] <= cfg.max_pos]
        print(f"\n  At max_pos={cfg.max_pos}: {len(keep)} anchors, "
              f"{sum(min(cap, r['n_pos']) for r in keep)} triplet rows.")
        print(f"  Re-run with --max-pos N to see what a different ceiling costs.")

        excluded = sorted((r for r in rows if r["n_pos"] > cfg.max_pos),
                          key=lambda r: -r["n_pos"])
        print(f"\n  Terms excluded by max_pos={cfg.max_pos}, largest first "
              f"({len(excluded)} total) -- these are the ones to eyeball for "
              f"'is this actually too broad to be a query?':")
        table(excluded[:15], ["term", "name", "namespace", "n_pos"])
        print("\n  ...and the smallest of them, i.e. what you lose at the margin:")
        table(excluded[-8:], ["term", "name", "namespace", "n_pos"])

    if args.terms:
        rows = run(driver, Q_TERMS % {"gene_filter": gene_filter, "count": count},
                   args.database, **p)
        if args.verdict:
            rows = [r for r in rows if r["verdict"] == args.verdict]
        by_verdict = defaultdict(int)
        for r in rows:
            by_verdict[r["verdict"]] += 1
        print(f"\nTop {args.terms} terms"
              f"{f' with verdict={args.verdict}' if args.verdict else ''} "
              f"({dict(by_verdict)}):")
        table(rows[:args.terms], ["term", "name", "namespace", "n_pos", "verdict"])

    driver.close()


if __name__ == "__main__":
    main()
