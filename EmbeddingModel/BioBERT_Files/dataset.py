"""
Builds a contrastive fine-tuning dataset for BioBERT from this project's
own knowledge base, instead of a synthetic/toy graph:

Anchor   = GO term name              (plays the query)
Positive = UniProt function text of a gene annotated to that term
Negative = function text of a gene that is confusable but NOT annotated

Pipeline
  1. extract    read GO structure + annotations from Neo4j, join gene ->
                function text across SQL (dbo.FunctionData, plus
                dbo.EntrezUniprotMap when the source keys genes by entrez id)
  2. clean      evidence + qualifier filters; separate NOT annotations
  3. propagate  true path rule over is_a/part_of: positives flow UP to
                ancestors, NOT annotations flow DOWN to descendants
  4. anchors    mid-specificity MF/BP terms
  5. split      hold out GO terms, plus a lineage buffer so no ancestor or
                descendant of a test term is trained on
  6. mine       (anchor, positive, hard negative) triplets

See train.py for turning a TrainingData into a fine-tuned model (or an
export for external training, e.g. Google Colab).
"""
from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable, Iterator, TYPE_CHECKING

# Neo4jCaller validates NEO4J_USERNAME/NEO4J_PASSWORD at import time, and
# SQLCaller unconditionally imports pyodbc (which needs a system ODBC
# driver Colab/most external machines won't have). Both would force every
# caller of this module -- including toy.py's offline smoke test and
# train.py's --from-export path, which trains from files and never touches
# either database -- to have them installed/configured just to import this
# module. Only needed for the extract() type hint below, so both are
# deferred to TYPE_CHECKING / a local import inside extract()'s caller.
if TYPE_CHECKING:
    from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
    from src.builders.SQL.SQLCaller import SQL_ETL

from src.builders.Neo4j.schema.nodes import Ontology, Gene

# ---------------------------------------------------------------------------
# 0. Schema: read off the canonical node classes, not guessed at.
#
# GO term name/deprecated come from OntoStateManager.flatten_go_props (see
# src/builders/Neo4j/OntologyState.py), which writes properties straight
# off go-basic.json's own node shape -- it does NOT go through the Ontology
# dataclass below at all (sync_ontology_structure hardcodes `MERGE (o:Ontology
# {id: ...})` itself). So Ontology.__label__/__key__ are safe to reuse (they
# match that hardcoded literal by construction), but Ontology's other field
# names cannot be trusted as the real property keys. Verified directly
# against a live data/go-basic.json node:
#   {"id": ".../GO_0000001", "meta": {"basicPropertyValues": [
#       {"pred": ".../oboInOwl#hasOBONamespace", "val": "biological_process"}]}}
# -> the actual Neo4j property is `hasOBONamespace` (capital OBO), not
# Ontology.hasOboNamespace as declared in schema/nodes.py -- that field
# appears to be unused/aspirational dead code. Term ids land in Neo4j in
# underscore form (e.g. "GO_0000001"), same as GOOntologyRecord builds them.
# ---------------------------------------------------------------------------
TERM_LABEL, TERM_KEY = Ontology.__label__, Ontology.__key__      # "Ontology", "id"
TERM_NAME_PROP = "name"
TERM_NAMESPACE_PROP = "hasOBONamespace"                          # see note above

GENE_LABEL, GENE_KEY = Gene.__label__, Gene.__key__               # "Gene", "id"

# True path rule applies to these hierarchy relations only. Real predicate
# names confirmed against go-basic.json's edges (is_a, and BFO_0000050 /
# RO_0002211-3 which OntoStateManager.sync_ontology_structure remaps to
# part_of / regulates / negatively_regulates / positively_regulates).
HIERARCHY_RELS = ("is_a", "part_of")
SIBLING_REL = "is_a"   # siblings = terms sharing an is_a parent

Q_ANNOTATIONS = f"""
MATCH (g:{GENE_LABEL})-[a]->(t:{TERM_LABEL})
RETURN g.{GENE_KEY} AS gene, t.{TERM_KEY} AS term,
       type(a) AS qualifier, a.evidence AS evidence
"""
Q_HIERARCHY = f"""
MATCH (c:{TERM_LABEL})-[r:{'|'.join(HIERARCHY_RELS)}]->(p:{TERM_LABEL})
RETURN c.{TERM_KEY} AS child, p.{TERM_KEY} AS parent, type(r) AS rel
"""
Q_TERMS = f"""
MATCH (t:{TERM_LABEL})
RETURN t.{TERM_KEY} AS term, t.{TERM_NAME_PROP} AS name, t.{TERM_NAMESPACE_PROP} AS namespace
"""


@dataclass
class Config:
    # Experimental, high-throughput, and curator-statement codes. Add "IBA"
    # (phylogenetic) for much more coverage at slightly lower precision.
    keep_evidence: frozenset = frozenset({
        "EXP", "IDA", "IPI", "IMP", "IGI", "IEP",
        "HTP", "HDA", "HMP", "HGI", "HEP", "TAS", "IC"})
    # Qualifiers too indirect to mean "this text describes this term"
    drop_qualifier_prefixes: tuple = ("acts_upstream_of", "colocalizes_with")
    # UniProt function text mostly describes MF and BP, rarely location (CC)
    keep_namespaces: frozenset = frozenset({"molecular_function", "biological_process"})
    blocklist: frozenset = frozenset({"GO_0005515"})  # "protein binding": uninformative
    min_pos: int = 3        # too few genes -> too few training pairs
    max_pos: int = 150      # too many -> term is too generic to be a useful query
    pos_per_anchor: int = 50
    p_curated_neg: float = 0.5  # how often to prefer a curated NOT when one exists
    test_frac: float = 0.1
    seed: int = 13


# ---------------------------------------------------------------------------
# 1. Extract
# ---------------------------------------------------------------------------
def _load_protein_data(sql_caller: SQL_ETL) -> tuple[dict[str, list[str]], list[dict]]:
    """
    Joins Neo4j's gene granularity to FunctionData's accession granularity.

    FunctionData is keyed by uniprot_id (protein). Q_ANNOTATIONS comes back
    keyed by Gene.id, and what Gene.id *is* depends on the source:

      kegg      an entrez id, so reaching an accession needs the
                dbo.EntrezUniprotMap crosswalk. A gene with several mapped
                accessions (isoforms, paralogous entries) can have genuinely
                different function text per accession, so nothing here picks
                a "representative": every accession with function text
                becomes its own corpus entry and extract() expands the
                gene's annotations onto all of them.

      reactome  the accession itself, so the mapping is the identity and
                the only work left is to drop genes with no function text.

    Returns (gene_to_accessions, protein_rows). gene_to_accessions feeds
    extract()'s annotation expansion and is keyed by *Gene.id*, whatever
    that is for the active source -- it is NOT an entrez crosswalk under
    Reactome, and must not be confused with load_entrez_crosswalk() below,
    which is keyed by entrez id and exists for an unrelated purpose.
    protein_rows is [{"gene": uniprot_id, "text": function_text}, ...] --
    kept as "gene" to match build_dataset's existing (schema-agnostic)
    field name, even though the id is a uniprot_id.
    """
    uniprot_text: dict[str, str] = {}
    try:
        for batch in sql_caller.sql_state.fetch_data(table_name="FunctionData", kind="dbo"):
            for row in batch:
                uniprot_text[row["uniprot_id"]] = row["function_text"]
    except ValueError:
        return {}, []

    # pathway_source, not "is EntrezUniprotMap in the schema" -- the table is
    # shared by both sources now, so its presence says nothing about whether
    # Gene.id needs crosswalking. Testing for it here would look up accessions
    # in an entrez-keyed dict, miss every time, and drop 100% of annotations.
    from src.builders.SQL.schema import pathway_source

    gene_to_accessions: dict[str, list[str]] = defaultdict(list)
    if pathway_source == "kegg":
        try:
            for batch in sql_caller.sql_state.fetch_data(table_name="EntrezUniprotMap", kind="dbo"):
                for row in batch:
                    if row["uniprot_id"] in uniprot_text:
                        gene_to_accessions[str(row["entrez_id"])].append(row["uniprot_id"])
        except ValueError:
            return {}, []
    else:
        for uniprot_id in uniprot_text:
            gene_to_accessions[uniprot_id].append(uniprot_id)

    protein_rows = [{"gene": uniprot_id, "text": text} for uniprot_id, text in uniprot_text.items()]
    return dict(gene_to_accessions), protein_rows


def load_entrez_crosswalk(sql_caller: SQL_ETL,
                          corpus: Iterable[str] | None = None) -> dict[str, list[str]]:
    """
    {entrez_id: [uniprot_id, ...]} straight from dbo.EntrezUniprotMap,
    optionally restricted to `corpus` (the accessions that actually have
    function text, so the exported file carries no dead entries).

    Deliberately separate from _load_protein_data(). Under KEGG the two
    happen to coincide; under Reactome they are different maps for
    different jobs, and conflating them is a silent failure in both
    directions:

      _load_protein_data   keyed by Gene.id, joins Neo4j annotations to
                           corpus text. Identity under Reactome.
      load_entrez_crosswalk  keyed by entrez id, rides down to the Qdrant
                           record payload as a lookup qualifier so a caller
                           holding an entrez id can still find the record.

    ~95% of this corpus has an entrez id; the rest legitimately has none and
    simply does not appear here. Returns {} if the table is absent.
    """
    keep = set(corpus) if corpus is not None else None
    crosswalk: dict[str, list[str]] = defaultdict(list)
    try:
        for batch in sql_caller.sql_state.fetch_data(table_name="EntrezUniprotMap", kind="dbo"):
            for row in batch:
                uid = row["uniprot_id"]
                if keep is None or uid in keep:
                    crosswalk[str(row["entrez_id"])].append(uid)
    except ValueError:
        return {}
    return {e: sorted(set(us)) for e, us in crosswalk.items()}


def extract(neo4j_caller: Neo4j_ETL, sql_caller: SQL_ETL, database: str | None = None) -> dict:
    from neo4j import RoutingControl

    def run(q: str) -> list[dict]:
        records, _, _ = neo4j_caller.driver.execute_query(q, database_=database, routing_=RoutingControl.READ)
        return [r.data() for r in records]

    gene_to_accessions, protein_rows = _load_protein_data(sql_caller)

    # Q_ANNOTATIONS comes back keyed by Gene.id -- an entrez id under KEGG,
    # an accession under Reactome. Expand each annotation onto every corpus
    # accession under that gene, so annotations line up with protein_rows's
    # per-accession corpus instead of a gene with 3 isoforms only ever
    # matching 1 of them. Where the mapping is the identity (Reactome) this
    # expansion is a 1:1 pass that drops genes with no function text, which
    # is exactly the scoping wanted.
    gene_annotations = run(Q_ANNOTATIONS)
    annotations = [
        {"gene": uniprot_id, "term": a["term"], "qualifier": a["qualifier"], "evidence": a["evidence"]}
        for a in gene_annotations
        for uniprot_id in gene_to_accessions.get(a["gene"], ())
    ]

    return {
        "annotations": annotations,
        "hierarchy": run(Q_HIERARCHY),
        "terms": run(Q_TERMS),
        "genes": protein_rows,
    }


# ---------------------------------------------------------------------------
# GO DAG helpers
# ---------------------------------------------------------------------------
class GODag:
    def __init__(self, edges):
        self.parents, self.children = defaultdict(set), defaultdict(set)
        self.isa_parents, self.isa_children = defaultdict(set), defaultdict(set)
        for e in edges:
            c, p = e["child"], e["parent"]
            self.parents[c].add(p)
            self.children[p].add(c)
            if e["rel"] == SIBLING_REL:
                self.isa_parents[c].add(p)
                self.isa_children[p].add(c)
        self._anc, self._desc = {}, {}

    @staticmethod
    def _closure(t, nbrs, cache):
        if t not in cache:
            seen, stack = set(), [t]
            while stack:
                for n in nbrs.get(stack.pop(), ()):
                    if n not in seen:
                        seen.add(n)
                        stack.append(n)
            cache[t] = seen
        return cache[t]

    def ancestors(self, t):
        return self._closure(t, self.parents, self._anc)

    def descendants(self, t):
        return self._closure(t, self.children, self._desc)

    def siblings(self, t):
        # A DAG term can have several is_a parents; siblings via any of them count
        return {s for p in self.isa_parents.get(t, ()) for s in self.isa_children[p]} - {t}


def parse_qualifier(q) -> tuple[bool, str]:
    """'NOT|involved_in' -> (True, 'involved_in'); 'enables' -> (False, 'enables')."""
    q = (q or "").strip()
    if q.upper().startswith("NOT"):
        return True, q.split("|", 1)[1].lower() if "|" in q else ""
    return False, q.lower()


# ---------------------------------------------------------------------------
# 2-6. Build the dataset
# ---------------------------------------------------------------------------
@dataclass
class TrainingData:
    rows: list = field(default_factory=list)       # {"anchor", "positive", "negative"}
    eval_queries: dict = field(default_factory=dict)   # term -> name
    eval_corpus: dict = field(default_factory=dict)    # gene -> text
    eval_relevant: dict = field(default_factory=dict)  # term -> {genes}
    stats: dict = field(default_factory=dict)


def build_dataset(annotations, hierarchy, terms, genes, cfg: Config = Config()) -> TrainingData:
    rng = random.Random(cfg.seed)
    dag = GODag(hierarchy)
    term_info = {r["term"]: r for r in terms}
    gene_text = {r["gene"]: r["text"] for r in genes}
    stats = defaultdict(int)

    # 2. Clean. Weak annotations are not trusted as positives, but they still
    #    block a gene from being used as a negative (see any_pos below).
    direct_pos, direct_not, direct_any = set(), set(), set()
    for a in annotations:
        g, t = a["gene"], a["term"]
        if g not in gene_text or t not in term_info:
            stats["drop_missing_text_or_term"] += 1
            continue
        negated, qual = parse_qualifier(a["qualifier"])
        if not negated:
            direct_any.add((g, t))
        ev = a["evidence"]
        evs = set(ev) if isinstance(ev, (list, tuple)) else {ev}
        if not evs & cfg.keep_evidence:
            stats["drop_weak_evidence"] += 1
            continue
        if qual.startswith(cfg.drop_qualifier_prefixes):
            stats["drop_indirect_qualifier"] += 1
            continue
        (direct_not if negated else direct_pos).add((g, t))

    # 3. Propagate. Annotation to a term implies annotation to all ancestors;
    #    NOT a term implies NOT every descendant.
    def up(pairs):
        out = defaultdict(set)
        for g, t in pairs:
            for x in {t} | dag.ancestors(t):
                out[x].add(g)
        return out

    pos, any_pos = up(direct_pos), up(direct_any)
    neg = defaultdict(set)
    for g, t in direct_not:
        for d in {t} | dag.descendants(t):
            neg[d].add(g)
    for t in list(neg):                      # trusted positive beats a contradicting NOT
        conflict = neg[t] & pos.get(t, set())
        stats["not_conflicts_dropped"] += len(conflict)
        neg[t] -= conflict

    # 4. Anchors
    def ok(t):
        info = term_info.get(t)
        return (info and info.get("name") and t not in cfg.blocklist
                and info.get("namespace") in cfg.keep_namespaces
                and cfg.min_pos <= len(pos[t]) <= cfg.max_pos)

    anchors = sorted(t for t in list(pos) if ok(t))

    # Why the size filter rejected what it rejected. A third of the genes
    # carrying keep_evidence annotations have no function text at all (they
    # never reach extract()'s annotation expansion, since gene_to_accessions
    # only holds genes with at least one accession in FunctionData), so
    # len(pos[t]) runs at roughly two thirds of a term's true annotation count.
    # A specific term can therefore be pushed under min_pos by that gap rather
    # than by genuine sparsity, and a term genuinely over max_pos can slip
    # under the ceiling and be admitted as an anchor. Both skew the anchor set
    # toward generic terms, and neither is visible from anchors_total alone --
    # hence recording the shape of the distribution the filter acted on.
    eligible = [t for t in pos
                if (i := term_info.get(t)) and i.get("name")
                and t not in cfg.blocklist and i.get("namespace") in cfg.keep_namespaces]
    stats["eligible_terms"] = len(eligible)
    stats["rejected_below_min_pos"] = sum(1 for t in eligible if len(pos[t]) < cfg.min_pos)
    stats["rejected_above_max_pos"] = sum(1 for t in eligible if len(pos[t]) > cfg.max_pos)
    stats["pos_size_near_min"] = {n: sum(1 for t in eligible if len(pos[t]) == n)
                                  for n in range(1, cfg.min_pos + 4)}

    # 5. Split by GO term, with a lineage buffer: related terms share
    #    positives through propagation, so training on them would leak.
    rng.shuffle(anchors)
    n_test = max(1, int(len(anchors) * cfg.test_frac)) if cfg.test_frac > 0 else 0
    test = set(anchors[:n_test])
    lineage = set().union(*(dag.ancestors(t) | dag.descendants(t) for t in test)) if test else set()
    train = sorted(t for t in anchors[n_test:] if t not in lineage)
    stats.update(anchors_total=len(anchors), anchors_test=len(test),
                 anchors_train=len(train),
                 anchors_lost_to_buffer=len(anchors) - n_test - len(train))

    # 6. Mine triplets. Negative priority: curated NOT > sibling-term gene > random.
    all_genes = sorted(gene_text)
    rows = []
    for t in train:
        blocked = any_pos[t]                          # never a negative if ANY evidence links it
        curated = sorted(neg.get(t, ()))
        sib_pool = sorted(set().union(*(pos.get(s, set()) for s in dag.siblings(t))) - blocked)
        free = [g for g in all_genes if g not in blocked]
        for g in rng.sample(sorted(pos[t]), min(cfg.pos_per_anchor, len(pos[t]))):
            if curated and (rng.random() < cfg.p_curated_neg or not sib_pool):
                n, src = rng.choice(curated), "curated_NOT"
            elif sib_pool:
                n, src = rng.choice(sib_pool), "sibling"
            elif free:
                n, src = rng.choice(free), "random"
            else:
                continue
            stats[f"neg_{src}"] += 1
            rows.append({"anchor": term_info[t]["name"],
                         "positive": gene_text[g], "negative": gene_text[n],
                         "_meta": (t, g, n, src)})

    return TrainingData(
        rows=rows,
        eval_queries={t: term_info[t]["name"] for t in test},
        eval_corpus=dict(gene_text),
        eval_relevant={t: set(pos[t]) for t in test},
        stats=dict(stats),
    )
