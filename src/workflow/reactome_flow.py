"""
Producers and consumers for the Reactome pipeline.

Same split as workflow/producers.py and consumers.py: a producer is a thin
generator over a caller method, a consumer stages and upserts. Kept in one
module because the Reactome set is small and every consumer here funnels into
the same `_stage_and_upsert`.
"""
import logging
from datetime import datetime
from operator import attrgetter
from collections import defaultdict
from typing import Iterable, Iterator

from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
from src.builders.SQL.SQLCaller import SQL_ETL
from src.builders.SQL.schema import table_schemas
from src.models.reactome import *
from src.parsers.Reactome.GeneNetwork import PathwayGraph
from src.parsers.Reactome.ReactomeCaller import Reactome_ETL
from src.workflow.consumers import _stage_and_upsert, batched

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# produce
# --------------------------------------------------------------------------

def produce_pathway_hierarchy(reactome_caller: Reactome_ETL) -> Iterator[PathwayHierarchy]:
    yield from reactome_caller.parse_event_hierarchy(
        reactome_caller.reactome_state.fetch_event_hierarchy())


def produce_pathway_ids(reactome_caller: Reactome_ETL) -> Iterator[PathwayIds]:
    """Same payload as the hierarchy -- there is no separate pathway-list
    endpoint worth calling. /data/pathways/top returns only the 29 top-level
    pathways, and the tree already names every one of them."""
    yield from reactome_caller.parse_pathway_ids(
        reactome_caller.reactome_state.fetch_event_hierarchy())


def produce_pathway_sbml(reactome_caller: Reactome_ETL, sql_caller: SQL_ETL,
                         failed: list[str] | None = None) -> Iterator[SBMLRecord]:
    """Fetch each known pathway's SBML and hash it in memory.

    The bytes ride along with the hash so a changed pathway can be written to
    disk from what is already in hand -- re-fetching after the diff would
    double the download for every change.

    One pathway must not end the run, but a skipped one leaves no
    PathwaySBMLMeta row and so is never resolved -- pass `failed` to see
    them and decide whether the coverage is good enough.

    The id list is read out in full before the first fetch, deliberately.
    Streaming it instead holds a result set open on read_conn for the whole
    download -- and since 2k ids arrive in a single 10k-row batch, the
    generator suspends there for the entire run and the closing fetchmany
    lands on a connection idle for ten minutes: HY010, after all the work.
    """
    for row in fetch_rows(sql_caller.sql_state, "PathwayIds", kind="dbo"):
        pathway_id = row["pathway_id"]
        try:
            content = reactome_caller.reactome_state.fetch_pathway_sbml(pathway_id)
        except Exception as e:                          # noqa: BLE001 - one bad pathway
            logger.error("SBML fetch failed for %s: %s", pathway_id, e)
            if failed is not None:
                failed.append(pathway_id)
            continue
        yield SBMLRecord(
            pathway_code=pathway_id,
            sbml_bytes=content,
            metadata=SBMLMetaData(
                pathway_id=pathway_id,
                sbml_hash=reactome_caller.reactome_state.compute_sbml_hash(content),
                last_checked=datetime.now()))


def produce_pathway_record(pathway_id: str,
                           reactome_caller: Reactome_ETL) -> Iterator[PathwayRecord]:
    yield from reactome_caller.parse_pathway_record(pathway_id)


def produce_gene_edges(pathway_ids: Iterable[str], sql_caller: SQL_ETL,
                       reactome_caller: Reactome_ETL) -> Iterator[list[GeneEdge]]:
    """Layer 2, SQL to SQL, one edge list per pathway.

    Reads back what the structure stage staged rather than re-parsing, so
    the projection is provably derived from the graph the rest of the
    pipeline serves.

    The four source tables are read once and indexed here, not re-read per
    pathway. fetch_rows has no WHERE clause -- it pulls the whole table and
    filters in Python -- so scoping inside the loop cost
    O(pathways x table size): 1,822 pathways over ~203k rows is 369M rows
    across ODBC, which is where three hours went.
    """
    state = sql_caller.sql_state
    memberships: dict[str, list[dict]] = defaultdict(list)
    for row in fetch_rows(state, Membership.__table_name__, kind="dbo"):
        memberships[row["pathway_id"]].append(row)
    participations: dict[str, list[dict]] = defaultdict(list)
    for row in fetch_rows(state, Participation.__table_name__, kind="dbo"):
        participations[row["pathway_id"]].append(row)

    # Not pathway-scoped: a moiety belongs to an entity, and identities are
    # global. One copy is shared by every graph.
    moieties = fetch_rows(state, EntityMoiety.__table_name__, kind="dbo")
    identities = {
        r["entity_id"]: (EntityType(r["reference_type"]), r["reference_id"])
        for r in fetch_rows(state, EntityIdentity.__table_name__, kind="dbo")
    }

    for pathway_id in pathway_ids:
        graph = PathwayGraph.from_rows(
            pathway_id, memberships.get(pathway_id, []), moieties,
            participations.get(pathway_id, []), identities)
        yield list(reactome_caller.derive_gene_edges(graph))


def fetch_rows(state, table: str, kind: str = "dbo",
               where: dict | None = None) -> list[dict]:
    """Rows from one table, empty when the table is.

    _batch_receive raises ValueError on an empty table rather than yielding
    nothing, and empty is normal here: a diff table is empty whenever
    nothing changed, and entity_moiety is empty for a corpus with no
    modified residues. Only that one message is swallowed -- the sibling
    ValueError about a missing diff strategy is a real misconfiguration.
    """
    out: list[dict] = []
    try:
        for batch in state.fetch_data(table, kind=kind):
            out.extend(r for r in batch
                       if not where or all(r.get(k) == v for k, v in where.items()))
    except ValueError as e:
        if not str(e).startswith("Empty data table"):
            raise
    return out


def _rows(state, table: str, where: dict | None = None) -> list[dict]:
    """Production rows, not staging -- layer 2 reads back what was upserted."""
    return fetch_rows(state, table, kind="dbo", where=where)


def fetch_diff_annotated(state, table: str, batch_size: int = 5000) -> Iterator[list[dict]]:
    """Changed rows of a keyed table, with their columns.

    A DiffSync diff stores the key and the action, never the changed
    columns, so the diff alone cannot annotate anything -- GeneData's diff
    is [run_id, action, uniprot_id]. The values come from production,
    matched on that table's own key, which is why fetch_diff_entities
    cannot do this job: it joins on entity_id and none of these tables
    have one.
    """
    key = table_schemas[table].key
    if not isinstance(key, str):
        raise ValueError(f"{table} has a composite key; annotate it explicitly")
    changed = {r[key] for r in fetch_rows(state, table, kind="diff")
               if r.get("action") != "DELETE"}
    if not changed:
        return
    rows = [r for r in fetch_rows(state, table, kind="dbo") if r.get(key) in changed]
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]


def fetch_diff_batches(state, table: str, batch_size: int = 5000) -> Iterator[list[dict]]:
    """Changed edge rows, batched, straight out of the diff table.

    Not fetch_diff_entities: that joins diff to dbo on entity_id, which an
    edge table like entity_membership (parent_id/child_id) does not have.
    The join exists for `entities`, whose diff carries only the key, so the
    row has to be completed from production -- an edge table's diff already
    carries every column.

    Reading the diff alone also keeps DELETEs. An INNER JOIN to dbo drops
    them by definition, since a deleted row is no longer there, and a
    retracted edge is exactly what has to reach Neo4j.

    Materialised before yielding so no cursor stays open across the Neo4j
    writes that consume this.
    """
    rows = fetch_rows(state, table, kind="diff")
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]


# --------------------------------------------------------------------------
# consume
# --------------------------------------------------------------------------

def consume_pathway_hierarchy(data: Iterator[PathwayHierarchy], sql_caller: SQL_ETL) -> dict:
    return _stage_and_upsert(
        data, sql_caller, {PathwayHierarchy.__table_name__: lambda r: [r]}, batch_size=1)


def consume_pathway_ids(data: Iterator[PathwayIds], sql_caller: SQL_ETL) -> dict:
    return _stage_and_upsert(data, sql_caller, {PathwayIds.__table_name__: lambda r: [r]})


def consume_sbml_meta_data(records: Iterator[SBMLRecord], sql_caller: SQL_ETL,
                           batch_size: int = 10) -> Iterator[tuple[list[str], dict[str, bytes]]]:
    """Stage, upsert and diff per batch, then hand back (changed_ids, bytes)
    so the caller can persist immediately from bytes already in hand.

    The diff is filtered to this batch's ids because the diff table
    accumulates across upserts within one run.
    """
    target_table = SBMLMetaData.__table_name__
    for batch in batched(records, batch_size):
        bytes_by_id = {r.pathway_code: r.sbml_bytes for r in batch}
        sql_caller.stage_data(target_table=target_table, data=[r.metadata for r in batch])
        sql_caller.upsert_data(target_table=target_table)
        sql_caller.sql_state._clear_table_rows(table_name=target_table, schema="staging")
        try:
            diff_batches = sql_caller.sql_state.fetch_data(table_name=target_table, kind="diff")
            changed_ids = [event["pathway_id"]
                           for diff_batch in diff_batches for event in diff_batch
                           if event["pathway_id"] in bytes_by_id]
        except ValueError:
            changed_ids = []
        yield changed_ids, bytes_by_id


def consume_pathway_record(records: Iterator[PathwayRecord], sql_caller: SQL_ETL,
                           batch_size: int = 5) -> dict:
    """One record fans out to every structure and annotation table.

    Entity first: it is the CDC node registry and everything else carries a
    foreign key into it.
    """
    extractors = {
        Entity.__table_name__:         attrgetter("entities"),
        Pathway.__table_name__:        lambda r: [r.pathway],
        EntityData.__table_name__:     attrgetter("entity_data"),
        Gene.__table_name__:           attrgetter("genes"),
        GeneXref.__table_name__:       attrgetter("gene_xrefs"),
        Compound.__table_name__:       attrgetter("compounds"),
        Drug.__table_name__:           attrgetter("drugs"),
        Reaction.__table_name__:       attrgetter("reactions"),
        EntityPathMem.__table_name__:  attrgetter("entity_path_mem"),
        Membership.__table_name__:     attrgetter("memberships"),
        Participation.__table_name__:  attrgetter("participations"),
        EntityIdentity.__table_name__: attrgetter("identities"),
        EntityMoiety.__table_name__:   attrgetter("moieties"),
    }
    return _stage_and_upsert(records, sql_caller, extractors, batch_size)


def consume_gene_edges(records: Iterator[list[GeneEdge]], sql_caller: SQL_ETL,
                       batch_size: int = 20) -> dict:
    """One record is one pathway's whole edge list, never a single edge.

    gene_edges syncs with coverage_scope_columns=("pathway_id",), so an
    upsert deletes every dbo row for a pathway in scope that staging does
    not carry. Feeding flat edges let a 500-row batch cut a pathway in
    half, and the batch holding the second half then deleted the first --
    57,295 of 142,547 edges on the first full run, with the loss invisible
    because every edge really had been inserted at some point.

    Batching by pathway makes the staging contents and the coverage scope
    agree by construction. Every other consumer here is already safe: they
    take PathwayRecords, so a batch boundary can only fall between
    pathways.
    """
    return _stage_and_upsert(
        records, sql_caller, {GeneEdge.__table_name__: lambda r: r}, batch_size)


# --------------------------------------------------------------------------
# Neo4j
# --------------------------------------------------------------------------

def consume_reactome_nodes(records: Iterator[list[dict]], neo4j_caller: Neo4j_ETL,
                           batch_size: int = 5000) -> dict:
    from src.builders.Neo4j.schema.reactome_graph import build_reactome_node
    # Every structural edge matches its endpoints on :Entity(id). The
    # per-class constraints Neo4j_ETL creates do not cover it -- they are
    # built from each class's static __label__, and the physical entities
    # share one dynamic-label class. Unindexed, each of ~240k edge rows
    # would scan every node.
    with neo4j_caller.driver.session() as session:
        session.run("CREATE INDEX reactome_entity_id IF NOT EXISTS "
                    "FOR (n:Entity) ON (n.id)")
    total = 0
    for batch in records:
        for chunk in batched(batch, batch_size):
            nodes = [build_reactome_node(r["entity_type"], r) for r in chunk]
            neo4j_caller.upsert_nodes(nodes)
            total += len(nodes)
    return {"nodes_upserted": total}


def consume_reactome_annotations(records: Iterator[list[dict]], neo4j_caller: Neo4j_ETL,
                                 entity_type: EntityType | None = None,
                                 as_physical: bool = False,
                                 batch_size: int = 5000) -> dict:
    """Fill node properties, which structure cannot.

    diff.entities carries only [run_id, action, entity_id] -- a DiffSync
    diff stores the key, not the diffed columns -- so the structure pass
    can create a node and give it a label but has no display name to put
    on it. The names live in the annotation tables and arrive here, onto
    the same ids, so this MERGEs onto the existing nodes rather than
    making new ones.

    entity_type is fixed for the single-type tables (a GeneData row is
    always a gene) and read from the row for EntityData, which covers all
    nine physical-entity classes and so must be joined to entities.
    """
    from src.builders.Neo4j.schema.reactome_graph import build_reactome_annotation
    total = 0
    for batch in records:
        # A DELETE here means the annotation row went away; dropping the
        # node is the entities diff's job, not this pass's.
        rows = [r for r in batch if r.get("action") != "DELETE"]
        for chunk in batched(rows, batch_size):
            nodes = [build_reactome_annotation(entity_type or r["entity_type"], r,
                                               as_physical=as_physical)
                     for r in chunk]
            neo4j_caller.upsert_nodes(nodes)
            total += len(nodes)
    return {"annotated": total}


def consume_reactome_edges(records: Iterator[list[dict]], table_name: str,
                           neo4j_caller: Neo4j_ETL, batch_size: int = 5000) -> dict:
    """Diff rows carry an action; an edge absent from the current parse is one
    Reactome removed, so DELETE is as meaningful as INSERT."""
    from itertools import groupby
    from src.builders.Neo4j.schema.reactome_graph import REACTOME_EDGE_REGISTRY

    builder = REACTOME_EDGE_REGISTRY[table_name]
    totals = {"INSERT": 0, "DELETE": 0}
    for batch in records:
        batch.sort(key=lambda n: n["action"])
        for action, rows in groupby(batch, key=lambda n: n["action"]):
            for chunk in batched(rows, batch_size):
                edges = [builder.from_sql(r) for r in chunk]
                if action == "INSERT":
                    neo4j_caller.upsert_edges(edges)
                else:
                    neo4j_caller.delete_edges(edges)
                totals[action] += len(edges)
    labels = {"INSERT": "inserted", "DELETE": "deleted"}
    return {f"{table_name}_{labels[k]}": v for k, v in totals.items()}
