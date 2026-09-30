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
from typing import Iterator

from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
from src.builders.SQL.SQLCaller import SQL_ETL
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
    yield from reactome_caller.parse_pathway_ids(
        reactome_caller.reactome_state.fetch_pathway_ids())


def produce_pathway_sbml(reactome_caller: Reactome_ETL,
                         sql_caller: SQL_ETL) -> Iterator[SBMLRecord]:
    """Fetch each known pathway's SBML and hash it in memory.

    The bytes ride along with the hash so a changed pathway can be written to
    disk from what is already in hand -- re-fetching after the diff would
    double the download for every change.
    """
    for batch in (sql_caller.sql_state.fetch_data("PathwayIds") or []):
        for row in batch:
            pathway_id = row["pathway_id"]
            try:
                content = reactome_caller.reactome_state.fetch_pathway_sbml(pathway_id)
            except Exception as e:                      # noqa: BLE001 - one bad pathway
                logger.error("SBML fetch failed for %s: %s", pathway_id, e)
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


def produce_gene_edges(pathway_id: str, sql_caller: SQL_ETL,
                       reactome_caller: Reactome_ETL) -> Iterator[GeneEdge]:
    """Layer 2, SQL to SQL. Reads back what the structure stage staged rather
    than re-parsing, so the projection is provably derived from the graph the
    rest of the pipeline serves."""
    state = sql_caller.sql_state
    scope = {"pathway_id": pathway_id}
    memberships = _rows(state, "entity_membership", scope)
    moieties = _rows(state, "entity_moiety")
    participations = _rows(state, "reaction_participants", scope)
    identities = {
        r["entity_id"]: (EntityType(r["reference_type"]), r["reference_id"])
        for r in _rows(state, "entity_identity")
    }
    graph = PathwayGraph.from_rows(pathway_id, memberships, moieties,
                                   participations, identities)
    yield from reactome_caller.derive_gene_edges(graph)


def _rows(state, table: str, where: dict | None = None) -> list[dict]:
    out = []
    for batch in (state.fetch_data(table) or []):
        out.extend(r for r in batch
                   if not where or all(r.get(k) == v for k, v in where.items()))
    return out


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


def consume_gene_edges(records: Iterator[GeneEdge], sql_caller: SQL_ETL,
                       batch_size: int = 500) -> dict:
    return _stage_and_upsert(
        records, sql_caller, {GeneEdge.__table_name__: lambda r: [r]}, batch_size)


# --------------------------------------------------------------------------
# Neo4j
# --------------------------------------------------------------------------

def consume_reactome_nodes(records: Iterator[list[dict]], neo4j_caller: Neo4j_ETL,
                           batch_size: int = 5000) -> dict:
    from src.builders.Neo4j.schema.reactome_graph import build_reactome_node
    total = 0
    for batch in records:
        for chunk in batched(batch, batch_size):
            nodes = [build_reactome_node(r["entity_type"], r) for r in chunk]
            neo4j_caller.upsert_nodes(nodes)
            total += len(nodes)
    return {"nodes_upserted": total}


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
