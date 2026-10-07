import logging
from src.builders.SQL.SQLCaller import SQL_ETL
from typing import Callable, Iterable, Iterator, TypeVar, Sequence
from itertools import islice
from src.models.kegg import *
from src.models.go import GOOntologyMeta, GOOntologyRecord, EntrezUniprotMap
from src.models.uniprot import FunctionData
from src.models.base import BaseSQLObject
from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
from operator import attrgetter

logger = logging.getLogger(__name__)

T = TypeVar("T")

def batched(iterable: Iterable[T], n: int) -> Iterator[list[T]]:
    it = iter(iterable)
    while chunk := list(islice(it, n)):
        yield chunk


def _stage_and_upsert(
    records: Iterable[T],
    sql_caller: SQL_ETL,
    extractors: dict[str, Callable[[T], Sequence[BaseSQLObject]]],
    batch_size: int = 500,
) -> dict:
    """
    Stages and upserts one or more tables, batched over `records`.
    Each extractor pulls that table's rows out of a single record;
    extractors must return a sequence (wrap singular fields in a list).

    Each batch is staged, upserted, and wiped before moving to the next --
    staging never accumulates more than one batch's worth of rows at a time.

    NOT suitable for a table whose sync has coverage_scope_columns unless a
    record carries every row for each scope value it touches -- batches are
    drawn by record count, so a scope value split across two batches has the
    second batch's coverage-scoped DELETE retract what the first wrote. The
    pathway-scoped tables satisfy this because one record is one pathway.
    For a source that is authoritative for the whole table, use SnapshotSync
    and _stage_all_then_replace instead.
    """
    totals = {table: 0 for table in extractors}
    buffers = {table: [] for table in extractors}

    for batch in batched(records, batch_size):
        for record in batch:
            for table, extract in extractors.items():
                buffers[table].extend(extract(record))

        for table, rows in buffers.items():
            if rows:
                sql_caller.stage_data(target_table=table, data=rows)
                totals[table] += len(rows)
                sql_caller.upsert_data(target_table=table)
                sql_caller.sql_state._clear_table_rows(table_name=table, schema='staging')
                rows.clear()

    return {f"{table}_staged": n for table, n in totals.items()}

def consume_pathway_hierarchy(data: Iterator[KEGG_CLASS], sql_caller: SQL_ETL):
    return _stage_and_upsert(data, sql_caller, {KEGG_CLASS.__table_name__: lambda r: [r]}, batch_size=1)


def consume_pathway_ids(data: Iterator[PathwayIds], sql_caller: SQL_ETL) -> dict:
    return _stage_and_upsert(data, sql_caller, {PathwayIds.__table_name__: lambda r: [r]})


def _stage_all_then_replace(
    records: Iterable[BaseSQLObject],
    sql_caller: SQL_ETL,
    table: str,
    batch_size: int = 5000,
) -> dict:
    """
    Load path for a SnapshotSync table: stage every row, then replace dbo
    with staging in one atomic swap.

    The ordering is the whole point. _stage_and_upsert upserts per batch,
    which for a replace would mean each batch deleting the previous one's
    rows. Here batching governs only how rows reach staging -- staging
    carries no delete semantics -- so the result is identical at any batch
    size, and the single swap at the end is what touches dbo.

    Staging is wiped first, not just last: a previous run that died between
    staging and the swap would otherwise leave rows behind and merge a stale
    snapshot into this one. ensure_staging_ready comes before that wipe
    because stage_data is normally what provisions the staging table, and on
    a database that has never run this table the wipe would otherwise hit a
    table that does not exist yet.
    """
    sql_caller.sql_state.ensure_staging_ready(table)
    sql_caller.wipe_staging(target_table=table)

    staged = 0
    for batch in batched(records, batch_size):
        if not batch:
            continue
        sql_caller.stage_data(target_table=table, data=batch)
        staged += len(batch)

    if not staged:
        raise ValueError(
            f"{table}: refusing to replace a snapshot table with zero rows -- "
            f"that would empty dbo.{table}.")

    sql_caller.upsert_data(target_table=table)

    # Count dbo, not what was staged. A staged-row count is the size of the
    # input, not the result: it reads 39878 whether the swap landed, deleted
    # half the rows on a MERGE, or threw an exception that SQL_ETL.sql_safe
    # swallowed into a log line. That is exactly how EntrezUniprotMap sat at
    # 21821 rows while every Airflow run reported 39878.
    actual = sql_caller.sql_state.row_count(table)
    if actual != staged:
        raise ValueError(
            f"{table}: staged {staged} rows but dbo holds {actual} after the swap. "
            f"The replace did not land -- check the log above for a swallowed "
            f"SQL error.")

    sql_caller.wipe_staging(target_table=table)
    return {f"{table}_rows": actual}


def consume_entrez_uniprot_map(data: Iterator[EntrezUniprotMap], sql_caller: SQL_ETL, batch_size: int = 5000) -> dict:
    """
    Replaces dbo.EntrezUniprotMap with the snapshot in `data`.

    The map comes straight from UniProt's idmapping file, which is always the
    complete mapping -- so there is nothing to reconcile and the old contents
    are simply superseded. Reconciling it was how 45% of the pairs went
    missing; see SnapshotSync.
    """
    return _stage_all_then_replace(data, sql_caller, EntrezUniprotMap.__table_name__, batch_size)


def consume_function_data(data: Iterator[FunctionData], sql_caller: SQL_ETL, batch_size: int = 500) -> dict:
    return _stage_and_upsert(data, sql_caller, {FunctionData.__table_name__: lambda r: [r]}, batch_size)


def consume_gene_uniprot_annotations(batches: Iterator[list[dict]], neo4j_caller: Neo4j_ETL) -> dict:
    total = 0
    for batch in batches:
        result = neo4j_caller.annotate_gene_uniprot_ids(batch)
        total += result.get("gene_uniprot_ids_annotated", 0)
    return {"gene_uniprot_ids_annotated": total}


def consume_gene_entrez_annotations(batches: Iterator[list[dict]], neo4j_caller: Neo4j_ETL) -> dict:
    total = 0
    offered = 0
    for batch in batches:
        result = neo4j_caller.annotate_gene_entrez_ids(batch)
        total += result.get("gene_entrez_ids_annotated", 0)
        offered += result.get("rows_offered", 0)
    return {"gene_entrez_ids_annotated": total, "rows_offered": offered}


def consume_kgml_meta_data(
    records: Iterator[KGMLRecord], sql_caller: SQL_ETL, batch_size: int = 10
) -> Iterator[tuple[list[str], dict[str, bytes]]]:
    """
    Stages and upserts KGML metadata per batch, then diffs — filtered
    down to this batch's pathway_ids, since the diff table accumulates
    across upserts within a run. Yields (changed_ids, bytes_by_id) per
    batch so the caller can persist immediately, using bytes already
    in hand, with no re-fetch.
    """
    target_table = KGMLMetaData.__table_name__

    for batch in batched(records, batch_size):
        metadata_batch = [r.metadata for r in batch]
        bytes_by_id = {r.pathway_code: r.kgml_bytes for r in batch}

        sql_caller.stage_data(target_table=target_table, data=metadata_batch)
        sql_caller.upsert_data(target_table=target_table)
        sql_caller.sql_state._clear_table_rows(table_name=target_table, schema='staging')

        try:
            diff_batches = sql_caller.sql_state.fetch_data(table_name=target_table, kind='diff')
            changed_ids = [
                event["pathway_id"]
                for diff_batch in diff_batches
                for event in diff_batch
                if event["pathway_id"] in bytes_by_id
            ]
        except ValueError:
            changed_ids = []

        yield changed_ids, bytes_by_id


def consume_kgml_structure_record(records: Iterator[PathwayKGMLRecord], sql_caller: SQL_ETL, batch_size: int = 10) -> dict:
    """
    Stages and upserts PathwayKGMLRecords table by table.
    """
    extractors = {
        Pathway.__table_name__:       lambda r: [r.pathway],
        Entity.__table_name__:        attrgetter("entities"),
        EntityPathMem.__table_name__: attrgetter("entity_path_mem"),
        Interaction.__table_name__:   attrgetter("relations"),
        ReactionP.__table_name__:     attrgetter("reaction_participants"),
    }
    return _stage_and_upsert(records, sql_caller, extractors, batch_size)


def consume_go_ontology_meta(record: GOOntologyRecord, sql_caller: SQL_ETL) -> bool:
    """
    Stages and upserts the GOOntologyMeta snapshot, then checks its diff
    table (IdentityHashSync) to determine whether the ontology's structure
    actually changed since the last recorded snapshot.
    """
    target_table = GOOntologyMeta.__table_name__

    sql_caller.stage_data(target_table=target_table, data=[record.metadata])
    sql_caller.upsert_data(target_table=target_table)
    sql_caller.sql_state._clear_table_rows(table_name=target_table, schema='staging')

    try:
        diff_batches = sql_caller.sql_state.fetch_data(table_name=target_table, kind='diff')
        return any(diff_batches)
    except ValueError:
        return False


def consume_go_ontology_structure(record: GOOntologyRecord, neo4j_caller: Neo4j_ETL):
    """
    Pushes the parsed GO graph into Neo4j: upserts current nodes/edges and
    detaches/deletes deprecated terms.
    """
    neo4j_caller.ontology_manager.sync_ontology_structure(record.graph)


def consume_neo4j_nodes(records: Iterator[list[dict]], neo4j_caller: Neo4j_ETL, batch_size: int=5000):
    """
    Streams SQL-fetched records into Neo4j node dataclasses and upserts them, chunk by chunk.
    """
    from src.builders.Neo4j.schema.nodes import build_neo4j_entity

    total = 0
    for batch in records:

        for chunk in batched(batch, batch_size):
            entity_nodes = [build_neo4j_entity(record["entity_type"], record) for record in chunk]
            neo4j_caller.upsert_nodes(entity_nodes)
            total += len(entity_nodes)
            logger.info("consume_neo4j_nodes: %d node(s) upserted so far", total)

    return {"nodes_upserted": total}

def consume_neo4j_edges(records: Iterator[list[dict]], table_name: str, neo4j_caller: Neo4j_ETL, batch_size: int=5000):
    """
    Streams SQL-fetched edge records groups them by Insert and Delete rows.
    """
    from itertools import groupby
    from src.builders.Neo4j.schema.edges import build_neo4j_edges

    totals = {"INSERT": 0, "DELETE": 0}
    for batch in records:
        batch.sort(key=lambda n: n["action"])

        for action, edges in groupby(batch, key= lambda n: n["action"]):
            if action == "INSERT":
                for chunk in batched(edges, batch_size):
                    entity_edges = [build_neo4j_edges(table_name, record) for record in chunk]
                    neo4j_caller.upsert_edges(entity_edges)
                    totals["INSERT"] += len(entity_edges)
                    logger.info("consume_neo4j_edges[%s]: %d upserted so far", table_name, totals["INSERT"])
            elif action == "DELETE":
                for chunk in  batched(edges, batch_size):
                    entity_edges = [build_neo4j_edges(table_name, record) for record in chunk]
                    neo4j_caller.delete_edges(entity_edges)
                    totals["DELETE"] += len(entity_edges)
                    logger.info("consume_neo4j_edges[%s]: %d deleted so far", table_name, totals["DELETE"])

    action_labels = {"INSERT": "inserted", "DELETE": "deleted"}
    return {f"{table_name}_{action_labels[k]}": v for k, v in totals.items()}



def consume_entity_annotations(records: Iterator[dict], sql_caller: SQL_ETL):
    """
    Streams KEGG batched and modified annotations packed inside their table names.
    Need to process batch by batch, unpack, stage and upsert

    It [{"table_name": }]
    It [KGMLRecord.pathway]
    """
    # A given diff batch won't necessarily contain every entity type (e.g.
    # pathways are deliberately never produced here -- see
    # produce_entity_annotations), so a table missing from a batch is the
    # normal case, not an error.
    extractors = {
        Pathway.__table_name__:       lambda r: r.get(Pathway.__table_name__, []),
        Gene.__table_name__:        lambda r: r.get(Gene.__table_name__, []),
        Compound.__table_name__: lambda r: r.get(Compound.__table_name__, []),
        Ortholog.__table_name__:   lambda r: r.get(Ortholog.__table_name__, []),
        Reaction.__table_name__:     lambda r: r.get(Reaction.__table_name__, []),
        Drug.__table_name__:     lambda r: r.get(Drug.__table_name__, []),
        Glycan.__table_name__:     lambda r: r.get(Glycan.__table_name__, []),
    }

    return _stage_and_upsert(records, sql_caller, extractors, batch_size=1)

def consume_neo4j_annotations(records: Iterator[list[dict]], table_name: str, neo4j_caller: Neo4j_ETL, batch_size: int=5000):
    """
    Streams SQL-fetched records into Neo4j node dataclasses and upserts them, chunk by chunk.
    """
    from src.builders.Neo4j.schema.nodes import build_neo4j_annotation
    from itertools import groupby

    total = 0
    for batch in records:
        batch.sort(key=lambda n: n["entity_type"])
        for _, group_batch in groupby(batch, key= lambda n: n["entity_type"]):

            for chunk in batched(group_batch, batch_size):
                entity_nodes = [build_neo4j_annotation(record["entity_type"], record) for record in chunk]
                neo4j_caller.upsert_nodes(entity_nodes)
                total += len(entity_nodes)
                logger.info("consume_neo4j_annotations[%s]: %d annotation(s) upserted so far", table_name, total)

    return {f"{table_name}_annotated": total}
