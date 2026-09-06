import logging
from src.builders.SQL.SQLCaller import SQL_ETL
from typing import Iterable, Iterator, TypeVar, Sequence
from itertools import islice
from src.models.kegg import *
from src.models.go import GOOntologyMeta, GOOntologyRecord, EntrezUniprotMap
from src.models.base import BaseSQLObject
from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
from operator import attrgetter

logger = logging.getLogger(__name__)

T = TypeVar("T")

def batched(iterable: Iterable[T], n: int) -> Iterator[list[T]]:
    it = iter(iterable)
    while chunk := list(islice(it, n)):
        yield chunk

from typing import Callable, Sequence

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


def consume_entrez_uniprot_map(data: Iterator[EntrezUniprotMap], sql_caller: SQL_ETL, batch_size: int = 5000) -> dict:
    return _stage_and_upsert(data, sql_caller, {EntrezUniprotMap.__table_name__: lambda r: [r]}, batch_size)


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
    extractors = {
        Pathway.__table_name__:       lambda r: r[Pathway.__table_name__],
        Gene.__table_name__:        lambda r: r[Gene.__table_name__],
        Compound.__table_name__: lambda r: r[Compound.__table_name__],
        Ortholog.__table_name__:   lambda r: r[Ortholog.__table_name__],
        Reaction.__table_name__:     lambda r: r[Reaction.__table_name__],
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
