from src.parsers.KEGG.KEGGCaller import KEGG_ETL, KEGGBlockedError
from src.parsers.GO.GOCaller import GO_ETL
from src.parsers.UniProt.UniProtCaller import UniProt_ETL, clean_function_text
from src.builders.SQL.SQLCaller import SQL_ETL
from src.models.kegg import *
from src.models.go import GOOntologyMeta, GOOntologyRecord, EntrezUniprotMap
from src.models.uniprot import FunctionData
from typing import Iterator, Optional
from typing import Iterable, Iterator, TypeVar, Sequence
from itertools import islice
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.builders.SQL.schema import table_schemas
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")

def batched(iterable: Iterable[T], n: int) -> Iterator[list[T]]:
    it = iter(iterable)
    while chunk := list(islice(it, n)):
        yield chunk


def produce_pathway_hierarchy(kegg_caller: KEGG_ETL)->Iterator[KEGG_CLASS]:
    all_nodes = kegg_caller.kegg_state.fetch_brite_hierarchy()
    return kegg_caller.parse_flatten_brite(all_nodes)


def produce_pathway_ids(kegg_caller: KEGG_ETL)-> Iterator[PathwayIds]:
    pathway_text = kegg_caller.kegg_state.fetch_pathway_ids()
    return kegg_caller.parse_pathway_ids(pathway_text)

def produce_pathway_kgml(kegg_caller: KEGG_ETL, sql_caller: SQL_ETL, max_workers: int = 3) -> Iterator[KGMLRecord]:
    """
    Uses SQL Pathway Ids to fetch KGML files, keeping bytes in memory
    alongside their hash so they can be persisted later without a
    second network fetch.

    Fetches run concurrently across a small worker pool -- actual request
    pacing is enforced separately by kegg_state's shared rate limiter (see
    KEGG_State._get), so this only bounds how many fetches are in flight,
    not how fast they leave the machine. A pathway that still fails after
    the session's built-in retries is logged and skipped rather than
    aborting every other pathway's fetch.
    """
    pathway_data = sql_caller.sql_state.fetch_data(table_name='PathwayIds', kind='dbo')

    schema = table_schemas['PathwayIds']
    if not schema.key:
        raise ValueError("Schema does not have a key")
    pathway_ids = [row[schema.key] for batch in pathway_data for row in batch]

    def fetch_one(pathway_code: str) -> KGMLRecord:
        kgml_bytes = kegg_caller.kegg_state.fetch_pathway_kgml(pathway_code)
        kgml_hash = kegg_caller.kegg_state.compute_kgml_hash(kgml_bytes)
        metadata = KGMLMetaData(pathway_code, kgml_hash, datetime.now())
        return KGMLRecord(pathway_code, kgml_bytes, metadata)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_one, code): code for code in pathway_ids}
        for future in as_completed(futures):
            try:
                yield future.result()
            except KEGGBlockedError:
                # Not an ordinary failed fetch -- stop issuing more requests
                # rather than working through the rest of the pathway list.
                logger.error(f"KEGG block detected fetching {futures[future]} -- aborting remaining fetches.")
                executor.shutdown(cancel_futures=True)
                raise
            except Exception as e:
                # Covers both network-level failures (requests.RequestException,
                # once retries in _get are exhausted) and fetch_pathway_kgml's
                # own plain Exception for a non-ok response (e.g. a genuinely
                # missing/withdrawn pathway code, which isn't retryable).
                logger.error(f"Failed to fetch KGML for {futures[future]}: {e}")
                continue



def produce_pathway_structure(pathway_id, kegg_caller: KEGG_ETL)-> Iterator[PathwayKGMLRecord]:
    """
    Delves into a downloaded kgml file and parses its structure.
    """
    yield from kegg_caller.parse_kgml_structure(pathway_id)


def produce_go_ontology(go_caller: GO_ETL) -> Optional[GOOntologyRecord]:
    """
    Fetches go-basic.json, skipping entirely if the HTTP cache says it's
    unchanged since last fetch (returns None in that case, since nothing
    downstream needs to run). Otherwise parses it into the in-memory graph
    and bundles it with a GOOntologyMeta snapshot built from the file's
    ETag/Last-Modified plus its node/edge counts.
    """
    downloaded = go_caller.fetch_latest_go_file(file_type='go')
    if not downloaded:
        return None

    graph = go_caller.read_to_graph()
    metadata = GOOntologyMeta.from_meta(
        source="go",
        meta=go_caller.read_meta('go'),
        node_count=len(graph["nodes"]),
        edge_count=len(graph["edges"]),
    )
    return GOOntologyRecord(graph=graph, metadata=metadata)


def produce_entrez_uniprot_map(uniprot_caller: UniProt_ETL) -> Optional[Iterator[EntrezUniprotMap]]:
    """
    Downloads the latest UniProt idmapping file, skipping entirely if the
    HTTP cache says it's unchanged (returns None in that case). Otherwise
    streams EntrezUniprotMap records parsed straight off disk -- the file's
    own batch_size (how many lines get buffered while parsing) is independent
    of whatever batch size the consumer stages/upserts with.
    """
    downloaded = uniprot_caller.fetch_latest_idmapping()
    if not downloaded:
        return None

    return (
        EntrezUniprotMap(entrez_id=int(entrez_id), uniprot_id=uniprot_id)
        for batch in uniprot_caller.read_entrez_uniprot_map()
        for entrez_id, uniprot_id in batch
    )


def produce_gene_uniprot_annotations(sql_caller: SQL_ETL, batch_size: int = 100) -> Iterator[list[dict]]:
    """
    Streams each gene's full current uniprot_id list from
    dbo.EntrezUniprotMap (one entrez_id can map to several uniprot_ids),
    batch_size distinct entrez_ids at a time via SQL_State.fetch_grouped.
    Each yielded batch is grouped independently -- fetch_grouped already
    guarantees no entrez_id's rows are split across batches, so there's
    never a partial group to worry about carrying over.

    A full resync rather than a diff-driven one, deliberately -- mirrors
    sync_ontology_structure's own "always upsert everything current, safe
    no-op if unchanged" idiom rather than diffing this specific mapping,
    since diffing would reopen the same diff-window timing gap that has
    already bitten annotation sync elsewhere in this pipeline (an
    entrez_id's diff row can be gone by the time this runs, even though
    dbo.EntrezUniprotMap still has it).
    """
    for batch in sql_caller.sql_state.fetch_grouped('EntrezUniprotMap', 'entrez_id', batch_size):
        grouped: dict[int, list[str]] = {}
        for row in batch:
            grouped.setdefault(row['entrez_id'], []).append(row['uniprot_id'])

        yield [
            {"id": str(entrez_id), "uniprot_ids": sorted(uniprot_ids)}
            for entrez_id, uniprot_ids in grouped.items()
        ]


def produce_function_targets(sql_caller: SQL_ETL) -> Iterator[str]:
    """
    Streams uniprot_ids for gene entities that changed this run
    (diff.entities) -- the same structural-change signal
    produce_entity_annotations uses for every other annotation table
    (GeneData, CompoundData, ...), joined through dbo.EntrezUniprotMap in a
    single query (see SQL_State.fetch_diff_gene_uniprot_ids). Deliberately
    does not skip a gene just because it already has a dbo.FunctionData
    row: an entity that structurally changed is meant to go through the
    full course of annotation, function text included.
    """
    for batch in sql_caller.sql_state.fetch_diff_gene_uniprot_ids():
        for row in batch:
            yield row["uniprot_id"]


def produce_function_data(uniprot_caller: UniProt_ETL, uniprot_ids: Iterator[str], batch_size: int = 100) -> Iterator[FunctionData]:
    """
    Streams FunctionData rows for uniprot_ids (e.g. from
    produce_function_targets), batch_size accessions per UniProt request,
    skipping accessions UniProt returned with no function annotation at all.
    """
    for batch in batched(uniprot_ids, batch_size):
        rows = uniprot_caller.fetch_functions_from_uniprot(batch, batch_size=batch_size)
        for row in rows:
            text = clean_function_text(row.get("Function [CC]", ""))
            if not text:
                continue
            yield FunctionData(uniprot_id=row["Entry"], function_text=text)


def produce_entity_annotations(diffed_entities: Iterator[list[dict]], kegg_caller: KEGG_ETL)-> Iterator[dict]:
    """
    Fetch diff entities then send batch requests.
    """
    from itertools import groupby

    for batch in diffed_entities:
        batch.sort(key=lambda e: e["entity_type"])
        batch_tables: dict[str, list] = {}

        for dtype, entities in groupby(batch, key=lambda e: e["entity_type"]):
            if dtype == EntityType.PATHWAY:
                # Pathways are already annotated straight off the KGML file's own
                # <pathway title=...> attribute at structure-parse time (see
                # KEGGCaller._parse_kgml_to_entry_map) -- no need for a second,
                # redundant live KEGG text-fetch here.
                continue

            entity_codes = [entity["entity_id"] for entity in entities]
            type_tables = kegg_caller.parse_kegg_txt(entity_codes, dtype)

            for table_name, rows in type_tables.items():
                batch_tables.setdefault(table_name, []).extend(rows)

        yield batch_tables

    


