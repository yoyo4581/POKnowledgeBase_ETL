from src.parsers.KEGGCaller import KEGG_ETL
from src.parsers.GO.GOCaller import GO_ETL
from src.parsers.UniProt.UniProtCaller import UniProt_ETL
from src.builders.SQL.SQLCaller import SQL_ETL
from src.models.kegg import *
from src.models.go import GOOntologyMeta, GOOntologyRecord, EntrezUniprotMap
from typing import Iterator, Optional
from typing import Iterable, Iterator, TypeVar, Sequence
from itertools import islice
from src.builders.SQL.schema import table_schemas
from datetime import datetime


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

def produce_pathway_kgml(kegg_caller: KEGG_ETL, sql_caller: SQL_ETL) -> Iterator[KGMLRecord]:
    """
    Uses SQL Pathway Ids to fetch KGML files, keeping bytes in memory
    alongside their hash so they can be persisted later without a
    second network fetch.
    """
    pathway_data = sql_caller.sql_state.fetch_data(table_name='PathwayIds', kind='dbo')

    schema = table_schemas['PathwayIds']
    if not schema.key:
        raise ValueError("Schema does not have a key")
    pathway_ids = [row[schema.key] for batch in pathway_data for row in batch]

    for pathway_code in pathway_ids:
        kgml_bytes = kegg_caller.kegg_state.fetch_pathway_kgml(pathway_code)
        kgml_hash = kegg_caller.kegg_state.compute_kgml_hash(kgml_bytes)
        metadata = KGMLMetaData(pathway_code, kgml_hash, datetime.now())
        yield KGMLRecord(pathway_code, kgml_bytes, metadata)



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


def produce_entity_annotations(diffed_entities: Iterator[list[dict]], kegg_caller: KEGG_ETL)-> Iterator[dict]:
    """
    Fetch diff entities then send batch requests.
    """
    from itertools import groupby

    for batch in diffed_entities:
        batch.sort(key=lambda e: e["entity_type"])
        batch_tables: dict[str, list] = {}

        for dtype, entities in groupby(batch, key=lambda e: e["entity_type"]):
            entity_codes = [entity["entity_id"] for entity in entities]
            type_tables = kegg_caller.parse_kegg_txt(entity_codes, dtype)

            for table_name, rows in type_tables.items():
                batch_tables.setdefault(table_name, []).extend(rows)

        yield batch_tables

    


