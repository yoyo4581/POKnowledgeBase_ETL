import json
from confluent_kafka import Consumer
from local_db.SQL.SQLCaller import SQL_ETL

KAFKA_TOPIC = "kegg.pathway"
KEGG_LIST_URL = "https://rest.kegg.jp/list/pathway"


def consume_pathway_ids(sql_caller: SQL_ETL) -> list[dict]:
    pathway_entries = sql_caller.sql_state.fetch_pathway_ids()
    pathway_ids = [entry.pathway_id for entry in pathway_entries if entry.pathway_id.startswith('hsa')]
    return pathway_ids

def consume_altered_pathways(sql_caller: SQL_ETL)-> tuple[dict]:
    pathway_events = sql_caller.sql_state.fetch_diff_table('PathwayKGMLMeta')
    return pathway_events

def consume_altered_structure(sql_caller: SQL_ETL) -> tuple[dict]:
    intx_events = sql_caller.sql_state.fetch_diff_table('interactions')
    entity_path_events = sql_caller.sql_state.fetch_diff_table('EntityPathMem')
    reaction_part_events = sql_caller.sql_state.fetch_diff_table('reaction_participants')
    return intx_events, entity_path_events, reaction_part_events

def consume_annotations_from_diff(sql_caller: SQL_ETL):
    """
    Will consume annotations from diff and return annotation relevant data.
    """
    compound_data = sql_caller.load_annotations_from_diff('CompoundData')
    reaction_data = sql_caller.load_reaction_annotations_from_diff()
    ortholog_data = sql_caller.load_annotations_from_diff('OrthoData')
    gene_data = sql_caller.load_annotations_from_diff('GeneData')

