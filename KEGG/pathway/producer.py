from KEGG.KEGGCaller import KEGG_ETL
from local_db.schema import *
from local_db.SQL.SQLCaller import SQL_ETL
from datetime import datetime

KAFKA_TOPIC = "kegg.pathway"
KEGG_LIST_URL = "https://rest.kegg.jp/list/pathway"



# -------------------------------------------------------------------
# PRODUCER FUNCTION (pure business logic)
# -------------------------------------------------------------------

def produce_kgml_temp_files(pathway_ids: list, kegg_caller: KEGG_ETL)->bool:
    downloaded = kegg_caller.kegg_state.download_kgml_temp_files(pathway_ids)
    return downloaded

def produce_kgml_hash(pathway_ids: list, kegg_caller: KEGG_ETL, sql_caller: SQL_ETL)->bool:
    """
    Will produce kgml hash and compare against SQL hashes from prior run.
    """
    code_to_hash = []
    for code in pathway_ids:
        xml_content = kegg_caller.kegg_state.read_kgml_temp_file(code)
        xml_hash = kegg_caller.kegg_state.compute_kgml_hash(xml_content)
        code_to_hash.append((code, xml_hash, datetime.now()))

    has_diff_rows = sql_caller.stage_and_upsert("PathwayKGMLMeta", code_to_hash)
    return has_diff_rows


def parse_kegg_kgml(kegg_caller: KEGG_ETL, pathway_id):
    print(f"Parsing pathway XML {pathway_id}")
    xml = kegg_caller.kegg_state.read_kgml_temp_file(pathway_id)
    entry_map = kegg_caller.parse_kgml_to_entry_map(xml)

    return kegg_caller.parse_map_sql(entry_map, pathway_id)




def produce_kgml_structure(kegg_caller: KEGG_ETL, sql_caller: SQL_ETL, pathway_id: str) -> bool:
    """
    Will not only check by parsing KGML but will stage data and upsert it holding onto any change in the diff tables.

    As long as any of these diff tables holds a value, it will return a true signal.
    """
    
    modified_any = False

    entity_list, entity_path_mem_list, intrx_list, rx_list, rx_pt_list = parse_kegg_kgml(kegg_caller, pathway_id)

    diff_rows_ent = sql_caller.stage_and_upsert(target_table='entities', data=entity_list)
    diff_rows_mem = sql_caller.stage_and_upsert(target_table='EntityPathMem', data=entity_path_mem_list)
    diff_rows_int = sql_caller.stage_and_upsert(target_table='interactions', data=intrx_list)


    if diff_rows_int or diff_rows_ent or diff_rows_mem:
        modified_any = True

    return modified_any


def produce_kegg_kgml(kegg_caller: KEGG_ETL, sql_caller: SQL_ETL, pathway_id: str):

    entity_list, entity_path_mem_list, intrx_list, rx_list, rx_pt_list = parse_kegg_kgml(kegg_caller, pathway_id)

    entity_table = kegg_caller.build_entity_tables(entity_list)
    reaction_table = kegg_caller.build_reaction_table(rx_list)

    # entities and reactions are both parents of reaction_participants' FKs
    # (ent_parent, rk_parent) — both must land in their production tables
    # before reaction_participants is inserted, or the FK check fails.
    for table_name, table_data in entity_table.items():
        result = sql_caller.stage_and_upsert(target_table=table_name, data=table_data)
        if result is None:
            print(f"Aborting: upsert failed for {table_name}")
            return

    rx_result = sql_caller.stage_and_upsert(target_table='reactions', data=reaction_table)
    if rx_result is None:
        print("Aborting: upsert failed for reactions")
        return

    sql_caller.stage_and_upsert(target_table='reaction_participants', data=rx_pt_list)
    
