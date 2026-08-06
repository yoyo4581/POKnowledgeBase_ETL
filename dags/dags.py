from airflow.sdk import Asset, dag, task, AssetAlias
from airflow.exceptions import AirflowSkipException

from KEGG.pathway.producer import *
from KEGG.pathway.consumer import *
from KEGG.gene.producer import *
from KEGG.gene.consumer import *

from KEGG.KEGGCaller import KEGG_ETL
from local_db.SQL.SQLCaller import SQL_ETL
from local_db.Neo4jCaller import Neo4j_ETL
from local_db.GOCaller import GOCaller
from local_db.UniProtCaller import UniProt_ETL
from local_db.BertEmbeddings import bio_bert_embeddings

from utils.colored_text import RED, GREEN, RESET, YELLOW


PATHWAY_META_UPDATED = Asset("kegg://pathway_metadata")

PATHWAY_KGML_SCAN = Asset("kegg://pathway_scan")
PATHWAY_KGML_ALIAS = AssetAlias("kegg://pathway_kgml")
PATHWAY_ANNOT_COMPLETE = Asset("kegg://pathway_kgml/annot")
PATHWAY_STRUCT_DIFF_READY = Asset("kegg://pathway_kgml/diff_ready")


#Fetch the pathway map once. Just assume that

uuid = '5B146EBC-98C5-4242-A049-A5331A641CB4'

@dag(
    schedule="@daily",
    catchup=False,
    tags=["kegg", "kafka"],
)
def kegg_pathway_metadata_producer_dag():

    @task()
    def produce_staging_tables():
        """
        Produce staging tables task based on staging tables schema
        """
        sql_caller = SQL_ETL(run_id=uuid)
        # sql_caller.sql_state.wipe_staging_run()
        sql_caller.sql_state.stage_tables()


    @task(outlets=[PATHWAY_META_UPDATED])
    def check_and_produce_meta():
        """
        1. Fetches data from PathwayIds which holds XML hashes from previous run.
        2. Uses PathwayIds to fetch KGML files and places them in them temporary data directory.
        3. Calculates hashes of them and compares it with last run hashes.
        4. Flags unequal hashes.
        """
        kegg_caller = KEGG_ETL()
        sql_caller = SQL_ETL(run_id=uuid)

        p_ids = consume_pathway_ids(sql_caller)
        downloaded = produce_kgml_temp_files(p_ids, kegg_caller)
        if downloaded:
            produced_any = produce_kgml_hash(p_ids, kegg_caller, sql_caller)
            if not produced_any:
                raise AirflowSkipException("No new pathways or modifications")
    
    stage = produce_staging_tables()    
    produce = check_and_produce_meta()

    stage >> produce



@dag(
    schedule=[PATHWAY_META_UPDATED],
    catchup=False,
    tags=["kegg", "kgml", "consumer"]
)
def kegg_pathway_materialize_dag():

    @task()
    def consume_pathway_events():
        sql_caller = SQL_ETL(run_id=uuid)
        events = consume_altered_pathways(sql_caller)
        if not events:
            raise AirflowSkipException("No data was received by consumer")
        
        return events
    
    @task(outlets=[PATHWAY_KGML_ALIAS, PATHWAY_STRUCT_DIFF_READY])
    def process_pathway_kgml(events: list[dict], outlet_events):
        """
        Will parse through all pathways that have been detected as modified through their metadata.
        Will fetch their KGML files and first look for their structure and/or modified entities.
        If no structure change is detected or entities added, move on, otherwise mark the asset with the changed pathway metadata.
        """
        kegg_caller = KEGG_ETL()
        sql_caller = SQL_ETL(run_id=uuid)

        pathway_ids = {
            e["pathway_id"]
            for e in events
        }

        for pathway_id in pathway_ids:
            produced_any = produce_kgml_structure(kegg_caller, sql_caller, pathway_id)
            
            if produced_any:
                outlet_events[PATHWAY_KGML_ALIAS].add(
                    Asset(
                        f"kegg://pathway/{pathway_id}/kgml",
                        extra= {"pathway_id": pathway_id}
                    )
                )
    consume = consume_pathway_events()
    process = process_pathway_kgml(consume)

    consume >> process

@dag(
    schedule="@weekly",
    catchup=False,
    tags=["kegg", "kgml", "producer"]
)
def kegg_pathway_scheduled_materializer_dag():

    @task(outlets=[PATHWAY_KGML_ALIAS, PATHWAY_STRUCT_DIFF_READY])
    def process_all_pathway_kgml(outlet_events):
        """Will fetch all current pathway data from SQL production database
        Will parse through kgml and look for any structural differences.
        If a structural difference is found will emit an asset signal indicating which pathway and triggering anotate dag"""
        kegg_caller = KEGG_ETL()
        sql_caller = SQL_ETL(run_id=uuid)

        rows = consume_altered_pathways(sql_caller)
        pathway_ids = [row["pathway_id"] for row in rows]

        for pathway_id in pathway_ids:
            produced_any = produce_kgml_structure(kegg_caller, sql_caller, pathway_id)

            if produced_any:
                outlet_events[PATHWAY_KGML_ALIAS].add(
                    Asset(
                        f"kegg://pathway/{pathway_id}/kgml",
                        extra={"pathway_id": pathway_id}
                    )
                )
    process_all_pathway_kgml()


@dag(
    schedule=[PATHWAY_KGML_ALIAS],
    catchup=False,
    tags=['kegg', 'neo4j']
)
def annotate_kegg_entities():

    @task
    def annotate(triggering_asset_events=None):
        """
        In a given updated pathway will delve into each entity and update its annotation.
        Changed entities in entities table will be marked, as inserted or modified.
        """
        kegg_caller = KEGG_ETL()
        sql_caller = SQL_ETL(run_id=uuid)
        if not triggering_asset_events:
            raise AirflowSkipException("No triggering asset events.")

        pathway_ids = set()

        for asset, events in triggering_asset_events.items():
            for event in events:
                pathway_id = event.extra.get("pathway_id") if event.extra else None
                if pathway_id:
                    pathway_ids.add(pathway_id)

        for pathway_id in pathway_ids:
            produce_kegg_kgml(
                kegg_caller,
                sql_caller,
                pathway_id,
            )

    @task(outlets= [PATHWAY_ANNOT_COMPLETE])
    def update_neo4j_entities_from_sql():
        '''
        Will use load annotation from diff table to fetch relevant changed annotations in list[tuple] format.
        Tuple format will have different length but first 3 fields are identifiers:
        
        Action, Entity_id, Entity_type, ...
        Rest are parameters to add to Neo4j node.
        '''
        sql_caller = SQL_ETL(run_id=uuid)
        neo4j_caller = Neo4j_ETL()
        # All of these methods rely on the diff.entities data table to find changed entities and load annotations.
        compound_data = sql_caller.load_annotations_from_diff('CompoundData')
        reaction_data = sql_caller.load_reaction_annotations_from_diff()
        ortholog_data = sql_caller.load_annotations_from_diff('OrthoData')
        gene_data = sql_caller.load_annotations_from_diff('GeneData')

        sources = [compound_data, ortholog_data, gene_data, reaction_data]
        all_entities = {}
        for source in sources:
            if source:
                all_entities.update(source)
            else:
                print(f"{RED}Warning: one of the diff sources failed to load{RESET}")

        neo4j_caller.update_entities(all_entities)

    annot = annotate()
    update_neo4j = update_neo4j_entities_from_sql()

    annot >> update_neo4j


@dag(
    schedule=None,
    catchup=False,
    tags=['kegg', 'neo4j']
)
def manual_kegg_annotate():

    @task
    def manual_annotate():
        """
        In a given updated pathway will delve into each entity and update its annotation.
        Changed entities in entities table will be marked, as inserted or modified.
        """
        kegg_caller = KEGG_ETL()
        sql_caller = SQL_ETL(run_id=uuid)

        rows = consume_altered_pathways(sql_caller)
        pathway_ids = set()

        for row in rows:
            pathway_ids.add(row["pathway_id"])

        for pathway_id in pathway_ids:
            produce_kegg_kgml(
                kegg_caller,
                sql_caller,
                pathway_id,
            )

    @task(outlets= [PATHWAY_ANNOT_COMPLETE])
    def manual_update_neo4j_entities_from_sql():
        '''
        Will use load annotation from diff table to fetch relevant changed annotations in list[tuple] format.
        Tuple format will have different length but first 3 fields are identifiers:
        
        Action, Entity_id, Entity_type, ...
        Rest are parameters to add to Neo4j node.
        '''
        sql_caller = SQL_ETL(run_id=uuid)
        neo4j_caller = Neo4j_ETL()
        # All of these methods rely on the diff.entities data table to find changed entities and load annotations.
        compound_data = sql_caller.load_annotations_from_diff('CompoundData')
        reaction_data = sql_caller.load_reaction_annotations_from_diff()
        ortholog_data = sql_caller.load_annotations_from_diff('OrthoData')
        gene_data = sql_caller.load_annotations_from_diff('GeneData')

        sources = [compound_data, ortholog_data, gene_data, reaction_data]
        all_entities = {}
        for source in sources:
            if source:
                all_entities.update(source)
            else:
                print(f"{RED}Warning: one of the diff sources failed to load{RESET}")

        neo4j_caller.update_entities(all_entities)

    annot = manual_annotate()
    update_neo4j = manual_update_neo4j_entities_from_sql()

    annot >> update_neo4j

@dag(
    schedule="@weekly",
    catchup=False,
    tags=["go", "ontology"],
)
def go_ontology_sync_dag():

    @task
    def sync_ontology_network():
        go_caller = GOCaller()
        neo4j_caller = Neo4j_ETL()

        is_new_ontology = go_caller.fetch_latest_go_file('go')
        if not is_new_ontology:
            raise AirflowSkipException("GO ontology file unchanged, skipping sync")

        graph = go_caller.read_to_graph()
        neo4j_caller.ontology_manager.sync_ontology_structure(graph)

    @task(trigger_rule="none_failed")
    def sync_ontology_annotations():
        go_caller = GOCaller()
        neo4j_caller = Neo4j_ETL()

        is_new_annotation = go_caller.fetch_latest_go_file('goa')
        if not is_new_annotation:
            raise AirflowSkipException("GO annotation file unchanged, skipping sync")

        annotations = go_caller.read_annotation()
        neo4j_caller.ontology_manager.sync_ontology_annotations(annotations)

    network_build = sync_ontology_network()
    network_annotate = sync_ontology_annotations()

    network_build >> network_annotate

"""
Now I have my SQL tables holding diff data that's structural, I need to translate that into Neo4j.
These are two diff tables, they are 'diff.interactions' and 'diff.EntityPathMem'

Interactions simply should get modified, source and target ids are simple to find. Relation_type is edge label, pathway is a parameter that is a list, pathways = [] and contains every pathway which this edge exists.

EntityPathMem is referencing an entity of a given type and its membership in a pathway. These  insertions are -belongs_to-> links to pathway nodes. A deletion means that the entity no longer belongs to a given pathway, which means the deletion of this belongs_to relation. 

Any free Gene, Compound, or Ortholog that is not bound to a Pathway node should get deleted after this step.
"""

@dag(
    schedule = None,
    catchup = False,
    tags = ["kegg", "go", "neo4j", "interactions"],
)
def structure_sync_dag():

    @task
    def sync_interactions_and_membership():
        sql_caller = SQL_ETL(run_id=uuid)
        neo4j_caller = Neo4j_ETL()

        intx_events, ent_path_events, reaction_part_events = consume_altered_structure(sql_caller)
        if not intx_events and not ent_path_events:
            raise AirflowSkipException("No altered interaction data found")

        neo4j_caller.edge_sync.upsert_relationships(intx_events, ent_path_events, reaction_part_events)
        
    sync_interactions_and_membership()


@dag(
    schedule = None,
    catchup = False,
    tags = ["kegg", "neo4j", "UniProt", "function"]
)
def annotate_and_chunk_function():

    @task()
    def annotate_gene_function():
        sql_caller = SQL_ETL(run_id=uuid)
        uniprot_caller = UniProt_ETL(sql_caller)
        neo4j_caller = Neo4j_ETL()
        is_altered = uniprot_caller.sync_entrez_uniprot_map()

        #Upsert into SQL using diff.entities table for only entity_type='gene' data.
        altered_genes = consume_altered_genes_from_diff(sql_caller)
        produce_function_annotations_from_diff(altered_genes, sql_caller, uniprot_caller, neo4j_caller, bio_bert_embeddings)

    annotate_gene_function()



kegg_pathway_metadata_producer_dag()
kegg_pathway_materialize_dag()
kegg_pathway_scheduled_materializer_dag()
annotate_kegg_entities()
go_ontology_sync_dag()
structure_sync_dag()
annotate_and_chunk_function()
manual_kegg_annotate()