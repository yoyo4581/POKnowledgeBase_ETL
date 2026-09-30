from airflow.sdk import Asset, dag, task, Metadata
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from src.workflow.consumers import *
from src.workflow.producers import *

from src.builders.SQL.SQLCaller import SQL_ETL
from parsers.KEGG.KEGGCaller import KEGG_ETL
import logging
from itertools import chain
from dotenv import load_dotenv
import os

load_dotenv()


PATHWAY_KGML_ASSET = Asset("kegg://pathway_kgml_batched")

KEGG_PATHWAYS_COMPLETE = Asset("kegg://structure_complete")
NEO4J_PATHWAYS_COMPLETE = Asset("neo4j://structure_complete")
NEO4J_KG_COMPLETE = Asset("neo4j://kgml_complete")


logger = logging.getLogger(__name__)
UUID = os.getenv('uuid')

@dag(
    schedule=None,
    catchup=False,
    tags=["kegg"]
)
def kegg_meta_build():

    @task()
    def ensure_sql_environment():
        """
        Will utilize SQL state to ensure its production environment is initialized.
        """
        sql_caller = SQL_ETL(run_id=UUID)
        sql_caller.sql_state.ensure_staging_environment()
        if sql_caller.sql_state.missing_tables_check('dbo') or sql_caller.sql_state.missing_tables_check('staging'):
            logger.error('Unable to intialize SQL ETL environment - missing tables')
            raise RuntimeError("Unable to intialize SQL ETL environment - missing tables")

    @task()
    def kegg_hierarchy_update():
        """
        Fetch Kegg pathway hierarchy and use producer and consumer functions to stage into SQL and upsert.
        """
        kegg_caller = KEGG_ETL()
        sql_caller = SQL_ETL(run_id=UUID)
        stream_data = produce_pathway_hierarchy(kegg_caller=kegg_caller)
        staged_result = consume_pathway_hierarchy(data=stream_data, sql_caller=sql_caller)
        logger.info(staged_result)

    @task()
    def kegg_pathway_ids():
        """
        Fetch Kegg pathway ids and build PathwayIds table.
        """
        kegg_caller = KEGG_ETL()
        sql_caller = SQL_ETL(run_id=UUID)
        stream_data = produce_pathway_ids(kegg_caller = kegg_caller)
        staged_result = consume_pathway_ids(data=stream_data, sql_caller=sql_caller)
        logger.info(staged_result)


    @task()
    def kegg_kgml_hash_produce():
        """
        Fetch KEGG kgml files and produce a KGML hash of them.
        1. Uses production PathwayIDs to fetch pathway ids. ✔
        2. Conducts api calls in batch, retains content in memory and then computes kgml hashes. 
        3. Upserts in batches, but after every upsert looks for what changed.
        4. Deletes staging data after upsertion.
        5. Downloads to temporary directory after noticing changes and triggers downstream assets.
        """
        kegg_caller = KEGG_ETL()
        sql_caller = SQL_ETL(run_id=UUID)
        records = produce_pathway_kgml(kegg_caller = kegg_caller, sql_caller=sql_caller)

        for changed_ids, bytes_by_id in consume_kgml_meta_data(records, sql_caller):
            for pathway_id in changed_ids:
                kegg_caller.kegg_state.download_kgml_temp_file(pathway_id, bytes_by_id[pathway_id])


    @task(outlets=[PATHWAY_KGML_ASSET])
    def detect_and_signal_changes(*, outlet_events):
        """
        Consolidates diffs from the hierarchy, pathway-id, and kgml-hash steps
        into a single set of pathway_ids that require deeper annotation, and
        emits exactly one batch of triggering events for downstream consumers.
        """
        sql_caller = SQL_ETL(run_id=UUID)

        changed_ids: set[str] = set()
        for table_name in ("PathwayIds", "PathwayKGMLMeta"):
            diff_rows = sql_caller.sql_state.fetch_data(table_name, kind="diff") or []
            changed_ids.update(row["pathway_id"] for batch in diff_rows for row in batch)

        logger.info("Pathways requiring re-annotation: %s", changed_ids)

        if changed_ids:
            yield Metadata(PATHWAY_KGML_ASSET, {"changed_ids": list(changed_ids)})
    
    env_build = ensure_sql_environment()
    kegg_hierarchy = kegg_hierarchy_update()
    kegg_pathway_id = kegg_pathway_ids()
    kegg_hash = kegg_kgml_hash_produce()
    signal = detect_and_signal_changes()

    env_build >> kegg_hierarchy >> kegg_pathway_id >> kegg_hash >> signal


@dag(schedule=[PATHWAY_KGML_ASSET],
     catchup=False,
     tags=["kegg", "neo4j"])
def kgml_structure_annotation():

    @task(outlets = [KEGG_PATHWAYS_COMPLETE])
    def structure_resolve(triggering_asset_events=None):
        """
        Basically the upstream signal shows that something has changed in the kgml file.
        There are two main types of changes, structural and annotative.
        - A structural difference manifest in the change of interaction, entity pathway membership, entity identification, reaction, or reaction participant.
        - Structural differences get propogated into annotations. These don't have diff tables, so their data will always get overriden with the new information.
        """
        kegg_caller = KEGG_ETL()
        sql_caller = SQL_ETL(run_id=UUID)
        if not triggering_asset_events:
            raise AirflowSkipException("No trigger asset events, KGML is unchanged")

        events = triggering_asset_events.get(PATHWAY_KGML_ASSET, {})

        if not events:
            raise AirflowSkipException("No events for PATHWAY_KGML_ASSET")

        latest_event = events[-1]
        changed_ids = latest_event.extra.get("changed_ids")

        if not changed_ids:
            raise AirflowSkipException("Latest event had no changed_ids")

        pathway_ids = set(changed_ids)        

        records = chain.from_iterable(
            produce_pathway_structure(pathway_id, kegg_caller) for pathway_id in pathway_ids
        )
        result = consume_kgml_structure_record(records, sql_caller)
        logger.info(result)


    @task()
    def structure_node_modify_neo4j():
        """
        - Will use the data found in diff tables to conduct neo4j structural modifications.
        
        Node Alterations:
        - These structural changes involve node insertion and deletions, should be covered by the entities table.
        - This will create nodes keyed by kegg identifiers with type annotation only.

        Edge Alterations:
        - These are captured by EntityPathMem, Interactions, and Reaction Participants. 
        - These constitute links and are either destroyed or created.
        - Interactions can happen between different entity types (e.g. Gene-Compound), not just Gene-Gene
        - EntityPathMem describes the pathway each entity (Reaction, Compound, Ortholog, Gene) is involved in.
        - Reaction Participants constitutes the Substrate -> Reaction -> Product relation, where a Gene or more Catalyzes it. Gene -catalyst-> Reaction.
        - All reaction relationships should keep a pathway_id parameter.
        """
        sql_caller = SQL_ETL(run_id=UUID)
        neo4j_caller = Neo4j_ETL()

        diffed_entities = sql_caller.sql_state.fetch_diff_entities(table_name = "entities")
        result = consume_neo4j_nodes(records=diffed_entities, neo4j_caller=neo4j_caller)
        logger.info(result)

    @task(outlets = [NEO4J_PATHWAYS_COMPLETE])
    def structure_edge_modify_neo4j():
        """
        - Will use the data in diff tables of EntityPathMem, reaction_participants, and interactions.
        - Modifications are immutable, meaning if something is not found it should cause that edge to be dropped.
        - This alteration is already found inside the diff table, as these tables have a IdentityHashSync, they will use their \
        coverage columns to check if a staging row is not found in production while covering that column (pathway_id); if its not Deletes are shown.
        
        Strategy:
        1. Group data coming out fetcher into insert and delete columns for all tables.
        2. Order doesn't matter, edge linkage should be modular.
        """
        sql_caller = SQL_ETL(run_id=UUID)
        neo4j_caller = Neo4j_ETL()

        structure_tables = ["reaction_participants"]
        for table in structure_tables:
            if table == "interactions":
                diffed_edges = sql_caller.sql_state.fetch_diff_interactions()
            else:
                diffed_edges = sql_caller.sql_state.fetch_diff_entities(table_name=table)
            result = consume_neo4j_edges(records= diffed_edges, table_name= table, neo4j_caller = neo4j_caller)
            logger.info(result)

    resolve = structure_resolve()
    node_modify = structure_node_modify_neo4j()
    edge_modify = structure_edge_modify_neo4j()

    resolve >> node_modify >> edge_modify



@dag(schedule=[KEGG_PATHWAYS_COMPLETE],
     catchup=False,
     tags=["kegg", "neo4j"])
def kgml_entity_annotation():

    @task()
    def annotate_from_diff():
        """
        - Will annotate entities from diff tables.
        - Major diff tables are built compositively from entities table.

        - Fetch entities table in batches, group, use entity_id to fetch text data in batch.
        """
        kegg_caller = KEGG_ETL()
        sql_caller = SQL_ETL(run_id=UUID)

        diffed_entities = sql_caller.sql_state.fetch_diff_entities("entities") 
        annotated_entities = produce_entity_annotations(diffed_entities, kegg_caller)

        annotation_results = consume_entity_annotations(annotated_entities, sql_caller)
        logger.info(annotation_results)


    @task()
    def annotate_to_neo4j():
        """
        Will build using diff.entities + dbo.annotations fetched data.
        Will consume by using id key on each node, and merge using params.
        Special merge conditions for pathways.
        """
        from src.builders.SQL.schema.definitions import AnnotationTables
        from typing import get_args

        neo4j_caller = Neo4j_ETL()
        sql_caller = SQL_ETL(run_id=UUID)

        annotation_tables: tuple[AnnotationTables, ...] = get_args(AnnotationTables)

        for table in annotation_tables:
            annotation_data = sql_caller.sql_state.load_from_diff(table_name=table)
            result = consume_neo4j_annotations(records = annotation_data, table_name=table, neo4j_caller=neo4j_caller)
            logger.info(result)

    annotate_sql = annotate_from_diff()
    annotate_neo4j = annotate_to_neo4j()

    annotate_sql >> annotate_neo4j

@dag(
    schedule = [KEGG_PATHWAYS_COMPLETE],
    catchup=False,
    tags=["neo4j", "go"]
)
def go_ontology_annotation():
    """
    In the event that new gene nodes are added (deleted nodes automatically delete their annotation), this step will require those nodes to get annotated.

    1. Explicitly triggers go_ontology_network and waits for it to finish, so the ontology is freshly screened (and rebuilt, if it drifted) on every run instead of relying on its independent monthly schedule to happen to be current.

    2. This will fetch diffed entities Uniprot_id from their entrez_id and populate dbo.UniProtEntrezMap.

    3. Involves redownloading up to date GOA file, and creating GO_Annotation edges between genes and their ontologies.
    """

    # wait_for_completion + reset_dag_run: this DAG's own timing is decoupled
    # from go_ontology_network's @monthly cron, so a triggered run can land
    # on the same logical date as another trigger (retry, or a second
    # KEGG-driven run in the same window) -- reset_dag_run clears and reruns
    # instead of failing on DagRunAlreadyExists.
    refresh_ontology = TriggerDagRunOperator(
        task_id="refresh_ontology",
        trigger_dag_id="go_ontology_network",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    # entrez_id -> uniprot_id is its own DAG (dags/ontology_build.py) since
    # it isn't a GO concept -- it's a general Gene<->UniProt crosswalk that
    # annotate_ontologies below happens to depend on (GO annotation edges
    # match genes by uniprot_id, so a Gene node with no uniprot_ids property
    # can never receive one). Same wait_for_completion/reset_dag_run pattern
    # as refresh_ontology above, for the same reason.
    refresh_gene_uniprot_map = TriggerDagRunOperator(
        task_id="refresh_gene_uniprot_map",
        trigger_dag_id="entrez_uniprot_annotation",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    @task(outlets=[NEO4J_KG_COMPLETE])
    def annotate_ontologies():
        go_caller = GO_ETL()
        neo4j_caller = Neo4j_ETL()

        downloaded = go_caller.fetch_latest_go_file(file_type='goa')
        if downloaded:
            logger.info("GOA annotation file updated, re-annotating ontology edges.")
            annotation_df = go_caller.read_annotation()
            neo4j_caller.ontology_manager.sync_ontology_annotations(annotation_df, batch_size=10000)


    annotate = annotate_ontologies()

    refresh_ontology >> refresh_gene_uniprot_map >> annotate

kegg_meta_build()
kgml_structure_annotation()
kgml_entity_annotation()
go_ontology_annotation()