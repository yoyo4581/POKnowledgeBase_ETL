from airflow.sdk import Asset, dag, task
from airflow.exceptions import AirflowSkipException

from src.workflow.consumers import *
from src.workflow.producers import *

from src.builders.SQL.SQLCaller import SQL_ETL
from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
from src.parsers.GO.GOCaller import GO_ETL
from src.parsers.UniProt.UniProtCaller import UniProt_ETL
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)
UUID = os.getenv('uuid')

GO_ONTOLOGY_COMPLETE = Asset("go://structure_complete")


@dag(
    schedule = "@monthly",
    catchup=False,
    tags=["neo4j", "go"]
)
def go_ontology_network():
    """
    A routine DAG that checks if GO has modified its internal ontology network.

    1. Pulls GO ontology file temporarily.
    2. Populates GOOntologyMeta table in SQL with metadata information. This consists of filenames but also requires the network to be built in memory so that nodes and edges are counted. Uses identity hashing to detect changes.
    3. If there are no changes then output asset GO_ONTOLOGY_COMPLETE
    4. If there are changes then deletes old ontology, and remakes it using network helpers, then outputs GO_ONTOLOGY_COMPLETE.
    """

    @task(outlets=[GO_ONTOLOGY_COMPLETE])
    def sync_go_ontology():
        """
        Fetches go-basic.json (a no-op if the HTTP cache says it's unchanged),
        then stages/upserts its GOOntologyMeta snapshot to find out whether
        the ontology's structure actually changed. Only rebuilds the Neo4j
        ontology subgraph when it did. Either way, ends by touching
        GO_ONTOLOGY_COMPLETE so downstream annotation can proceed.
        """
        go_caller = GO_ETL()
        sql_caller = SQL_ETL(run_id=UUID)

        record = produce_go_ontology(go_caller)
        if record is None:
            logger.info("go-basic.json unchanged since last fetch; ontology network is already up to date.")
            return

        structure_changed = consume_go_ontology_meta(record, sql_caller)
        logger.info("GO ontology structure changed: %s", structure_changed)

        if not structure_changed:
            return

        neo4j_caller = Neo4j_ETL()
        consume_go_ontology_structure(record, neo4j_caller)

    sync_go_ontology()


@dag(
    schedule=None,
    catchup=False,
    tags=["neo4j", "uniprot"]
)
def entrez_uniprot_annotation():
    """
    Owns the entrez_id -> uniprot_id crosswalk end to end: refreshes
    dbo.EntrezUniprotMap from UniProt's idmapping file, then pushes each
    gene's full current uniprot_id list onto its Neo4j Gene node. Triggered
    synchronously by go_ontology_annotation before it annotates GO edges,
    since that step matches genes by uniprot_id -- a Gene node with no
    uniprot_ids property can never receive an annotation edge.
    """

    @task()
    def sync_entrez_uniprot_map():
        """Refreshes dbo.EntrezUniprotMap from UniProt's idmapping file."""
        uniprot_caller = UniProt_ETL()

        if not uniprot_caller.has_idmapping_changed():
            raise AirflowSkipException("idmapping file unchanged, skipping EntrezUniprotMap update.")

        sql_caller = SQL_ETL(run_id=UUID)
        records = produce_entrez_uniprot_map(uniprot_caller)
        if not records:
            raise AirflowSkipException("No EntrezUniprotMap records produced.")

        staged_result = consume_entrez_uniprot_map(records, sql_caller)
        logger.info(staged_result)

    @task(trigger_rule="all_done")
    def annotate_gene_uniprot_ids():
        """
        Always pushes the FULL current dbo.EntrezUniprotMap to Neo4j
        Gene nodes, regardless of whether sync_entrez_uniprot_map ran or
        was skipped above (trigger_rule="all_done") -- a Gene node can be
        newly structurally synced since the last time this ran even when
        the mapping file itself hasn't changed, and it still needs its
        (already-known, unchanged) uniprot_ids pushed to it.
        """
        sql_caller = SQL_ETL(run_id=UUID)
        neo4j_caller = Neo4j_ETL()

        batches = produce_gene_uniprot_annotations(sql_caller)
        result = consume_gene_uniprot_annotations(batches, neo4j_caller)
        logger.info(result)

    sync = sync_entrez_uniprot_map()
    annotate = annotate_gene_uniprot_ids()

    sync >> annotate


@dag(
    schedule=None,
    catchup=False,
    tags=["sql", "uniprot"]
)
def function_data_build():
    """
    Manual DAG (run by hand, not on any trigger) that fetches UniProt
    function-annotation text for gene entities that structurally changed
    and stages/upserts it into dbo.FunctionData.

    FunctionData itself has no diff sync (DefaultSync, staging + production
    only) -- like GeneData, CompoundData, and the rest of AnnotationTables,
    it's driven by diff.entities instead: a gene goes through function
    annotation again whenever its entities row changed, the same
    structural-change trigger kgml_entity_annotation.annotate_from_diff
    uses for its own annotation tables, joined to UniProt ids through
    dbo.EntrezUniprotMap.
    """

    @task()
    def fetch_function_data():
        sql_caller = SQL_ETL(run_id=UUID)
        uniprot_caller = UniProt_ETL()

        targets = produce_function_targets(sql_caller)
        records = produce_function_data(uniprot_caller, targets)
        result = consume_function_data(records, sql_caller)
        logger.info(result)

    fetch_function_data()


go_ontology_network()
entrez_uniprot_annotation()
function_data_build()
