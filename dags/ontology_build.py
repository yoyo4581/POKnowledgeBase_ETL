from airflow.sdk import Asset, dag, task

from src.workflow.consumers import *
from src.workflow.producers import *

from src.builders.SQL.SQLCaller import SQL_ETL
from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
from src.parsers.GO.GOCaller import GO_ETL
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


go_ontology_network()
