"""
Reactome -> SQL -> Neo4j, in three stages.

Replaces the KEGG network DAGs (kegg_meta_build, kgml_structure_annotation,
kgml_entity_annotation). Structure and annotation are one stage here, not two:
Reactome resolves both in a single pass, and bqbiol:hasPart arrives flattened,
so the membership tree only exists via /data/query/ids. Splitting would call
the expensive endpoint twice.

Modelling rules: src/parsers/Reactome/MODELING_NOTES.md.
Refactor plan:   src/parsers/Reactome/REFACTOR_NOTES.md.
"""
import logging
import os
from itertools import chain

from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, Metadata, dag, task
from dotenv import load_dotenv

from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
from src.builders.SQL.SQLCaller import SQL_ETL
from src.models.reactome import (
    Entity, EntityIdentity, EntityMoiety, EntityPathMem, GeneEdge, Membership,
    Participation,
)
from src.parsers.Reactome.ReactomeCaller import Reactome_ETL
from src.workflow.reactome_flow import *

load_dotenv()
logger = logging.getLogger(__name__)
UUID = os.getenv("uuid")

PATHWAY_SBML_ASSET = Asset("reactome://pathway_sbml_batched")
REACTOME_STRUCTURE_COMPLETE = Asset("reactome://structure_complete")
REACTOME_EDGES_COMPLETE = Asset("reactome://gene_edges_complete")


@dag(schedule=None, catchup=False, tags=["reactome"])
def reactome_meta_build():

    @task()
    def ensure_sql_environment():
        sql_caller = SQL_ETL(run_id=UUID)
        sql_caller.sql_state.ensure_staging_environment()
        if (sql_caller.sql_state.missing_tables_check("dbo")
                or sql_caller.sql_state.missing_tables_check("staging")):
            raise RuntimeError("Unable to initialize SQL ETL environment - missing tables")

    @task()
    def reactome_hierarchy_update():
        reactome_caller, sql_caller = Reactome_ETL(), SQL_ETL(run_id=UUID)
        logger.info(consume_pathway_hierarchy(
            produce_pathway_hierarchy(reactome_caller), sql_caller))

    @task()
    def reactome_pathway_ids():
        reactome_caller, sql_caller = Reactome_ETL(), SQL_ETL(run_id=UUID)
        logger.info(consume_pathway_ids(
            produce_pathway_ids(reactome_caller), sql_caller))

    @task()
    def reactome_sbml_hash_produce():
        """Fetch every pathway's SBML, hash it, diff, and write only what
        changed to disk -- from bytes already in hand."""
        reactome_caller, sql_caller = Reactome_ETL(), SQL_ETL(run_id=UUID)
        records = produce_pathway_sbml(reactome_caller, sql_caller)
        for changed_ids, bytes_by_id in consume_sbml_meta_data(records, sql_caller):
            for pathway_id in changed_ids:
                reactome_caller.reactome_state.download_sbml_temp_file(
                    pathway_id, bytes_by_id[pathway_id])

    @task(outlets=[PATHWAY_SBML_ASSET])
    def detect_and_signal_changes(*, outlet_events):
        """One event carrying every pathway that needs re-resolving."""
        sql_caller = SQL_ETL(run_id=UUID)
        changed_ids: set[str] = set()
        for table_name in ("PathwayIds", "PathwaySBMLMeta"):
            diff_rows = sql_caller.sql_state.fetch_data(table_name, kind="diff") or []
            changed_ids.update(row["pathway_id"] for batch in diff_rows for row in batch)
        logger.info("Pathways requiring re-resolution: %s", len(changed_ids))
        if changed_ids:
            yield Metadata(PATHWAY_SBML_ASSET, {"changed_ids": sorted(changed_ids)})

    (ensure_sql_environment() >> reactome_hierarchy_update() >> reactome_pathway_ids()
     >> reactome_sbml_hash_produce() >> detect_and_signal_changes())


@dag(schedule=[PATHWAY_SBML_ASSET], catchup=False, tags=["reactome", "neo4j"])
def reactome_structure():

    @task()
    def resolve_structure(triggering_asset_events=None):
        """SBML + /data/query/ids for every changed pathway, staged into the
        structure and annotation tables together.

        Creates interior entities -- a DefinedSet's member complexes -- which
        exist nowhere in the SBML and only appear once the tree is walked.
        They are nodes like any other, so the entities diff covers them.
        """
        reactome_caller, sql_caller = Reactome_ETL(), SQL_ETL(run_id=UUID)
        pathway_ids = _changed_ids(triggering_asset_events)
        records = chain.from_iterable(
            produce_pathway_record(pathway_id, reactome_caller)
            for pathway_id in pathway_ids)
        logger.info(consume_pathway_record(records, sql_caller))

    @task()
    def structure_nodes_neo4j():
        sql_caller, neo4j_caller = SQL_ETL(run_id=UUID), Neo4j_ETL()
        diffed = sql_caller.sql_state.fetch_diff_entities(table_name=Entity.__table_name__)
        logger.info(consume_reactome_nodes(diffed, neo4j_caller))

    @task(outlets=[REACTOME_STRUCTURE_COMPLETE])
    def structure_edges_neo4j():
        sql_caller, neo4j_caller = SQL_ETL(run_id=UUID), Neo4j_ETL()
        for table in (EntityPathMem.__table_name__, EntityIdentity.__table_name__,
                      Membership.__table_name__, Participation.__table_name__,
                      EntityMoiety.__table_name__):
            diffed = sql_caller.sql_state.fetch_diff_entities(table_name=table)
            logger.info(consume_reactome_edges(diffed, table, neo4j_caller))

    resolve_structure() >> structure_nodes_neo4j() >> structure_edges_neo4j()


@dag(schedule=[REACTOME_STRUCTURE_COMPLETE], catchup=False, tags=["reactome", "neo4j"])
def reactome_gene_edges():

    @task()
    def derive_edges(triggering_asset_events=None):
        """Layer 2, read back out of SQL rather than re-parsed -- so the
        projection is provably derived from the graph being served."""
        reactome_caller, sql_caller = Reactome_ETL(), SQL_ETL(run_id=UUID)
        pathway_ids = _changed_ids(triggering_asset_events, asset=PATHWAY_SBML_ASSET)
        records = chain.from_iterable(
            produce_gene_edges(pathway_id, sql_caller, reactome_caller)
            for pathway_id in pathway_ids)
        logger.info(consume_gene_edges(records, sql_caller))

    @task(outlets=[REACTOME_EDGES_COMPLETE])
    def edges_to_neo4j():
        sql_caller, neo4j_caller = SQL_ETL(run_id=UUID), Neo4j_ETL()
        diffed = sql_caller.sql_state.fetch_diff_entities(table_name=GeneEdge.__table_name__)
        logger.info(consume_reactome_edges(diffed, GeneEdge.__table_name__, neo4j_caller))

    derive_edges() >> edges_to_neo4j()


def _changed_ids(triggering_asset_events, asset: Asset = PATHWAY_SBML_ASSET) -> set[str]:
    if not triggering_asset_events:
        raise AirflowSkipException("No trigger asset events")
    events = triggering_asset_events.get(asset, {})
    if not events:
        raise AirflowSkipException(f"No events for {asset.uri}")
    changed_ids = events[-1].extra.get("changed_ids")
    if not changed_ids:
        raise AirflowSkipException("Latest event had no changed_ids")
    return set(changed_ids)


reactome_meta_build()
reactome_structure()
reactome_gene_edges()
