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
from typing import get_args

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Asset, Metadata, dag, task
from dotenv import load_dotenv

from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
from src.builders.SQL.SQLCaller import SQL_ETL
from src.builders.SQL.schema import AnnotationTables
from src.models.reactome import (
    Compound, Drug, Entity, EntityData, EntityIdentity, EntityMoiety,
    EntityPathMem, Gene, GeneEdge, Membership, Participation, Pathway, Reaction,
)
from src.parsers.Reactome.ReactomeCaller import Reactome_ETL
from src.workflow.reactome_flow import *

load_dotenv()
logger = logging.getLogger(__name__)
UUID = os.getenv("uuid")

PATHWAY_SBML_ASSET = Asset("reactome://pathway_sbml_batched")
REACTOME_STRUCTURE_COMPLETE = Asset("reactome://structure_complete")
REACTOME_EDGES_COMPLETE = Asset("reactome://gene_edges_complete")
# The downstream chain (embedding_dataset_export, neo4j_snapshot) listens on
# this. KEGG's go_ontology_annotation was the only thing emitting it, so
# retiring that DAG would strand both. Name kept as-is rather than renamed,
# to avoid editing two DAGs that are otherwise untouched by this migration.
NEO4J_KG_COMPLETE = Asset("neo4j://kgml_complete")


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
        failed: list[str] = []
        records = produce_pathway_sbml(reactome_caller, sql_caller, failed)
        for changed_ids, bytes_by_id in consume_sbml_meta_data(records, sql_caller):
            for pathway_id in changed_ids:
                reactome_caller.reactome_state.download_sbml_temp_file(
                    pathway_id, bytes_by_id[pathway_id])

        # A pathway that never got a hash is never resolved, and nothing
        # downstream can tell the difference between "Reactome dropped it"
        # and "the fetch failed". Losing a handful to origin blips is
        # tolerable and self-heals next run, since meta_build re-fetches
        # every pathway; losing 1% means the corpus is quietly incomplete.
        total = len(fetch_rows(sql_caller.sql_state, "PathwayIds", kind="dbo"))
        if failed:
            logger.error("SBML fetch failed for %d/%d pathways: %s",
                         len(failed), total, ", ".join(sorted(failed)[:20]))
            if len(failed) > max(5, total // 100):
                raise RuntimeError(
                    f"{len(failed)}/{total} SBML fetches failed - refusing to "
                    "build on a partial corpus. Re-run once Reactome is healthy.")

    @task(outlets=[PATHWAY_SBML_ASSET])
    def detect_and_signal_changes(*, outlet_events):
        """One event carrying every pathway that needs re-resolving."""
        sql_caller = SQL_ETL(run_id=UUID)
        changed_ids: set[str] = set()
        for table_name in ("PathwayIds", "PathwaySBMLMeta"):
            changed_ids.update(
                row["pathway_id"]
                for row in fetch_rows(sql_caller.sql_state, table_name, kind="diff"))
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
        pathway_ids = _changed_ids(triggering_asset_events, PATHWAY_SBML_ASSET)
        records = chain.from_iterable(
            produce_pathway_record(pathway_id, reactome_caller)
            for pathway_id in pathway_ids)
        logger.info(consume_pathway_record(records, sql_caller))

    @task()
    def structure_nodes_neo4j():
        sql_caller, neo4j_caller = SQL_ETL(run_id=UUID), Neo4j_ETL()
        diffed = sql_caller.sql_state.fetch_diff_entities(table_name=Entity.__table_name__)
        logger.info(consume_reactome_nodes(diffed, neo4j_caller))

    @task()
    def annotate_nodes_neo4j():
        """Properties onto the nodes structure created.

        Driven by diff.entities through load_from_diff, not by a diff on
        each annotation table. An annotation table holds one row per
        entity, so its key IS an entity_id -- GeneData is keyed on the
        accession that `entities` stores for that gene, EntityData on the
        stId, CompoundData on the ChEBI id. One change signal covers all
        of them, and the join carries entity_type along for the label.

        That is also why these tables are DefaultSync: with a single row
        per entity the MERGE converges on its own, and comparing columns
        to decide whether to write adds nothing -- while `<>` silently
        misses every NULL-to-value transition.
        """
        sql_caller, neo4j_caller = SQL_ETL(run_id=UUID), Neo4j_ETL()
        for table in get_args(AnnotationTables):
            # EntityData rows are physical entities whatever entity_type
            # says; EntityType.DRUG doubles as an identity type, so without
            # this the 1,083 drug entities build as drug identities.
            physical = table == EntityData.__table_name__
            try:
                diffed = sql_caller.sql_state.load_from_diff(table_name=table)
                logger.info({table: consume_reactome_annotations(
                    diffed, neo4j_caller, as_physical=physical)})
            except ValueError as e:
                if not str(e).startswith("Empty data table"):
                    raise
                logger.info("%s: nothing changed", table)

    @task(outlets=[REACTOME_STRUCTURE_COMPLETE])
    def structure_edges_neo4j():
        sql_caller, neo4j_caller = SQL_ETL(run_id=UUID), Neo4j_ETL()
        for table in (EntityPathMem.__table_name__, EntityIdentity.__table_name__,
                      Membership.__table_name__, Participation.__table_name__,
                      EntityMoiety.__table_name__):
            diffed = fetch_diff_batches(sql_caller.sql_state, table)
            logger.info(consume_reactome_edges(diffed, table, neo4j_caller))

    (resolve_structure() >> structure_nodes_neo4j() >> annotate_nodes_neo4j()
     >> structure_edges_neo4j())


@dag(schedule=[REACTOME_STRUCTURE_COMPLETE], catchup=False,
     params={"full_rebuild": False}, tags=["reactome", "neo4j"])
def reactome_gene_edges():

    @task()
    def derive_edges(params=None):
        """Layer 2, read back out of SQL rather than re-parsed -- so the
        projection is provably derived from the graph being served.

        Diff-driven, like KEGG's annotate_from_diff: only the first stage of
        a chain can carry changed_ids on its asset, because nothing
        downstream of it knows them. Reading REACTOME_STRUCTURE_COMPLETE for
        a `changed_ids` it never carries is how this task silently skipped
        every run.

        Set the `full_rebuild` param to recompute every pathway -- needed
        after a rule change, since an edge that a rule stops producing is
        only retracted for pathways this task actually revisits.
        """
        reactome_caller, sql_caller = Reactome_ETL(), SQL_ETL(run_id=UUID)
        pathway_ids = _pathways_to_reproject(sql_caller, (params or {}).get("full_rebuild"))
        if not pathway_ids:
            raise AirflowSkipException("No pathway structure changed")
        # One list per pathway, NOT chain.from_iterable: the staging batch
        # boundary has to fall between pathways, or the coverage-scoped
        # delete retracts edges a previous batch just wrote.
        records = produce_gene_edges(pathway_ids, sql_caller, reactome_caller)
        logger.info(consume_gene_edges(records, sql_caller))

    @task(outlets=[REACTOME_EDGES_COMPLETE])
    def edges_to_neo4j():
        sql_caller, neo4j_caller = SQL_ETL(run_id=UUID), Neo4j_ETL()
        diffed = fetch_diff_batches(sql_caller.sql_state, GeneEdge.__table_name__)
        logger.info(consume_reactome_edges(diffed, GeneEdge.__table_name__, neo4j_caller))

    derive_edges() >> edges_to_neo4j()


@dag(schedule=[REACTOME_EDGES_COMPLETE], catchup=False, tags=["reactome", "go"])
def reactome_ontology_annotation():
    """GO annotation over the finished graph, and the tail of the chain.

    Mirrors KEGG's go_ontology_annotation minus the entrez_uniprot_annotation
    hop: that existed only to hang a uniprot_ids property on Entrez-keyed Gene
    nodes, and a Reactome Gene node already is its accession.
    """

    refresh_ontology = TriggerDagRunOperator(
        task_id="refresh_ontology",
        trigger_dag_id="go_ontology_network",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    @task(outlets=[NEO4J_KG_COMPLETE])
    def annotate_ontologies():
        from src.parsers.GO.GOCaller import GO_ETL
        go_caller, neo4j_caller = GO_ETL(), Neo4j_ETL()
        if go_caller.fetch_latest_go_file(file_type="goa"):
            logger.info("GOA file updated, re-annotating ontology edges.")
            neo4j_caller.ontology_manager.sync_ontology_annotations(
                go_caller.read_annotation(), batch_size=10000)
        else:
            logger.info("GOA unchanged; ontology edges left as they are.")

    refresh_ontology >> annotate_ontologies()


def _pathways_to_reproject(sql_caller: SQL_ETL, full_rebuild: bool = False) -> set[str]:
    """Pathways whose derived edges need recomputing.

    Only tables carrying a pathway_id can scope this. An identity or moiety
    change on an entity in an otherwise-unchanged pathway is therefore NOT
    caught -- rare, since Reactome re-annotating an entity usually moves the
    entity too, but `full_rebuild` is the answer when it matters.
    """
    if full_rebuild:
        return {r["pathway_id"]
                for r in fetch_rows(sql_caller.sql_state, "PathwayIds", kind="dbo")}

    changed: set[str] = set()
    for table in (Membership.__table_name__, Participation.__table_name__,
                  EntityPathMem.__table_name__):
        changed.update(r["pathway_id"]
                       for r in fetch_rows(sql_caller.sql_state, table, kind="diff")
                       if r.get("pathway_id"))
    return changed


def _changed_ids(triggering_asset_events, asset: Asset) -> set[str]:
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
reactome_ontology_annotation()
