from neo4j import GraphDatabase, Driver
import logging
import os
import time
from typing import Any, Iterable, Sequence
from src.builders.Neo4j.schema.nodes import *
from src.builders.Neo4j.schema.edges import *
from src.builders.batcher import batched
from src.builders.Neo4j.OntologyState import OntoStateManager

logger = logging.getLogger(__name__)

neo4j_username = os.getenv("NEO4J_USERNAME")
neo4j_password = os.getenv("NEO4J_PASSWORD")

if neo4j_username is None:
    raise ValueError("NEO4J_USERNAME is not set")
if neo4j_password is None:
    raise ValueError("NEO4J_PASSWORD is not set")


GRAPH_AUTH: tuple[str, str] = (neo4j_username, neo4j_password)



class Neo4j_ETL:
    def __init__(self):
        self._driver = None
        self._ontology_manager = None

    def _get_wsl_host_ip(self)->str:
        """
        Returns the windows host ip as seen from WSL2.
        Falls back to localhost if look up fails.
        """
        import subprocess
        try:
            result = subprocess.run(
                ["ip", "route", "show"],
                capture_output=True, text=True, check=True
            )
            for line in result.stdout.splitlines():
                if line.startswith("default"):
                    return line.split()[2]
        except (subprocess.CalledProcessError, IndexError, FileNotFoundError):
            pass
        return "localhost"

    @property
    def driver(self):
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                f"bolt://{self._get_wsl_host_ip()}:7687",
                auth= GRAPH_AUTH
            )
            self._driver.verify_connectivity()
            self._ensure_constraints()
        return self._driver

    def _ensure_constraints(self) -> None:
        """
        Uniqueness constraint per node label's key -- Neo4j backs a
        uniqueness constraint with an index, so this is also what makes
        every MATCH/MERGE by id an index seek instead of a full label scan.
        Nothing in this codebase created any Neo4j index or constraint
        before this, for any label.

        Driven by BaseNeo4jNode's own subclasses rather than a hand-kept
        label list, so it stays correct regardless of label as nodes.py
        gains new node types -- no separate list to remember to update.
        StructureNode is skipped: __dynamic_label__ means its real label is
        decided per-row from data (always one of the concrete labels
        already covered here), so it has no fixed label of its own.
        `IF NOT EXISTS` makes this safe to run on every Neo4j_ETL's first
        connection, same as SQL_State enforcing its schema on every
        instantiation.
        """
        seen_labels: set[str] = set()
        failed: dict[str, str] = {}
        with self._driver.session() as session:
            for cls in BaseNeo4jNode.__subclasses__():
                if cls.__dynamic_label__ or not cls.__label__ or cls.__label__ in seen_labels:
                    continue
                seen_labels.add(cls.__label__)
                constraint_name = f"unique_{cls.__label__.lower()}_{cls.__key__}"
                try:
                    session.run(
                        f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
                        f"FOR (n:`{cls.__label__}`) REQUIRE (n.{cls.__key__}) IS UNIQUE"
                    )
                except Exception as e:
                    # Neo4j refuses to create a uniqueness constraint over
                    # existing duplicate values rather than touching any
                    # data -- so this can only mean pre-existing duplicates
                    # for this one label, not something this method did. One
                    # label's bad data must not stop every other label from
                    # getting its constraint, and must not break the
                    # `driver` property for the rest of this process.
                    failed[cls.__label__] = str(e)
                    logger.error(
                        "Could not create uniqueness constraint for label %r (likely "
                        "pre-existing duplicate %s values) -- leaving it unindexed: %s",
                        cls.__label__, cls.__key__, e,
                    )
        if failed:
            logger.warning(
                "Neo4j uniqueness constraints missing for %d label(s), still relying on "
                "full label scans there: %s", len(failed), sorted(failed),
            )
        logger.info(
            "Ensured Neo4j uniqueness constraints for labels: %s",
            sorted(seen_labels - failed.keys()),
        )

    @property
    def ontology_manager(self):
        if self._ontology_manager is None:
            self._ontology_manager = OntoStateManager(self.driver)
        return self._ontology_manager

    def _run(self, tx, cypher, rows, batch_size= 2000):
        for batch in batched(rows, batch_size):
            tx.run(cypher, rows=batch)

    def _group_by_type_execute(self, neo4j_objects: Sequence[BaseNeo4jEdge|BaseNeo4jNode], action: Literal['upsert', 'delete']):
        neo4j_objects = list(neo4j_objects)
        if not neo4j_objects:
            return

        first = neo4j_objects[0]
        if isinstance(first, BaseNeo4jEdge):
            base = BaseNeo4jEdge
        elif isinstance(first, BaseNeo4jNode):
            base = BaseNeo4jNode
        else:
            raise TypeError(f"Unsupported neo4j object type: {type(first)}")

        assert self.driver is not None, ConnectionError("Couldn't connect to the database using Neo4j Driver")

        type_name = type(first).__name__
        logger.info("%s %s: %d row(s) to process", action, type_name, len(neo4j_objects))

        with self.driver.session() as session:
            for cypher, rows in base.batch_cypher(neo4j_objects, action):
                start = time.perf_counter()
                session.execute_write(self._run, cypher, rows)
                elapsed = time.perf_counter() - start
                logger.info(
                    "%s %s: wrote %d row(s) in %.2fs (%.0f rows/s)",
                    action, type_name, len(rows), elapsed, len(rows) / elapsed if elapsed > 0 else float("inf"),
                )

    

    
    def upsert_nodes(self, nodes: Sequence[BaseNeo4jNode]):
        self._group_by_type_execute(nodes, 'upsert')

    def delete_nodes(self, nodes: Sequence[BaseNeo4jNode]):
        self._group_by_type_execute(nodes, 'delete')

    def upsert_edges(self, edges: Sequence[BaseNeo4jEdge]):
        self._group_by_type_execute(edges, 'upsert')

    def delete_edges(self, edges: Sequence[BaseNeo4jEdge]):
        self._group_by_type_execute(edges, 'delete')

    @staticmethod
    def _set_gene_uniprot_ids(tx, rows):
        tx.run("""
            UNWIND $rows AS row
            MATCH (g:Gene {id: row.id})
            SET g.uniprot_ids = row.uniprot_ids
        """, rows=rows)

    def annotate_gene_uniprot_ids(self, rows: Sequence[dict], batch_size: int = 5000) -> dict:
        """
        Sets each Gene's full current uniprot_ids list, matched by entrez id
        (Gene's own key). Deliberately MATCH, not MERGE, for the Gene node --
        this patches a property onto a node structure already created, and
        must never be the thing that creates a Gene node. A gene not yet
        structurally synced is silently skipped here rather than created
        half-formed; it gets its uniprot_ids whenever this next runs after
        that gene exists (see produce_gene_uniprot_annotations).
        """
        if not rows:
            return {"gene_uniprot_ids_annotated": 0}

        total = 0
        with self.driver.session() as session:
            for chunk in batched(rows, batch_size):
                session.execute_write(self._set_gene_uniprot_ids, chunk)
                total += len(chunk)
                logger.info("annotate_gene_uniprot_ids: %d gene(s) annotated so far", total)
        return {"gene_uniprot_ids_annotated": total}